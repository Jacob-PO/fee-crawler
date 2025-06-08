#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LG U+ 휴대폰 지원금 통합 크롤러 - v7.2 수정 버전
v3.6 작동 코드 기반으로 완전 재작성

주요 수정사항:
    - 가입유형/기기종류 선택 로직 수정 (id 사용)
    - 테이블 데이터 추출 로직 v3.6 기반 재작성
    - 페이지네이션 처리 개선
    - 안정적인 요소 대기 및 클릭

작성일: 2025-01-11
버전: 7.2
"""

import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import json
from datetime import datetime
import logging
import os
import argparse
import sys
from typing import List, Dict, Optional, Tuple
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import defaultdict
from queue import Queue
import traceback

# Rich library for better UI
try:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, MofNCompleteColumn
    from rich.table import Table
    from rich.live import Live
    from rich.layout import Layout
    from rich.panel import Panel
    from rich import print as rprint
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Warning: Rich library not installed. Install with: pip install rich")

# Console 초기화
console = Console() if RICH_AVAILABLE else None

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=[
        logging.FileHandler('lg_crawler.log', encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)


class LGCrawlerV7:
    """LG U+ 크롤러 v7.2 - 수정 버전"""
    
    def __init__(self, config=None):
        """초기화"""
        self.base_url = "https://www.lguplus.com/mobile/financing-model"
        self.data = []
        self.data_lock = threading.Lock()
        
        # 기본 설정
        self.config = {
            'headless': False,
            'page_load_timeout': 30,
            'element_wait_timeout': 15,
            'table_wait_timeout': 30,
            'max_workers': 1,
            'retry_count': 3,
            'output_dir': 'data',
            'save_formats': ['excel', 'csv'],
            'max_rate_plans': 0,
            'minimal_wait': False,
            'show_browser': True,
            'debug_screenshots': False,
            'single_thread': True,
            'wait_after_click': 1.5,
            'delay_between_actions': 1,
        }
        
        if config:
            self.config.update(config)
        
        # 디렉토리 생성
        os.makedirs(self.config['output_dir'], exist_ok=True)
        if self.config['debug_screenshots']:
            os.makedirs('debug_screenshots', exist_ok=True)
        
        # 전역 변수
        self.all_combinations = []
        self.completed_count = 0
        self.failed_count = 0
        self.total_extracted = 0
        self.start_time = None
        
        # 진행 상태 추적
        self.status_lock = threading.Lock()
        self.current_tasks = {}
        
        logger.info(f"크롤러 초기화 완료. 설정: {self.config}")
    
    def create_driver(self):
        """Chrome 드라이버 생성"""
        chrome_options = Options()
        
        # 기본 옵션
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-first-run')
        chrome_options.add_argument('--no-default-browser-check')
        
        # User-Agent 설정
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Headless 모드
        if self.config['headless'] and not self.config.get('show_browser'):
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--window-size=1920,1080')
        else:
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--start-maximized')
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(self.config['page_load_timeout'])
            driver.maximize_window()
            
            logger.info("드라이버 생성 성공")
            return driver
            
        except Exception as e:
            logger.error(f"드라이버 생성 실패: {str(e)}")
            raise
    
    def wait_for_page_ready(self, driver):
        """페이지가 완전히 로드될 때까지 대기"""
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            # jQuery가 있으면 AJAX 완료 대기
            try:
                WebDriverWait(driver, 5).until(
                    lambda d: d.execute_script("return typeof jQuery != 'undefined' && jQuery.active == 0")
                )
            except:
                pass
            time.sleep(0.5)
        except:
            pass
    
    def safe_click(self, driver, element):
        """안전한 클릭 (JavaScript 실행)"""
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", element)
            return True
        except:
            try:
                element.click()
                return True
            except:
                return False
    
    def select_option(self, driver, name: str, value: str) -> bool:
        """라디오 버튼 선택 - v3.6 기반"""
        try:
            # 가입유형과 기기종류 모두 id로 선택
            if name in ["가입유형", "기기종류"]:
                radio = WebDriverWait(driver, self.config['element_wait_timeout']).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, f'input[name="{name}"][id="{value}"]'))
                )
            else:
                radio = WebDriverWait(driver, self.config['element_wait_timeout']).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, f'input[name="{name}"][value="{value}"]'))
                )
            
            if not radio.is_selected():
                radio_id = radio.get_attribute('id')
                label = driver.find_element(By.CSS_SELECTOR, f'label[for="{radio_id}"]')
                self.safe_click(driver, label)
                time.sleep(self.config['delay_between_actions'])
                
            logger.info(f"{name} 선택: {value}")
            return True
            
        except TimeoutException:
            logger.error(f"옵션을 찾을 수 없음 ({name}, {value})")
            return False
        except Exception as e:
            logger.error(f"옵션 선택 오류 ({name}, {value}): {e}")
            return False
    
    def select_all_manufacturers(self, driver) -> bool:
        """제조사 전체 선택"""
        try:
            all_checkbox = driver.find_element(By.CSS_SELECTOR, 'input[id="전체"]')
            if not all_checkbox.is_selected():
                all_label = driver.find_element(By.CSS_SELECTOR, 'label[for="전체"]')
                self.safe_click(driver, all_label)
                time.sleep(self.config['delay_between_actions'])
                logger.info("제조사 '전체' 선택 완료")
            return True
        except Exception as e:
            logger.error(f"제조사 전체 선택 오류: {e}")
            return False
    
    def wait_for_table_ready(self, driver) -> bool:
        """테이블이 준비될 때까지 대기"""
        try:
            self.wait_for_page_ready(driver)
            
            # 테이블 찾기
            table = WebDriverWait(driver, self.config['table_wait_timeout']).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'table.b-table'))
            )
            
            # 행이 있는지 확인
            rows = table.find_elements(By.CSS_SELECTOR, 'tbody tr')
            if len(rows) > 0:
                logger.info(f"테이블 발견: {len(rows)}개 행")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"테이블 대기 중 오류: {e}")
            return False
    
    def extract_table_data(self, driver, subscription_type: str, device_type: str, 
                          rate_plan_name: str = "전체", rate_plan_id: str = None) -> int:
        """테이블 데이터 추출 - v3.6 기반"""
        extracted_count = 0
        
        try:
            if not self.wait_for_table_ready(driver):
                return 0
            
            rows = driver.find_elements(By.CSS_SELECTOR, 'table.b-table tbody tr')
            if not rows:
                logger.error("테이블 행을 찾을 수 없습니다")
                return 0
            
            current_device = None
            current_price = None
            current_date = None
            
            for row_index, row in enumerate(rows):
                try:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    
                    if not cells:
                        continue
                    
                    # 첫 번째 셀에 rowspan이 있으면 새로운 기기
                    if len(cells) >= 9 and cells[0].get_attribute('rowspan'):
                        # 기기 정보 추출
                        try:
                            device_link = cells[0].find_element(By.CSS_SELECTOR, 'a.link')
                            device_name = device_link.find_element(By.CSS_SELECTOR, 'span.tit').text.strip()
                            model_code = device_link.find_element(By.CSS_SELECTOR, 'span.txt').text.strip()
                            current_device = f"{device_name} ({model_code})"
                            
                            current_price = cells[1].text.strip().replace('원', '').replace(',', '')
                            current_date = cells[2].text.strip()
                            
                            # 나머지 데이터 (인덱스 3부터)
                            plan_duration = cells[3].text.strip()
                            subsidy = cells[4].text.strip().replace('원', '').replace(',', '')
                            additional_subsidy = cells[5].text.strip().replace('원', '').replace(',', '')
                            total_subsidy = cells[6].text.strip().replace('원', '').replace(',', '')
                            
                            try:
                                recommended_discount = cells[7].find_element(By.CSS_SELECTOR, 'p.fw-b').text.strip().replace('원', '').replace(',', '')
                            except:
                                recommended_discount = '0'
                            
                            final_price = cells[8].text.strip().replace('원', '').replace(',', '')
                            
                        except Exception as e:
                            logger.debug(f"새 기기 정보 추출 오류: {e}")
                            continue
                    
                    elif len(cells) >= 6:  # rowspan이 없는 행 (같은 기기의 다른 약정)
                        plan_duration = cells[0].text.strip()
                        subsidy = cells[1].text.strip().replace('원', '').replace(',', '')
                        additional_subsidy = cells[2].text.strip().replace('원', '').replace(',', '')
                        total_subsidy = cells[3].text.strip().replace('원', '').replace(',', '')
                        
                        try:
                            recommended_discount = cells[4].find_element(By.CSS_SELECTOR, 'p.fw-b').text.strip().replace('원', '').replace(',', '')
                        except:
                            recommended_discount = '0'
                        
                        final_price = cells[5].text.strip().replace('원', '').replace(',', '')
                    else:
                        continue
                    
                    # 데이터 저장
                    if current_device and subsidy and final_price:
                        with self.data_lock:
                            self.data.append({
                                '가입유형': subscription_type,
                                '기기종류': device_type,
                                '제조사': '전체',
                                '요금제': rate_plan_name,
                                '요금제ID': rate_plan_id or '',
                                '월납부금액': '0',
                                '기기명': current_device,
                                '출고가': current_price,
                                '공시일자': current_date,
                                '요금제유지기간': plan_duration,
                                '공시지원금': subsidy,
                                '추가공시지원금': additional_subsidy,
                                '지원금총액': total_subsidy,
                                '추천할인': recommended_discount,
                                '최종구매가': final_price,
                                '크롤링시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            })
                        extracted_count += 1
                        
                except StaleElementReferenceException:
                    logger.debug(f"Stale element at row {row_index}, skipping...")
                    continue
                except Exception as e:
                    logger.debug(f"행 {row_index} 처리 오류: {e}")
                    continue
            
            logger.info(f"페이지에서 {extracted_count}개 데이터 추출")
            return extracted_count
            
        except Exception as e:
            logger.error(f"테이블 데이터 추출 오류: {e}")
            return 0
    
    def handle_pagination(self, driver, subscription_type: str, device_type: str, 
                         rate_plan_name: str = "전체", rate_plan_id: str = None) -> int:
        """페이지네이션 처리"""
        page = 1
        total_extracted = 0
        max_pages = 50  # 무한 루프 방지
        
        # 테스트 모드에서는 첫 2페이지만
        if self.config.get('test_mode', False):
            max_pages = 2
            
        while page <= max_pages:
            try:
                logger.info(f"페이지 {page} 크롤링 중...")
                
                # 현재 페이지 데이터 추출
                extracted = self.extract_table_data(driver, subscription_type, device_type, rate_plan_name, rate_plan_id)
                total_extracted += extracted
                
                if extracted == 0 and page == 1:
                    logger.warning("첫 페이지에서 데이터를 찾지 못함")
                    break
                
                # 다음 페이지 확인
                try:
                    pagination = driver.find_element(By.CSS_SELECTOR, 'ul.pagination')
                    buttons = pagination.find_elements(By.TAG_NAME, 'li')
                    
                    next_button = None
                    for i, button in enumerate(buttons):
                        try:
                            if 'active' in button.get_attribute('class'):
                                # 현재 페이지 다음 버튼이 다음 페이지
                                if i + 1 < len(buttons):
                                    next_candidate = buttons[i + 1]
                                    if 'disabled' not in next_candidate.get_attribute('class'):
                                        next_button = next_candidate.find_element(By.TAG_NAME, 'button')
                                        break
                        except:
                            continue
                    
                    if next_button:
                        logger.info(f"다음 페이지로 이동 (페이지 {page + 1})")
                        self.safe_click(driver, next_button)
                        time.sleep(3)
                        self.wait_for_page_ready(driver)
                        page += 1
                    else:
                        logger.info(f"마지막 페이지 도달 (페이지 {page})")
                        break
                        
                except Exception as e:
                    logger.debug(f"페이지네이션 처리 중 오류: {e}")
                    break
                    
            except Exception as e:
                logger.error(f"페이지 {page} 처리 중 오류: {e}")
                break
        
        logger.info(f"총 {page}개 페이지에서 {total_extracted}개 데이터 수집")
        return total_extracted
    
    def open_rate_plan_modal(self, driver) -> bool:
        """요금제 선택 모달 열기"""
        try:
            more_button = driver.find_element(By.CSS_SELECTOR, 'button.c-btn-rect-2')
            self.safe_click(driver, more_button)
            time.sleep(2)
            
            # 모달 확인
            modal = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.modal-content'))
            )
            
            if modal:
                logger.info("요금제 선택 모달 열기 성공")
                return True
                
        except Exception as e:
            logger.error(f"요금제 모달 열기 실패: {e}")
            
        return False
    
    def get_all_rate_plans(self, driver) -> List[Dict]:
        """모달에서 모든 요금제 추출"""
        try:
            rate_plans = []
            
            sections = driver.find_elements(By.CSS_SELECTOR, 'div.c-section')
            
            for section in sections:
                try:
                    radios = section.find_elements(By.CSS_SELECTOR, 'input[type="radio"]')
                    
                    for radio in radios:
                        try:
                            plan_id = radio.get_attribute('id')
                            plan_value = radio.get_attribute('value')
                            label = driver.find_element(By.CSS_SELECTOR, f'label[for="{plan_id}"]')
                            plan_name = label.text.strip()
                            
                            if plan_name:
                                rate_plans.append({
                                    'id': plan_id,
                                    'value': plan_value,
                                    'name': plan_name
                                })
                                
                        except:
                            continue
                            
                except:
                    continue
            
            logger.info(f"총 {len(rate_plans)}개의 요금제 발견")
            return rate_plans
            
        except Exception as e:
            logger.error(f"요금제 목록 추출 오류: {e}")
            return []
    
    def collect_all_combinations(self):
        """모든 조합 수집"""
        if RICH_AVAILABLE:
            console.print("[bold cyan]요금제 조합 수집 시작...[/bold cyan]")
        else:
            print("요금제 조합 수집 시작...")
        
        driver = self.create_driver()
        
        try:
            subscription_types = [
                ('1', '기기변경'),
                ('2', '번호이동'), 
                ('3', '신규가입')
            ]
            
            device_types = [
                ('00', '5G폰'),
                ('01', 'LTE폰')
            ]
            
            combinations = []
            
            # Progress 표시
            total_steps = len(subscription_types) * len(device_types)
            
            if RICH_AVAILABLE:
                progress = Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TimeRemainingColumn(),
                    console=console
                )
                
                with progress:
                    task = progress.add_task("[cyan]요금제 수집", total=total_steps)
                    
                    for sub_value, sub_name in subscription_types:
                        for dev_value, dev_name in device_types:
                            progress.update(task, description=f"[cyan]{sub_name} - {dev_name}")
                            
                            rate_plans = self._collect_rate_plans_for_combination(
                                driver, sub_value, sub_name, dev_value, dev_name
                            )
                            
                            # 요금제 수 제한
                            if self.config['max_rate_plans'] > 0:
                                rate_plans = rate_plans[:self.config['max_rate_plans']]
                            
                            for rate_plan in rate_plans:
                                combinations.append({
                                    'sub_value': sub_value,
                                    'sub_name': sub_name,
                                    'dev_value': dev_value,
                                    'dev_name': dev_name,
                                    'rate_plan': rate_plan
                                })
                            
                            if rate_plans:
                                console.print(f"[green]✓[/green] {sub_name} - {dev_name}: {len(rate_plans)}개")
                            
                            progress.advance(task)
            else:
                step = 0
                for sub_value, sub_name in subscription_types:
                    for dev_value, dev_name in device_types:
                        step += 1
                        print(f"[{step}/{total_steps}] {sub_name} - {dev_name}")
                        
                        rate_plans = self._collect_rate_plans_for_combination(
                            driver, sub_value, sub_name, dev_value, dev_name
                        )
                        
                        if self.config['max_rate_plans'] > 0:
                            rate_plans = rate_plans[:self.config['max_rate_plans']]
                        
                        for rate_plan in rate_plans:
                            combinations.append({
                                'sub_value': sub_value,
                                'sub_name': sub_name,
                                'dev_value': dev_value,
                                'dev_name': dev_name,
                                'rate_plan': rate_plan
                            })
                        
                        if rate_plans:
                            print(f"✓ {sub_name} - {dev_name}: {len(rate_plans)}개")
            
            self.all_combinations = combinations
            
            if RICH_AVAILABLE:
                console.print(f"\n[bold green]총 {len(combinations)}개 조합 준비 완료![/bold green]\n")
            else:
                print(f"\n총 {len(combinations)}개 조합 준비 완료!\n")
            
        except Exception as e:
            logger.error(f"조합 수집 오류: {str(e)}")
            logger.error(traceback.format_exc())
            
        finally:
            driver.quit()
    
    def _collect_rate_plans_for_combination(self, driver, sub_value, sub_name, dev_value, dev_name):
        """특정 조합의 요금제 수집"""
        try:
            logger.info(f"요금제 수집 시작: {sub_name} - {dev_name}")
            
            # 페이지 로드
            driver.get(self.base_url)
            self.wait_for_page_ready(driver)
            time.sleep(3)
            
            # 가입유형 선택
            if not self.select_option(driver, '가입유형', sub_value):
                return []
            
            # 기기종류 선택
            if not self.select_option(driver, '기기종류', dev_value):
                return []
            
            # 요금제 모달 열기
            if not self.open_rate_plan_modal(driver):
                return []
            
            # 요금제 수집
            rate_plans = self.get_all_rate_plans(driver)
            
            # 모달 닫기
            try:
                close_button = driver.find_element(By.CSS_SELECTOR, 'button.c-btn-close')
                self.safe_click(driver, close_button)
                time.sleep(1)
            except:
                pass
            
            return rate_plans
            
        except Exception as e:
            logger.error(f"요금제 수집 오류 ({sub_name} - {dev_name}): {str(e)}")
            return []
    
    def process_combination(self, combo_index, progress=None, task_id=None):
        """단일 조합 처리"""
        combo = self.all_combinations[combo_index]
        driver = None
        
        try:
            logger.info(f"[{combo_index+1}/{len(self.all_combinations)}] 처리 시작: {combo['sub_name']} - {combo['dev_name']} - {combo['rate_plan']['name']}")
            
            driver = self.create_driver()
            
            # 상세 진행 상황 업데이트
            if progress and task_id is not None:
                desc = f"[{combo_index+1}/{len(self.all_combinations)}] {combo['sub_name']} - {combo['dev_name']} - {combo['rate_plan']['name'][:30]}..."
                progress.update(task_id, description=desc)
            
            # 재시도 로직
            for retry in range(self.config['retry_count']):
                try:
                    # 페이지 로드
                    driver.get(self.base_url)
                    self.wait_for_page_ready(driver)
                    time.sleep(3)
                    
                    # 옵션 선택
                    if not self._select_options(driver, combo):
                        logger.warning(f"옵션 선택 실패 (재시도 {retry+1}/{self.config['retry_count']})")
                        continue
                    
                    # 데이터 추출
                    extracted = self.handle_pagination(driver, combo['sub_name'], combo['dev_name'], 
                                                     combo['rate_plan']['name'], combo['rate_plan'].get('value'))
                    
                    if extracted > 0:
                        with self.data_lock:
                            self.completed_count += 1
                            self.total_extracted += extracted
                        
                        logger.info(f"✓ [{combo_index+1}] 성공: {extracted}개 추출")
                        
                        if RICH_AVAILABLE and extracted > 0:
                            console.print(f"[green]✓[/green] [{combo_index+1}/{len(self.all_combinations)}] {combo['rate_plan']['name'][:40]}... - [bold]{extracted}개[/bold]")
                        
                        return True
                    else:
                        logger.warning(f"데이터 추출 실패 (재시도 {retry+1}/{self.config['retry_count']})")
                        
                except Exception as e:
                    logger.error(f"재시도 중 오류 ({retry+1}/{self.config['retry_count']}): {str(e)}")
                    if retry < self.config['retry_count'] - 1:
                        time.sleep(3)
            
            # 모든 재시도 실패
            with self.data_lock:
                self.failed_count += 1
            logger.error(f"✗ [{combo_index+1}] 최종 실패")
            return False
                
        except Exception as e:
            logger.error(f"처리 오류 [{combo_index+1}]: {str(e)}")
            with self.data_lock:
                self.failed_count += 1
            return False
            
        finally:
            if driver:
                driver.quit()
    
    def _select_options(self, driver, combo):
        """옵션 선택"""
        try:
            # 가입유형 선택
            if not self.select_option(driver, '가입유형', combo['sub_value']):
                return False
            
            # 기기종류 선택
            if not self.select_option(driver, '기기종류', combo['dev_value']):
                return False
            
            # 요금제 선택
            if self.open_rate_plan_modal(driver):
                try:
                    # 요금제 라디오 선택
                    radio = driver.find_element(By.CSS_SELECTOR, f'input[id="{combo["rate_plan"]["id"]}"]')
                    if not radio.is_selected():
                        label = driver.find_element(By.CSS_SELECTOR, f'label[for="{combo["rate_plan"]["id"]}"]')
                        self.safe_click(driver, label)
                        time.sleep(1)
                    
                    # 적용 버튼
                    apply_button = driver.find_element(By.CSS_SELECTOR, 'button.c-btn-solid-1-m')
                    self.safe_click(driver, apply_button)
                    time.sleep(3)
                except Exception as e:
                    logger.error(f"요금제 선택 오류: {e}")
                    return False
            else:
                return False
            
            # 제조사 전체 선택
            if not self.select_all_manufacturers(driver):
                return False
            
            # 데이터 로딩 대기
            time.sleep(3)
            
            return True
            
        except Exception as e:
            logger.error(f"옵션 선택 오류: {str(e)}")
            return False
    
    def run_single_thread_crawling(self):
        """단일 스레드 크롤링"""
        if RICH_AVAILABLE:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("• {task.fields[status]}"),
                TimeRemainingColumn(),
                console=console,
                refresh_per_second=1
            ) as progress:
                
                main_task = progress.add_task(
                    "[green]전체 진행률",
                    total=len(self.all_combinations),
                    status=f"추출: 0개"
                )
                
                for i in range(len(self.all_combinations)):
                    self.process_combination(i, progress, main_task)
                    progress.advance(main_task)
                    
                    # 상태 업데이트
                    elapsed = time.time() - self.start_time
                    speed = self.completed_count / (elapsed / 60) if elapsed > 0 else 0
                    
                    progress.update(
                        main_task,
                        status=f"추출: {self.total_extracted:,}개 | 속도: {speed:.1f}/분"
                    )
        else:
            for i in range(len(self.all_combinations)):
                self.process_combination(i)
                print(f"진행: {i+1}/{len(self.all_combinations)} ({(i+1)/len(self.all_combinations)*100:.1f}%)")
    
    def save_data(self):
        """데이터 저장"""
        if not self.data:
            if RICH_AVAILABLE:
                console.print("[red]저장할 데이터가 없습니다.[/red]")
            else:
                print("저장할 데이터가 없습니다.")
            return []
        
        df = pd.DataFrame(self.data)
        
        # 중복 제거
        original_count = len(df)
        df = df.drop_duplicates()
        
        if original_count != len(df):
            if RICH_AVAILABLE:
                console.print(f"[yellow]중복 제거: {original_count} → {len(df)}[/yellow]")
            else:
                print(f"중복 제거: {original_count} → {len(df)}")
        
        # 파일 저장
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        saved_files = []
        
        if 'excel' in self.config['save_formats']:
            excel_file = os.path.join(self.config['output_dir'], f'LGUPlus_지원금정보_{timestamp}.xlsx')
            df.to_excel(excel_file, index=False, engine='openpyxl')
            saved_files.append(excel_file)
            
            if RICH_AVAILABLE:
                console.print(f"[green]Excel 저장:[/green] {excel_file}")
            else:
                print(f"Excel 저장: {excel_file}")
        
        if 'csv' in self.config['save_formats']:
            csv_file = os.path.join(self.config['output_dir'], f'LGUPlus_지원금정보_{timestamp}.csv')
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            saved_files.append(csv_file)
            
            if RICH_AVAILABLE:
                console.print(f"[green]CSV 저장:[/green] {csv_file}")
            else:
                print(f"CSV 저장: {csv_file}")
        
        # 요약 통계
        if RICH_AVAILABLE:
            console.print(f"\n[bold]데이터 요약:[/bold]")
            console.print(f"  - 총 레코드: {len(df):,}개")
            console.print(f"  - 기기 종류: {df['기기명'].nunique():,}개")
            console.print(f"  - 요금제: {df['요금제'].nunique():,}개")
        else:
            print(f"\n데이터 요약:")
            print(f"  - 총 레코드: {len(df)}개")
            print(f"  - 기기 종류: {df['기기명'].nunique()}개")
            print(f"  - 요금제: {df['요금제'].nunique()}개")
        
        return saved_files
    
    def run(self):
        """메인 실행"""
        try:
            if RICH_AVAILABLE:
                console.print(Panel.fit(
                    "[bold cyan]LG U+ 휴대폰 지원금 크롤러 v7.2[/bold cyan]\n"
                    "[yellow]수정 버전[/yellow]",
                    border_style="cyan"
                ))
            else:
                print("="*50)
                print("LG U+ 휴대폰 지원금 크롤러 v7.2")
                print("수정 버전")
                print("="*50)
            
            # 1. 조합 수집
            self.collect_all_combinations()
            
            if not self.all_combinations:
                if RICH_AVAILABLE:
                    console.print("[red]수집된 조합이 없습니다.[/red]")
                else:
                    print("수집된 조합이 없습니다.")
                return []
            
            # 2. 크롤링 실행
            self.start_time = time.time()
            
            if RICH_AVAILABLE:
                console.print(f"[bold cyan]크롤링 시작 (워커: 1개)[/bold cyan]\n")
            else:
                print(f"크롤링 시작 (워커: 1개)\n")
            
            self.run_single_thread_crawling()
            
            # 최종 통계
            elapsed = time.time() - self.start_time
            
            if RICH_AVAILABLE:
                table = Table(title="크롤링 완료", show_header=True, header_style="bold magenta")
                table.add_column("항목", style="cyan", width=20)
                table.add_column("수치", justify="right", style="yellow")
                
                table.add_row("소요 시간", f"{elapsed/60:.1f}분")
                table.add_row("성공", f"{self.completed_count:,}개")
                table.add_row("실패", f"{self.failed_count:,}개")
                table.add_row("총 추출 데이터", f"{self.total_extracted:,}개")
                if self.completed_count > 0:
                    table.add_row("평균 속도", f"{self.completed_count/(elapsed/60):.1f}개/분")
                
                console.print("\n")
                console.print(table)
            else:
                print(f"\n크롤링 완료!")
                print(f"소요 시간: {elapsed/60:.1f}분")
                print(f"성공: {self.completed_count}개")
                print(f"실패: {self.failed_count}개") 
                print(f"총 추출 데이터: {self.total_extracted}개")
            
            # 3. 데이터 저장
            saved_files = self.save_data()
            
            return saved_files
            
        except KeyboardInterrupt:
            if RICH_AVAILABLE:
                console.print("\n[yellow]사용자에 의해 중단되었습니다.[/yellow]")
            else:
                print("\n사용자에 의해 중단되었습니다.")
            return []
            
        except Exception as e:
            if RICH_AVAILABLE:
                console.print(f"\n[red]오류 발생: {str(e)}[/red]")
            else:
                print(f"\n오류 발생: {str(e)}")
            logger.error(traceback.format_exc())
            return []


def main():
    """CLI 인터페이스"""
    parser = argparse.ArgumentParser(
        description='LG U+ 휴대폰 지원금 크롤러 v7.2',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--workers', type=int, default=1,
                        help='동시 실행 워커 수 (기본: 1)')
    parser.add_argument('--max-rate-plans', type=int, default=0,
                        help='최대 요금제 수 (0=전체)')
    parser.add_argument('--show-browser', action='store_true',
                        help='브라우저 표시')
    parser.add_argument('--headless', action='store_true',
                        help='Headless 모드 실행')
    parser.add_argument('--output', type=str, default='data',
                        help='출력 디렉토리')
    parser.add_argument('--test', action='store_true',
                        help='테스트 모드 (처음 3개 조합만)')
    parser.add_argument('--debug', action='store_true',
                        help='디버그 모드')
    
    args = parser.parse_args()
    
    # 설정
    config = {
        'max_workers': args.workers,
        'max_rate_plans': 3 if args.test else args.max_rate_plans,
        'show_browser': args.show_browser,
        'headless': args.headless,
        'output_dir': args.output,
        'debug_screenshots': args.debug,
        'test_mode': args.test
    }
    
    # 테스트 모드에서는 브라우저 표시
    if args.test:
        config['show_browser'] = True
        config['headless'] = False
    
    # 크롤러 실행
    crawler = LGCrawlerV7(config)
    saved_files = crawler.run()
    
    if saved_files:
        if RICH_AVAILABLE:
            console.print(f"\n[bold green]✅ 완료! {len(saved_files)}개 파일 저장됨[/bold green]")
        else:
            print(f"\n✅ 완료! {len(saved_files)}개 파일 저장됨")
        sys.exit(0)
    else:
        if RICH_AVAILABLE:
            console.print("\n[red]⚠️ 저장된 파일이 없습니다.[/red]")
        else:
            print("\n⚠️ 저장된 파일이 없습니다.")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        if RICH_AVAILABLE:
            console.print(Panel(
                "[bold cyan]LG U+ 휴대폰 지원금 크롤러 v7.2[/bold cyan]\n\n"
                "[yellow]수정 버전[/yellow]\n\n"
                "주요 수정사항:\n"
                "  • v3.6 작동 코드 기반 재작성\n"
                "  • 올바른 선택자 사용\n"
                "  • 안정적인 데이터 추출\n"
                "  • 개선된 페이지네이션\n\n"
                "사용법:\n"
                "  [green]python lg_crawler.py[/green]                    # 기본 실행\n"
                "  [green]python lg_crawler.py --test[/green]             # 테스트 모드\n"
                "  [green]python lg_crawler.py --headless[/green]         # Headless 모드\n"
                "  [green]python lg_crawler.py --debug[/green]            # 디버그 모드\n"
                "  [green]python lg_crawler.py --help[/green]             # 도움말",
                border_style="cyan"
            ))
        else:
            print("LG U+ 휴대폰 지원금 크롤러 v7.2")
            print("\n사용법:")
            print("  python lg_crawler.py                    # 기본 실행")
            print("  python lg_crawler.py --test             # 테스트 모드")
            print("  python lg_crawler.py --headless         # Headless 모드")
            print("  python lg_crawler.py --debug            # 디버그 모드")
            print("  python lg_crawler.py --help             # 도움말")
        print()
    
    main()
