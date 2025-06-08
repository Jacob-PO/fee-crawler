#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LG U+ 휴대폰 지원금 통합 크롤러 - v7.0 안정화 버전
문제 해결 및 안정성 개선

주요 개선사항:
    - JavaScript 처리 최적화
    - 더 강력한 요소 대기 메커니즘
    - 상세한 오류 로깅
    - 스크린샷 디버깅 기능
    - 단일/멀티 스레드 모드 지원
    - 안정적인 데이터 추출

작성일: 2025-01-11
버전: 7.0
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

# 로깅 설정 개선
logging.basicConfig(
    level=logging.DEBUG,  # DEBUG 레벨로 변경
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=[
        logging.FileHandler('lg_crawler_debug.log', encoding='utf-8'),
        logging.StreamHandler()  # 콘솔에도 출력
    ]
)
logger = logging.getLogger(__name__)


class LGCrawlerV7:
    """LG U+ 크롤러 v7.0 - 안정화 버전"""
    
    def __init__(self, config=None):
        """초기화"""
        self.base_url = "https://www.lguplus.com/mobile/financing-model"
        self.data = []
        self.data_lock = threading.Lock()
        
        # 기본 설정 (타임아웃 증가)
        self.config = {
            'headless': False,  # 디버깅을 위해 기본값 False
            'page_load_timeout': 30,  # 증가
            'element_wait_timeout': 15,  # 증가
            'max_workers': 1,  # 안정성을 위해 기본값 1
            'retry_count': 3,  # 재시도 횟수 증가
            'output_dir': 'data',
            'save_formats': ['excel', 'csv'],
            'max_rate_plans': 0,
            'minimal_wait': False,  # 안정성을 위해 False
            'show_browser': True,
            'debug_screenshots': True,  # 스크린샷 저장
            'single_thread': False,  # 단일 스레드 모드
            'wait_after_click': 2.0,  # 클릭 후 대기 시간
        }
        
        if config:
            self.config.update(config)
        
        # 디렉토리 생성
        os.makedirs(self.config['output_dir'], exist_ok=True)
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
        """Chrome 드라이버 생성 - 개선된 옵션"""
        chrome_options = Options()
        
        # 기본 옵션
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # User-Agent 설정
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # 성능 옵션 (JavaScript는 활성화 유지)
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-images')  # 이미지만 비활성화
        
        # 추가 안정성 옵션
        chrome_options.add_argument('--disable-logging')
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--disable-features=VizDisplayCompositor')
        
        # Headless 모드
        if self.config['headless'] and not self.config.get('show_browser'):
            chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--window-size=1920,1080')
        else:
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--start-maximized')
        
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(self.config['page_load_timeout'])
            
            # 자동화 감지 우회
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("드라이버 생성 성공")
            return driver
            
        except Exception as e:
            logger.error(f"드라이버 생성 실패: {str(e)}")
            raise
    
    def safe_find_element(self, driver, by, value, timeout=None):
        """안전한 요소 찾기 - 개선된 버전"""
        if timeout is None:
            timeout = self.config['element_wait_timeout']
            
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            # 요소가 실제로 상호작용 가능한지 확인
            WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((by, value))
            )
            return element
        except TimeoutException:
            logger.warning(f"요소를 찾을 수 없음: {by}={value}")
            if self.config['debug_screenshots']:
                self.take_screenshot(driver, f"element_not_found_{value}")
            return None
        except Exception as e:
            logger.error(f"요소 찾기 오류: {str(e)}")
            return None
    
    def take_screenshot(self, driver, name):
        """디버깅용 스크린샷"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"debug_screenshots/{name}_{timestamp}.png"
            driver.save_screenshot(filename)
            logger.debug(f"스크린샷 저장: {filename}")
        except:
            pass
    
    def wait_for_page_load(self, driver, timeout=None):
        """페이지 로딩 대기"""
        if timeout is None:
            timeout = self.config['page_load_timeout']
            
        try:
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            time.sleep(1)  # 추가 안정성
            return True
        except:
            logger.warning("페이지 로딩 타임아웃")
            return False
    
    def collect_all_combinations(self):
        """모든 조합 수집 - 개선된 버전"""
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
                # Rich가 없을 때
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
            self.take_screenshot(driver, "combination_collection_error")
            
        finally:
            driver.quit()
    
    def _collect_rate_plans_for_combination(self, driver, sub_value, sub_name, dev_value, dev_name):
        """특정 조합의 요금제 수집 - 개선된 버전"""
        try:
            logger.info(f"요금제 수집 시작: {sub_name} - {dev_name}")
            
            # 페이지 로드
            driver.get(self.base_url)
            self.wait_for_page_load(driver)
            time.sleep(2)
            
            # 가입유형 선택
            sub_selector = f'input[name="가입유형"][value="{sub_value}"]'
            sub_radio = self.safe_find_element(driver, By.CSS_SELECTOR, sub_selector)
            if not sub_radio:
                logger.error(f"가입유형 라디오 버튼을 찾을 수 없음: {sub_selector}")
                self.take_screenshot(driver, f"sub_radio_not_found_{sub_value}")
                return []
                
            if not sub_radio.is_selected():
                # 라벨 클릭으로 선택
                label_selector = f'label[for="{sub_value}"]'
                label = self.safe_find_element(driver, By.CSS_SELECTOR, label_selector)
                if label:
                    driver.execute_script("arguments[0].scrollIntoView(true);", label)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", label)
                    time.sleep(self.config['wait_after_click'])
                    logger.debug(f"가입유형 선택: {sub_name}")
            
            # 기기종류 선택
            dev_selector = f'input[name="기기종류"][value="{dev_value}"]'
            dev_radio = self.safe_find_element(driver, By.CSS_SELECTOR, dev_selector)
            if not dev_radio:
                logger.error(f"기기종류 라디오 버튼을 찾을 수 없음: {dev_selector}")
                self.take_screenshot(driver, f"dev_radio_not_found_{dev_value}")
                return []
                
            if not dev_radio.is_selected():
                label_selector = f'label[for="{dev_value}"]'
                label = self.safe_find_element(driver, By.CSS_SELECTOR, label_selector)
                if label:
                    driver.execute_script("arguments[0].scrollIntoView(true);", label)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", label)
                    time.sleep(self.config['wait_after_click'])
                    logger.debug(f"기기종류 선택: {dev_name}")
            
            # 요금제 모달 열기 - 다양한 선택자 시도
            more_btn_selectors = [
                'button.c-btn-rect-2',
                'button[type="button"].c-btn-rect-2',
                'button:contains("더보기")',
                '//button[contains(text(), "더보기")]',
                '//button[contains(@class, "c-btn-rect-2")]'
            ]
            
            more_btn = None
            for selector in more_btn_selectors:
                if selector.startswith('//'):
                    more_btn = self.safe_find_element(driver, By.XPATH, selector)
                else:
                    more_btn = self.safe_find_element(driver, By.CSS_SELECTOR, selector)
                
                if more_btn:
                    logger.debug(f"더보기 버튼 찾음: {selector}")
                    break
            
            if not more_btn:
                logger.error("더보기 버튼을 찾을 수 없음")
                self.take_screenshot(driver, "more_button_not_found")
                return []
            
            # 버튼 클릭
            driver.execute_script("arguments[0].scrollIntoView(true);", more_btn)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", more_btn)
            time.sleep(2)  # 모달 열림 대기
            
            # 요금제 수집
            rate_plans = []
            
            # 다양한 선택자로 시도
            section_selectors = [
                'div.c-section',
                'div[class*="section"]',
                'div.modal-content div'
            ]
            
            sections = []
            for selector in section_selectors:
                sections = driver.find_elements(By.CSS_SELECTOR, selector)
                if sections:
                    logger.debug(f"섹션 찾음: {selector}, 개수: {len(sections)}")
                    break
            
            if not sections:
                logger.warning("요금제 섹션을 찾을 수 없음")
                self.take_screenshot(driver, "sections_not_found")
                
                # 대체 방법: 모든 라디오 버튼 찾기
                radios = driver.find_elements(By.CSS_SELECTOR, 'input[type="radio"][name*="요금"]')
                logger.debug(f"대체 방법으로 찾은 라디오 버튼: {len(radios)}개")
                
                for radio in radios:
                    try:
                        plan_id = radio.get_attribute('id')
                        plan_value = radio.get_attribute('value')
                        
                        # 라벨 찾기
                        label = driver.find_element(By.CSS_SELECTOR, f'label[for="{plan_id}"]')
                        plan_name = label.text.strip()
                        
                        if plan_name and plan_name not in ['기기변경', '번호이동', '신규가입', '5G폰', 'LTE폰', '전체']:
                            rate_plans.append({
                                'id': plan_id,
                                'value': plan_value,
                                'name': plan_name
                            })
                            logger.debug(f"요금제 추가: {plan_name}")
                    except:
                        continue
            else:
                # 섹션별로 요금제 수집
                for section in sections:
                    radios = section.find_elements(By.CSS_SELECTOR, 'input[type="radio"]')
                    
                    for radio in radios:
                        try:
                            plan_id = radio.get_attribute('id')
                            plan_value = radio.get_attribute('value')
                            label = driver.find_element(By.CSS_SELECTOR, f'label[for="{plan_id}"]')
                            plan_name = label.text.strip()
                            
                            if plan_name and plan_name not in ['기기변경', '번호이동', '신규가입', '5G폰', 'LTE폰', '전체']:
                                rate_plans.append({
                                    'id': plan_id,
                                    'value': plan_value,
                                    'name': plan_name
                                })
                                logger.debug(f"요금제 추가: {plan_name}")
                        except:
                            continue
            
            logger.info(f"수집된 요금제 수: {len(rate_plans)}")
            
            # 모달 닫기
            close_selectors = [
                'button.c-btn-close',
                'button[class*="close"]',
                'button[aria-label="닫기"]',
                '//button[contains(@class, "close")]'
            ]
            
            for selector in close_selectors:
                if selector.startswith('//'):
                    close_btn = self.safe_find_element(driver, By.XPATH, selector, timeout=3)
                else:
                    close_btn = self.safe_find_element(driver, By.CSS_SELECTOR, selector, timeout=3)
                
                if close_btn:
                    driver.execute_script("arguments[0].click();", close_btn)
                    time.sleep(1)
                    break
            
            return rate_plans
            
        except Exception as e:
            logger.error(f"요금제 수집 오류 ({sub_name} - {dev_name}): {str(e)}")
            logger.error(traceback.format_exc())
            self.take_screenshot(driver, f"rate_plan_collection_error_{sub_name}_{dev_name}")
            return []
    
    def process_combination(self, combo_index, progress=None, task_id=None):
        """단일 조합 처리 - 개선된 버전"""
        combo = self.all_combinations[combo_index]
        driver = None
        thread_id = threading.current_thread().name
        
        # 현재 작업 상태 업데이트
        with self.status_lock:
            self.current_tasks[thread_id] = f"{combo['sub_name']} - {combo['dev_name']} - {combo['rate_plan']['name'][:30]}"
        
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
                    self.wait_for_page_load(driver)
                    time.sleep(2)
                    
                    # 옵션 선택
                    if not self._select_options(driver, combo):
                        logger.warning(f"옵션 선택 실패 (재시도 {retry+1}/{self.config['retry_count']})")
                        continue
                    
                    # 데이터 추출
                    extracted_data = self._extract_data(driver, combo)
                    
                    if extracted_data:
                        # 데이터 저장
                        with self.data_lock:
                            self.data.extend(extracted_data)
                            self.total_extracted += len(extracted_data)
                            self.completed_count += 1
                        
                        logger.info(f"✓ [{combo_index+1}] 성공: {len(extracted_data)}개 추출")
                        
                        if RICH_AVAILABLE and len(extracted_data) > 0:
                            console.print(f"[green]✓[/green] [{combo_index+1}/{len(self.all_combinations)}] {combo['rate_plan']['name'][:40]}... - [bold]{len(extracted_data)}개[/bold]")
                        
                        return True
                    else:
                        logger.warning(f"데이터 추출 실패 (재시도 {retry+1}/{self.config['retry_count']})")
                        
                except Exception as e:
                    logger.error(f"재시도 중 오류 ({retry+1}/{self.config['retry_count']}): {str(e)}")
                    if retry < self.config['retry_count'] - 1:
                        time.sleep(2)  # 재시도 전 대기
            
            # 모든 재시도 실패
            with self.status_lock:
                self.failed_count += 1
            logger.error(f"✗ [{combo_index+1}] 최종 실패")
            return False
                
        except Exception as e:
            logger.error(f"처리 오류 [{combo_index+1}]: {str(e)}")
            logger.error(traceback.format_exc())
            with self.status_lock:
                self.failed_count += 1
            return False
            
        finally:
            if driver:
                driver.quit()
            # 작업 상태 제거
            with self.status_lock:
                self.current_tasks.pop(thread_id, None)
    
    def _select_options(self, driver, combo):
        """옵션 선택 - 개선된 버전"""
        try:
            # 가입유형 선택
            sub_selector = f'input[name="가입유형"][value="{combo["sub_value"]}"]'
            sub_radio = self.safe_find_element(driver, By.CSS_SELECTOR, sub_selector)
            if not sub_radio:
                logger.error(f"가입유형 라디오 버튼을 찾을 수 없음")
                return False
                
            if not sub_radio.is_selected():
                label = driver.find_element(By.CSS_SELECTOR, f'label[for="{combo["sub_value"]}"]')
                driver.execute_script("arguments[0].scrollIntoView(true);", label)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", label)
                time.sleep(self.config['wait_after_click'])
            
            # 기기종류 선택
            dev_selector = f'input[name="기기종류"][value="{combo["dev_value"]}"]'
            dev_radio = self.safe_find_element(driver, By.CSS_SELECTOR, dev_selector)
            if not dev_radio:
                logger.error(f"기기종류 라디오 버튼을 찾을 수 없음")
                return False
                
            if not dev_radio.is_selected():
                label = driver.find_element(By.CSS_SELECTOR, f'label[for="{combo["dev_value"]}"]')
                driver.execute_script("arguments[0].scrollIntoView(true);", label)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", label)
                time.sleep(self.config['wait_after_click'])
            
            # 요금제 선택
            more_btn = self.safe_find_element(driver, By.CSS_SELECTOR, 'button.c-btn-rect-2')
            if not more_btn:
                # 대체 선택자 시도
                more_btn = self.safe_find_element(driver, By.XPATH, '//button[contains(@class, "c-btn-rect-2")]')
            
            if more_btn:
                driver.execute_script("arguments[0].scrollIntoView(true);", more_btn)
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", more_btn)
                time.sleep(2)  # 모달 열림 대기
                
                # 요금제 라디오 선택
                rate_radio = self.safe_find_element(driver, By.CSS_SELECTOR, f'input[id="{combo["rate_plan"]["id"]}"]')
                if not rate_radio:
                    logger.error(f"요금제 라디오 버튼을 찾을 수 없음: {combo['rate_plan']['id']}")
                    return False
                    
                if not rate_radio.is_selected():
                    label = driver.find_element(By.CSS_SELECTOR, f'label[for="{combo["rate_plan"]["id"]}"]')
                    driver.execute_script("arguments[0].scrollIntoView(true);", label)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", label)
                    time.sleep(0.5)
                
                # 적용 버튼
                apply_btn = self.safe_find_element(driver, By.CSS_SELECTOR, 'button.c-btn-solid-1-m')
                if not apply_btn:
                    # 대체 선택자
                    apply_btn = self.safe_find_element(driver, By.XPATH, '//button[contains(text(), "적용")]')
                
                if apply_btn:
                    driver.execute_script("arguments[0].click();", apply_btn)
                    time.sleep(2)
                else:
                    logger.error("적용 버튼을 찾을 수 없음")
                    return False
            else:
                logger.error("더보기 버튼을 찾을 수 없음")
                return False
            
            # 제조사 전체 선택
            all_checkbox = self.safe_find_element(driver, By.CSS_SELECTOR, 'input[id="전체"]')
            if not all_checkbox:
                # 대체 선택자
                all_checkbox = self.safe_find_element(driver, By.CSS_SELECTOR, 'input[value="전체"]')
            
            if all_checkbox and not all_checkbox.is_selected():
                label = driver.find_element(By.CSS_SELECTOR, 'label[for="전체"]')
                driver.execute_script("arguments[0].click();", label)
                time.sleep(1.5)
            
            return True
            
        except Exception as e:
            logger.error(f"옵션 선택 오류: {str(e)}")
            self.take_screenshot(driver, "option_selection_error")
            return False
    
    def _extract_data(self, driver, combo):
        """데이터 추출 - 개선된 버전"""
        all_data = []
        page = 1
        max_pages = 10
        empty_page_count = 0
        
        while page <= max_pages:
            try:
                # 테이블 찾기 - 다양한 선택자 시도
                table_selectors = [
                    'table.c-table',
                    'table',
                    '//table',
                    'div.table-responsive table'
                ]
                
                table = None
                for selector in table_selectors:
                    if selector.startswith('//'):
                        tables = driver.find_elements(By.XPATH, selector)
                    else:
                        tables = driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    if tables:
                        table = tables[0]
                        logger.debug(f"테이블 찾음: {selector}")
                        break
                
                if not table:
                    logger.warning(f"페이지 {page}: 테이블을 찾을 수 없음")
                    self.take_screenshot(driver, f"no_table_page_{page}")
                    break
                
                # 테이블 행 찾기
                rows = table.find_elements(By.CSS_SELECTOR, 'tbody tr')
                if not rows:
                    # 대체 선택자
                    rows = table.find_elements(By.XPATH, './/tr[position()>1]')
                
                if not rows:
                    empty_page_count += 1
                    if empty_page_count >= 2:
                        logger.info(f"빈 페이지 연속 {empty_page_count}회 - 추출 종료")
                        break
                    logger.warning(f"페이지 {page}: 데이터 행 없음")
                else:
                    empty_page_count = 0
                    logger.debug(f"페이지 {page}: {len(rows)}개 행 발견")
                
                # 현재 페이지 데이터 추출
                page_data = self._parse_table_rows(rows, combo)
                if page_data:
                    all_data.extend(page_data)
                    logger.info(f"페이지 {page}: {len(page_data)}개 데이터 추출")
                
                # 다음 페이지 확인
                next_exists = False
                pagination_selectors = [
                    'ul.pagination',
                    'div.pagination',
                    'nav[aria-label="pagination"]'
                ]
                
                for selector in pagination_selectors:
                    try:
                        pagination = driver.find_element(By.CSS_SELECTOR, selector)
                        next_button = self._find_next_page_button(pagination)
                        
                        if next_button:
                            driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                            time.sleep(0.5)
                            driver.execute_script("arguments[0].click();", next_button)
                            time.sleep(2)  # 페이지 로딩 대기
                            page += 1
                            next_exists = True
                            break
                    except:
                        continue
                
                if not next_exists:
                    logger.info(f"마지막 페이지 도달 (페이지 {page})")
                    break
                    
            except Exception as e:
                logger.error(f"페이지 {page} 추출 오류: {str(e)}")
                self.take_screenshot(driver, f"extraction_error_page_{page}")
                break
        
        logger.info(f"총 {len(all_data)}개 데이터 추출 완료")
        return all_data
    
    def _parse_table_rows(self, rows, combo):
        """테이블 행 파싱 - 개선된 버전"""
        data_list = []
        current_device = None
        current_price = None
        current_date = None
        
        for i, row in enumerate(rows):
            try:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if not cells:
                    continue
                
                # 셀 내용 로깅 (디버깅용)
                if i < 3:  # 처음 3개 행만
                    cell_texts = [cell.text.strip() for cell in cells]
                    logger.debug(f"행 {i+1} 셀 내용: {cell_texts}")
                
                # 새 기기 정보가 있는 행 (rowspan 있는 경우)
                if len(cells) >= 9:
                    # 첫 번째 셀에 rowspan이 있는지 확인
                    if cells[0].get_attribute('rowspan'):
                        current_device = cells[0].text.strip()
                        current_price = cells[1].text.strip()
                        current_date = cells[2].text.strip()
                        
                        data = self._create_data_dict(
                            cells[3:], combo, current_device, current_price, current_date
                        )
                        if data:
                            data_list.append(data)
                    else:
                        # rowspan이 없지만 전체 데이터가 있는 경우
                        data = self._create_data_dict(
                            cells[3:], combo, cells[0].text.strip(), cells[1].text.strip(), cells[2].text.strip()
                        )
                        if data:
                            data_list.append(data)
                
                # 기존 기기의 다른 약정 (rowspan으로 인해 셀이 적은 경우)
                elif len(cells) >= 6 and current_device:
                    data = self._create_data_dict(
                        cells, combo, current_device, current_price, current_date
                    )
                    if data:
                        data_list.append(data)
                        
            except Exception as e:
                logger.debug(f"행 {i+1} 파싱 오류: {str(e)}")
                continue
        
        logger.debug(f"파싱 결과: {len(data_list)}개 데이터")
        return data_list
    
    def _create_data_dict(self, cells, combo, device, price, date):
        """데이터 딕셔너리 생성 - 개선된 버전"""
        try:
            if len(cells) >= 6:
                # 숫자 추출 헬퍼 함수
                def extract_number(text):
                    # "원", ",", 공백 제거하고 숫자만 추출
                    cleaned = re.sub(r'[^\d-]', '', text)
                    return cleaned if cleaned else '0'
                
                data = {
                    '가입유형': combo['sub_name'],
                    '기기종류': combo['dev_name'],
                    '제조사': '전체',
                    '요금제': combo['rate_plan']['name'],
                    '요금제ID': combo['rate_plan'].get('value', ''),
                    '월납부금액': '0',  # 필요시 추가 조회
                    '기기명': device,
                    '출고가': extract_number(price),
                    '공시일자': date,
                    '요금제유지기간': cells[0].text.strip(),
                    '공시지원금': extract_number(cells[1].text),
                    '추가공시지원금': extract_number(cells[2].text),
                    '지원금총액': extract_number(cells[3].text),
                    '추천할인': extract_number(cells[4].text),
                    '최종구매가': extract_number(cells[5].text),
                    '크롤링시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # 데이터 검증
                if device and device != '-' and price != '0':
                    return data
                else:
                    logger.debug(f"유효하지 않은 데이터: 기기명={device}, 출고가={price}")
                    return None
            else:
                logger.debug(f"셀 개수 부족: {len(cells)}개")
                return None
                
        except Exception as e:
            logger.debug(f"데이터 생성 오류: {str(e)}")
            return None
    
    def _find_next_page_button(self, pagination):
        """다음 페이지 버튼 찾기 - 개선된 버전"""
        try:
            # 현재 활성 페이지 찾기
            active_page = pagination.find_element(By.CSS_SELECTOR, 'li.active')
            current_page_num = int(active_page.text.strip())
            logger.debug(f"현재 페이지: {current_page_num}")
            
            # 다음 페이지 번호 버튼 찾기
            next_page_num = current_page_num + 1
            buttons = pagination.find_elements(By.CSS_SELECTOR, f'li button')
            
            for button in buttons:
                if button.text.strip() == str(next_page_num):
                    return button
            
            # 다음 버튼 (화살표) 찾기
            next_buttons = pagination.find_elements(By.CSS_SELECTOR, 'li:not(.disabled) button[aria-label*="다음"]')
            if next_buttons:
                return next_buttons[0]
            
            # > 버튼 찾기
            next_arrow = pagination.find_elements(By.XPATH, './/button[contains(text(), ">")]')
            if next_arrow:
                parent_li = next_arrow[0].find_element(By.XPATH, '..')
                if 'disabled' not in parent_li.get_attribute('class'):
                    return next_arrow[0]
            
            return None
            
        except Exception as e:
            logger.debug(f"다음 페이지 버튼 찾기 오류: {str(e)}")
            return None
    
    def run_parallel_crawling(self):
        """병렬 크롤링 실행 - 개선된 버전"""
        self.start_time = time.time()
        
        # 단일 스레드 모드 확인
        if self.config.get('single_thread', False):
            self.config['max_workers'] = 1
        
        if RICH_AVAILABLE:
            console.print(f"[bold cyan]크롤링 시작 (워커: {self.config['max_workers']}개)[/bold cyan]\n")
        else:
            print(f"크롤링 시작 (워커: {self.config['max_workers']}개)\n")
        
        # 단일 스레드 모드
        if self.config['max_workers'] == 1:
            self.run_single_thread_crawling()
        else:
            # 멀티 스레드 모드
            self.run_multi_thread_crawling()
    
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
    
    def run_multi_thread_crawling(self):
        """멀티 스레드 크롤링"""
        with ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            
            if RICH_AVAILABLE:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TextColumn("• {task.fields[status]}"),
                    TimeRemainingColumn(),
                    console=console,
                    refresh_per_second=2
                ) as progress:
                    
                    main_task = progress.add_task(
                        "[green]전체 진행률",
                        total=len(self.all_combinations),
                        status=f"추출: 0개"
                    )
                    
                    futures = []
                    for i in range(len(self.all_combinations)):
                        future = executor.submit(self.process_combination, i, progress, main_task)
                        futures.append(future)
                    
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            progress.advance(main_task)
                            
                            elapsed = time.time() - self.start_time
                            speed = self.completed_count / (elapsed / 60) if elapsed > 0 else 0
                            
                            progress.update(
                                main_task,
                                status=f"추출: {self.total_extracted:,}개 | 속도: {speed:.1f}/분"
                            )
                            
                        except Exception as e:
                            logger.error(f"Future 오류: {str(e)}")
                            progress.advance(main_task)
            else:
                futures = []
                for i in range(len(self.all_combinations)):
                    future = executor.submit(self.process_combination, i)
                    futures.append(future)
                
                completed = 0
                for future in as_completed(futures):
                    completed += 1
                    print(f"진행: {completed}/{len(self.all_combinations)} ({completed/len(self.all_combinations)*100:.1f}%)")
        
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
        
        # 데이터 정렬
        df = df.sort_values(['가입유형', '기기종류', '요금제', '기기명'])
        
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
                    "[bold cyan]LG U+ 휴대폰 지원금 크롤러 v7.0[/bold cyan]\n"
                    "[yellow]안정화 버전[/yellow]",
                    border_style="cyan"
                ))
            else:
                print("="*50)
                print("LG U+ 휴대폰 지원금 크롤러 v7.0")
                print("안정화 버전")
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
            self.run_parallel_crawling()
            
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
        description='LG U+ 휴대폰 지원금 크롤러 v7.0',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--workers', type=int, default=1,
                        help='동시 실행 워커 수 (기본: 1, 안정성)')
    parser.add_argument('--max-rate-plans', type=int, default=0,
                        help='최대 요금제 수 (0=전체)')
    parser.add_argument('--show-browser', action='store_true',
                        help='브라우저 표시')
    parser.add_argument('--headless', action='store_true',
                        help='Headless 모드 강제 실행')
    parser.add_argument('--output', type=str, default='data',
                        help='출력 디렉토리')
    parser.add_argument('--test', action='store_true',
                        help='테스트 모드 (처음 3개 조합만)')
    parser.add_argument('--single-thread', action='store_true',
                        help='단일 스레드 모드 (안정성)')
    parser.add_argument('--debug', action='store_true',
                        help='디버그 모드 (스크린샷 저장)')
    parser.add_argument('--retry', type=int, default=3,
                        help='재시도 횟수 (기본: 3)')
    
    args = parser.parse_args()
    
    # 설정
    config = {
        'max_workers': 1 if args.single_thread else args.workers,
        'max_rate_plans': 3 if args.test else args.max_rate_plans,
        'show_browser': args.show_browser,
        'headless': args.headless,
        'output_dir': args.output,
        'single_thread': args.single_thread,
        'debug_screenshots': args.debug,
        'retry_count': args.retry
    }
    
    # 테스트 모드에서는 브라우저 표시
    if args.test:
        config['show_browser'] = True
        config['headless'] = False
        config['debug_screenshots'] = True
    
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
                "[bold cyan]LG U+ 휴대폰 지원금 크롤러 v7.0[/bold cyan]\n\n"
                "[yellow]안정화 버전[/yellow]\n\n"
                "주요 특징:\n"
                "  • 개선된 요소 감지 및 대기 메커니즘\n"
                "  • 상세한 오류 로깅 및 디버깅\n"
                "  • 스크린샷 디버깅 기능\n"
                "  • 단일/멀티 스레드 모드 지원\n"
                "  • 강화된 재시도 메커니즘\n\n"
                "사용법:\n"
                "  [green]python lg_crawler.py[/green]                    # 기본 실행 (안정성)\n"
                "  [green]python lg_crawler.py --test[/green]             # 테스트 모드 (3개만)\n"
                "  [green]python lg_crawler.py --workers 5[/green]        # 멀티스레드 (5개)\n"
                "  [green]python lg_crawler.py --single-thread[/green]    # 단일 스레드\n"
                "  [green]python lg_crawler.py --debug[/green]            # 디버그 모드\n"
                "  [green]python lg_crawler.py --help[/green]             # 도움말",
                border_style="cyan"
            ))
        else:
            print("LG U+ 휴대폰 지원금 크롤러 v7.0")
            print("\n사용법:")
            print("  python lg_crawler.py                    # 기본 실행")
            print("  python lg_crawler.py --test             # 테스트 모드")
            print("  python lg_crawler.py --workers 5        # 멀티스레드")
            print("  python lg_crawler.py --single-thread    # 단일 스레드")
            print("  python lg_crawler.py --debug            # 디버그 모드")
            print("  python lg_crawler.py --help             # 도움말")
        print()
    
    main()
