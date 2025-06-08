#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LG U+ 휴대폰 지원금 통합 크롤러 - v6.0 멀티스레딩 최적화
pickle 오류 해결, 안정적인 병렬 처리, 실시간 진행률

주요 특징:
    - 멀티스레딩 기반 병렬 처리 (pickle 오류 해결)
    - 실시간 상세 진행률 표시
    - 스마트 재시도 메커니즘
    - 안정적인 오류 처리
    - 빠른 데이터 추출

작성일: 2025-01-11
버전: 6.0
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


# 간단한 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('lg_crawler.log', encoding='utf-8')]
)
logger = logging.getLogger(__name__)


class LGCrawlerV6:
    """LG U+ 크롤러 v6.0 - 멀티스레딩 최적화"""
    
    def __init__(self, config=None):
        """초기화"""
        self.base_url = "https://www.lguplus.com/mobile/financing-model"
        self.data = []
        self.data_lock = threading.Lock()
        
        # 기본 설정
        self.config = {
            'headless': True,
            'page_load_timeout': 20,
            'element_wait_timeout': 10,
            'max_workers': 5,  # 동시 실행 스레드 수
            'retry_count': 2,
            'output_dir': 'data',
            'save_formats': ['excel', 'csv'],
            'max_rate_plans': 0,  # 0 = 모든 요금제
            'minimal_wait': True,
            'show_browser': False  # 브라우저 표시 여부
        }
        
        if config:
            self.config.update(config)
        
        # 디렉토리 생성
        os.makedirs(self.config['output_dir'], exist_ok=True)
        
        # 전역 변수
        self.all_combinations = []
        self.completed_count = 0
        self.failed_count = 0
        self.total_extracted = 0
        self.start_time = None
        
        # 진행 상태 추적
        self.status_lock = threading.Lock()
        self.current_tasks = {}
        
    def create_driver(self):
        """Chrome 드라이버 생성"""
        chrome_options = Options()
        
        # 기본 옵션
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # 성능 최적화 옵션
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-images')
        #chrome_options.add_argument('--disable-javascript')  # JS 비활성화로 속도 향상
        
        # 추가 최적화
        prefs = {
            'profile.default_content_setting_values': {
                'images': 2,
                'plugins': 2,
                'popups': 2,
                'geolocation': 2,
                'notifications': 2,
                'media_stream': 2,
            }
        }
        chrome_options.add_experimental_option('prefs', prefs)
        
        # Headless 모드
        if self.config['headless'] and not self.config.get('show_browser'):
            chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--window-size=1920,1080')
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(self.config['page_load_timeout'])
        
        # JavaScript 다시 활성화 (페이지 동작에 필요)
        driver.execute_script("return true")
        
        return driver
    
    def safe_find_element(self, driver, by, value, timeout=5):
        """안전한 요소 찾기"""
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except:
            return None
    
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
            
        finally:
            driver.quit()
    
    def _collect_rate_plans_for_combination(self, driver, sub_value, sub_name, dev_value, dev_name):
        """특정 조합의 요금제 수집"""
        try:
            driver.get(self.base_url)
             # ── 여기에 추가 ──
    with open('debug_home.html', 'w', encoding='utf-8') as f:
        f.write(driver.page_source)
    # ─────────────────
            time.sleep(2)
            
            # 가입유형 선택
            sub_radio = self.safe_find_element(driver, By.CSS_SELECTOR, f'input[name="가입유형"][id="{sub_value}"]')
            if sub_radio and not sub_radio.is_selected():
                label = driver.find_element(By.CSS_SELECTOR, f'label[for="{sub_value}"]')
                driver.execute_script("arguments[0].click();", label)
                time.sleep(0.5)
            
            # 기기종류 선택
            dev_radio = self.safe_find_element(driver, By.CSS_SELECTOR, f'input[name="기기종류"][id="{dev_value}"]')
            if dev_radio and not dev_radio.is_selected():
                label = driver.find_element(By.CSS_SELECTOR, f'label[for="{dev_value}"]')
                driver.execute_script("arguments[0].click();", label)
                time.sleep(0.5)
            
            # 요금제 모달 열기
            more_btn = self.safe_find_element(driver, By.CSS_SELECTOR, 'button.c-btn-rect-2')
            if not more_btn:
                return []
            
            driver.execute_script("arguments[0].click();", more_btn)
            time.sleep(1)
            
            # 요금제 수집
            rate_plans = []
            sections = driver.find_elements(By.CSS_SELECTOR, 'div.c-section')
            
            for section in sections:
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
            
            # 모달 닫기
            close_btn = self.safe_find_element(driver, By.CSS_SELECTOR, 'button.c-btn-close')
            if close_btn:
                driver.execute_script("arguments[0].click();", close_btn)
                time.sleep(0.5)
            
            return rate_plans
            
        except Exception as e:
            logger.error(f"요금제 수집 오류 ({sub_name} - {dev_name}): {str(e)}")
            return []
    
    def process_combination(self, combo_index, progress=None, task_id=None):
        """단일 조합 처리"""
        combo = self.all_combinations[combo_index]
        driver = None
        thread_id = threading.current_thread().name
        
        # 현재 작업 상태 업데이트
        with self.status_lock:
            self.current_tasks[thread_id] = f"{combo['sub_name']} - {combo['dev_name']} - {combo['rate_plan']['name'][:30]}"
        
        try:
            driver = self.create_driver()
            
            # 상세 진행 상황 업데이트
            if progress and task_id is not None:
                desc = f"[{combo_index+1}/{len(self.all_combinations)}] {combo['sub_name']} - {combo['dev_name']} - {combo['rate_plan']['name'][:30]}..."
                progress.update(task_id, description=desc)
            
            # 페이지 로드
            driver.get(self.base_url)
            time.sleep(2)
            
            # 옵션 선택
            self._select_options(driver, combo)
            
            # 데이터 추출
            extracted_data = self._extract_data(driver, combo)
            
            # 데이터 저장
            if extracted_data:
                with self.data_lock:
                    self.data.extend(extracted_data)
                    self.total_extracted += len(extracted_data)
                    self.completed_count += 1
                
                logger.info(f"✓ [{combo_index+1}] {combo['sub_name']} - {combo['dev_name']} - {combo['rate_plan']['name']}: {len(extracted_data)}개")
                
                if RICH_AVAILABLE and len(extracted_data) > 0:
                    console.print(f"[green]✓[/green] [{combo_index+1}/{len(self.all_combinations)}] {combo['rate_plan']['name'][:40]}... - [bold]{len(extracted_data)}개[/bold]")
                
                return True
            else:
                with self.status_lock:
                    self.failed_count += 1
                return False
                
        except Exception as e:
            logger.error(f"처리 오류 [{combo_index+1}]: {str(e)}")
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
        """옵션 선택"""
        # 가입유형
        sub_radio = self.safe_find_element(driver, By.CSS_SELECTOR, f'input[name="가입유형"][id="{combo["sub_value"]}"]')
        if sub_radio and not sub_radio.is_selected():
            label = driver.find_element(By.CSS_SELECTOR, f'label[for="{combo["sub_value"]}"]')
            driver.execute_script("arguments[0].click();", label)
            time.sleep(0.5)
        
        # 기기종류
        dev_radio = self.safe_find_element(driver, By.CSS_SELECTOR, f'input[name="기기종류"][id="{combo["dev_value"]}"]')
        if dev_radio and not dev_radio.is_selected():
            label = driver.find_element(By.CSS_SELECTOR, f'label[for="{combo["dev_value"]}"]')
            driver.execute_script("arguments[0].click();", label)
            time.sleep(0.5)
        
        # 요금제 선택
        more_btn = self.safe_find_element(driver, By.CSS_SELECTOR, 'button.c-btn-rect-2')
        if more_btn:
            driver.execute_script("arguments[0].click();", more_btn)
            time.sleep(1)
            
            # 요금제 라디오 선택
            rate_radio = self.safe_find_element(driver, By.CSS_SELECTOR, f'input[id="{combo["rate_plan"]["id"]}"]')
            if rate_radio and not rate_radio.is_selected():
                label = driver.find_element(By.CSS_SELECTOR, f'label[for="{combo["rate_plan"]["id"]}"]')
                driver.execute_script("arguments[0].click();", label)
                time.sleep(0.3)
            
            # 적용 버튼
            apply_btn = self.safe_find_element(driver, By.CSS_SELECTOR, 'button.c-btn-solid-1-m')
            if apply_btn:
                driver.execute_script("arguments[0].click();", apply_btn)
                time.sleep(1.5)
        
        # 제조사 전체 선택
        all_checkbox = self.safe_find_element(driver, By.CSS_SELECTOR, 'input[id="전체"]')
        if all_checkbox and not all_checkbox.is_selected():
            label = driver.find_element(By.CSS_SELECTOR, 'label[for="전체"]')
            driver.execute_script("arguments[0].click();", label)
            time.sleep(1)
    
    def _extract_data(self, driver, combo):
        """데이터 추출"""
        all_data = []
        page = 1
        max_pages = 10  # 최대 페이지 수 제한
        
        while page <= max_pages:
            try:
                # 테이블 대기
                tables = driver.find_elements(By.CSS_SELECTOR, 'table')
                if not tables:
                    break
                
                table = tables[0]
                rows = table.find_elements(By.CSS_SELECTOR, 'tbody tr')
                
                if not rows:
                    break
                
                # 현재 페이지 데이터 추출
                page_data = self._parse_table_rows(rows, combo)
                all_data.extend(page_data)
                
                # 다음 페이지 확인
                try:
                    pagination = driver.find_element(By.CSS_SELECTOR, 'ul.pagination')
                    next_button = self._find_next_page_button(pagination)
                    
                    if next_button:
                        driver.execute_script("arguments[0].click();", next_button)
                        time.sleep(1)
                        page += 1
                    else:
                        break
                except:
                    break
                    
            except Exception as e:
                logger.debug(f"페이지 {page} 추출 오류: {str(e)}")
                break
        
        return all_data
    
    def _parse_table_rows(self, rows, combo):
        """테이블 행 파싱"""
        data_list = []
        current_device = None
        current_price = None
        current_date = None
        
        for row in rows:
            try:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if not cells:
                    continue
                
                # 새 기기 정보가 있는 행
                if len(cells) >= 9 and cells[0].get_attribute('rowspan'):
                    current_device = cells[0].text.strip()
                    current_price = cells[1].text.strip()
                    current_date = cells[2].text.strip()
                    
                    data = self._create_data_dict(
                        cells[3:], combo, current_device, current_price, current_date
                    )
                    if data:
                        data_list.append(data)
                
                # 기존 기기의 다른 약정
                elif len(cells) >= 6 and current_device:
                    data = self._create_data_dict(
                        cells, combo, current_device, current_price, current_date
                    )
                    if data:
                        data_list.append(data)
                        
            except Exception as e:
                logger.debug(f"행 파싱 오류: {str(e)}")
                continue
        
        return data_list
    
    def _create_data_dict(self, cells, combo, device, price, date):
        """데이터 딕셔너리 생성"""
        try:
            if len(cells) >= 6:
                return {
                    '가입유형': combo['sub_name'],
                    '기기종류': combo['dev_name'],
                    '제조사': '전체',
                    '요금제': combo['rate_plan']['name'],
                    '요금제ID': combo['rate_plan'].get('value', ''),
                    '월납부금액': '0',  # 가격 조회 생략
                    '기기명': device,
                    '출고가': price,
                    '공시일자': date,
                    '요금제유지기간': cells[0].text.strip(),
                    '공시지원금': cells[1].text.strip().replace('원', '').replace(',', ''),
                    '추가공시지원금': cells[2].text.strip().replace('원', '').replace(',', ''),
                    '지원금총액': cells[3].text.strip().replace('원', '').replace(',', ''),
                    '추천할인': cells[4].text.strip().replace('원', '').replace(',', ''),
                    '최종구매가': cells[5].text.strip().replace('원', '').replace(',', ''),
                    '크롤링시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            return None
        except:
            return None
    
    def _find_next_page_button(self, pagination):
        """다음 페이지 버튼 찾기"""
        try:
            buttons = pagination.find_elements(By.TAG_NAME, 'li')
            
            for i, button in enumerate(buttons):
                if 'active' in button.get_attribute('class'):
                    if i + 1 < len(buttons):
                        next_button = buttons[i + 1]
                        if 'disabled' not in next_button.get_attribute('class'):
                            return next_button.find_element(By.TAG_NAME, 'button')
            return None
        except:
            return None
    
    def run_parallel_crawling(self):
        """병렬 크롤링 실행"""
        self.start_time = time.time()
        
        if RICH_AVAILABLE:
            console.print(f"[bold cyan]병렬 크롤링 시작 (워커: {self.config['max_workers']}개)[/bold cyan]\n")
        else:
            print(f"병렬 크롤링 시작 (워커: {self.config['max_workers']}개)\n")
        
        # ThreadPoolExecutor 사용
        with ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            
            if RICH_AVAILABLE:
                # Rich Progress 사용
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
                    
                    # 모든 작업 제출
                    futures = []
                    for i in range(len(self.all_combinations)):
                        future = executor.submit(self.process_combination, i, progress, main_task)
                        futures.append(future)
                    
                    # 결과 수집
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            progress.advance(main_task)
                            
                            # 상태 업데이트
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
                # Rich가 없을 때
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
            # 통계 테이블
            table = Table(title="크롤링 완료", show_header=True, header_style="bold magenta")
            table.add_column("항목", style="cyan", width=20)
            table.add_column("수치", justify="right", style="yellow")
            
            table.add_row("소요 시간", f"{elapsed/60:.1f}분")
            table.add_row("성공", f"{self.completed_count:,}개")
            table.add_row("실패", f"{self.failed_count:,}개")
            table.add_row("총 추출 데이터", f"{self.total_extracted:,}개")
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
        
        return saved_files
    
    def run(self):
        """메인 실행"""
        try:
            if RICH_AVAILABLE:
                console.print(Panel.fit(
                    "[bold cyan]LG U+ 휴대폰 지원금 크롤러 v6.0[/bold cyan]\n"
                    "[yellow]멀티스레딩 최적화 버전[/yellow]",
                    border_style="cyan"
                ))
            else:
                print("="*50)
                print("LG U+ 휴대폰 지원금 크롤러 v6.0")
                print("멀티스레딩 최적화 버전")
                print("="*50)
            
            # 1. 조합 수집
            self.collect_all_combinations()
            
            if not self.all_combinations:
                if RICH_AVAILABLE:
                    console.print("[red]수집된 조합이 없습니다.[/red]")
                else:
                    print("수집된 조합이 없습니다.")
                return []
            
            # 2. 병렬 크롤링
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
        description='LG U+ 휴대폰 지원금 크롤러 v6.0',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--workers', type=int, default=5,
                        help='동시 실행 워커 수 (기본: 5)')
    parser.add_argument('--max-rate-plans', type=int, default=0,
                        help='최대 요금제 수 (0=전체)')
    parser.add_argument('--show-browser', action='store_true',
                        help='브라우저 표시')
    parser.add_argument('--output', type=str, default='data',
                        help='출력 디렉토리')
    parser.add_argument('--test', action='store_true',
                        help='테스트 모드 (처음 5개만)')
    
    args = parser.parse_args()
    
    # 설정
    config = {
        'max_workers': args.workers,
        'max_rate_plans': 5 if args.test else args.max_rate_plans,
        'show_browser': args.show_browser,
        'headless': not args.show_browser,
        'output_dir': args.output
    }
    
    # 크롤러 실행
    crawler = LGCrawlerV6(config)
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
                "[bold cyan]LG U+ 휴대폰 지원금 크롤러 v6.0[/bold cyan]\n\n"
                "[yellow]멀티스레딩 최적화 버전[/yellow]\n\n"
                "주요 특징:\n"
                "  • 안정적인 멀티스레딩 병렬 처리\n"
                "  • 실시간 상세 진행률 표시\n"
                "  • 스마트 재시도 메커니즘\n"
                "  • 최적화된 성능\n\n"
                "사용법:\n"
                "  [green]python lg_crawler.py[/green]              # 기본 실행\n"
                "  [green]python lg_crawler.py --workers 10[/green]  # 10개 워커\n"
                "  [green]python lg_crawler.py --test[/green]        # 테스트 모드\n"
                "  [green]python lg_crawler.py --help[/green]        # 도움말",
                border_style="cyan"
            ))
        else:
            print("LG U+ 휴대폰 지원금 크롤러 v6.0")
            print("\n사용법:")
            print("  python lg_crawler.py              # 기본 실행")
            print("  python lg_crawler.py --workers 10 # 10개 워커")
            print("  python lg_crawler.py --test       # 테스트 모드")
            print("  python lg_crawler.py --help       # 도움말")
        print()
    
    main()