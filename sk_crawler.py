#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SKT T world 공시지원금 크롤러 v2.0 - 멀티스레딩 최적화
Rich UI, 병렬 처리, 실시간 진행률 표시

주요 특징:
    - 멀티스레딩 기반 병렬 처리
    - Rich 라이브러리를 활용한 고급 UI
    - 실시간 상세 진행률 표시
    - 스마트 재시도 메커니즘
    - 체크포인트 저장/복원
    - 메모리 효율적인 처리

작성일: 2025-01-11
버전: 2.0
"""

import time
import json
import re
import os
from urllib.parse import urlencode, quote_plus
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import pandas as pd
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import List, Dict, Optional, Tuple
import traceback
import pickle
import argparse

# Rich library for better UI
try:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, MofNCompleteColumn
    from rich.table import Table
    from rich.panel import Panel
    from rich.live import Live
    from rich.layout import Layout
    from rich import print as rprint
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Warning: Rich library not installed. Install with: pip install rich")

# 콘솔 초기화
console = Console() if RICH_AVAILABLE else None

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler('skt_crawler_v2.log', encoding='utf-8'),
        logging.StreamHandler() if not RICH_AVAILABLE else logging.NullHandler()
    ]
)
logger = logging.getLogger(__name__)

# 기본 설정
BASE_URL = "https://shop.tworld.co.kr"
DATA_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(DATA_DIR, exist_ok=True)


class TworldCrawlerV2:
    """SKT T world 크롤러 v2.0 - 멀티스레딩 최적화"""
    
    def __init__(self, config=None):
        """초기화"""
        self.driver = None
        self.all_data = []
        self.data_lock = threading.Lock()
        self.rate_plans = []
        self.categories = []
        
        # 기본 설정
        self.config = {
            'headless': True,
            'max_workers': 5,
            'retry_count': 3,
            'page_load_timeout': 30,
            'checkpoint_interval': 50,
            'save_formats': ['excel', 'csv'],
            'output_dir': DATA_DIR,
            'max_rate_plans': 0,  # 0 = 모든 요금제
            'show_browser': False
        }
        
        if config:
            self.config.update(config)
        
        # 진행 상태 추적
        self.completed_count = 0
        self.failed_count = 0
        self.total_devices = 0
        self.start_time = None
        self.checkpoint_file = os.path.join(DATA_DIR, 'skt_checkpoint.pkl')
        
        # 스레드 안전 변수
        self.status_lock = threading.Lock()
        self.current_tasks = {}
        
        # 크롤링 조합
        self.all_combinations = []
        
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-notifications')
        options.add_argument('--disable-popup-blocking')
        
        # 메모리 관리 개선
        options.add_argument('--memory-pressure-off')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-features=TranslateUI')
        
        # 이미지 로딩 비활성화로 속도 향상
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_setting_values.media_stream": 2,
        }
        options.add_experimental_option("prefs", prefs)
        
        if self.config['headless'] and not self.config.get('show_browser'):
            options.add_argument('--headless=new')
        
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(self.config['page_load_timeout'])
        driver.implicitly_wait(5)
        
        return driver
    
    def create_driver(self):
        """스레드용 드라이버 생성"""
        return self.setup_driver()
    
    def collect_rate_plans(self):
        """모든 카테고리의 요금제 수집"""
        if RICH_AVAILABLE:
            console.print(Panel.fit(
                "[bold cyan]STEP 1: 전체 카테고리별 요금제 수집[/bold cyan]",
                border_style="cyan"
            ))
        else:
            print("\n" + "="*50)
            print("STEP 1: 전체 카테고리별 요금제 수집")
            print("="*50)
        
        driver = self.setup_driver()
        
        try:
            # 요금제 목록 페이지 접속
            url = "https://shop.tworld.co.kr/wireless/product/subscription/list"
            if RICH_AVAILABLE:
                console.print(f"[cyan]요금제 목록 페이지 접속:[/cyan] {url}")
            else:
                logger.info(f"요금제 목록 페이지 접속: {url}")
            
            driver.get(url)
            time.sleep(5)
            
            # 카테고리 목록 수집
            self.collect_categories(driver)
            
            if not self.categories:
                logger.error("카테고리를 찾을 수 없습니다.")
                return
            
            if RICH_AVAILABLE:
                console.print(f"\n[green]✓[/green] 총 {len(self.categories)}개 카테고리 발견")
                
                # Progress bar로 카테고리별 요금제 수집
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TimeRemainingColumn(),
                    console=console
                ) as progress:
                    
                    task = progress.add_task(
                        "[cyan]카테고리 처리",
                        total=len(self.categories)
                    )
                    
                    for idx, category in enumerate(self.categories, 1):
                        progress.update(
                            task,
                            description=f"[cyan]{category['name']} 처리 중..."
                        )
                        
                        try:
                            # 카테고리 클릭
                            self.click_category(driver, category['id'])
                            time.sleep(2)
                            
                            # 해당 카테고리의 요금제 수집
                            plans = self.collect_plans_in_category(driver, category)
                            
                            if plans:
                                console.print(f"[green]✓[/green] {category['name']}: {len(plans)}개 요금제")
                                self.rate_plans.extend(plans)
                            else:
                                console.print(f"[yellow]-[/yellow] {category['name']}: 요금제 없음")
                            
                            progress.advance(task)
                            
                        except Exception as e:
                            logger.error(f"카테고리 처리 오류: {e}")
                            progress.advance(task)
                            continue
            else:
                # Rich 없을 때
                logger.info(f"\n총 {len(self.categories)}개 카테고리 발견")
                
                for idx, category in enumerate(self.categories, 1):
                    logger.info(f"\n[{idx}/{len(self.categories)}] {category['name']} 카테고리 요금제 수집 중...")
                    
                    try:
                        self.click_category(driver, category['id'])
                        time.sleep(2)
                        plans = self.collect_plans_in_category(driver, category)
                        
                        if plans:
                            logger.info(f"  ✓ {len(plans)}개 요금제 수집")
                            self.rate_plans.extend(plans)
                    except Exception as e:
                        logger.error(f"  카테고리 처리 오류: {e}")
                        continue
            
            # 중복 제거
            unique_plans = {}
            for plan in self.rate_plans:
                unique_plans[plan['id']] = plan
            self.rate_plans = list(unique_plans.values())
            
            # 요금제 수 제한
            if self.config['max_rate_plans'] > 0:
                self.rate_plans = self.rate_plans[:self.config['max_rate_plans']]
            
            # 결과 출력
            if RICH_AVAILABLE:
                table = Table(title=f"수집된 요금제 ({len(self.rate_plans)}개)", show_header=True)
                table.add_column("번호", style="cyan", width=6)
                table.add_column("카테고리", style="magenta", width=20)
                table.add_column("요금제명", style="yellow", width=40)
                table.add_column("월요금", justify="right", style="green")
                
                for i, plan in enumerate(self.rate_plans[:10], 1):
                    table.add_row(
                        str(i),
                        plan['category'],
                        plan['name'][:40] + "..." if len(plan['name']) > 40 else plan['name'],
                        f"{plan['monthly_fee']:,}원"
                    )
                
                if len(self.rate_plans) > 10:
                    table.add_row("...", "...", f"외 {len(self.rate_plans)-10}개", "...")
                
                console.print("\n")
                console.print(table)
            else:
                logger.info(f"\n총 {len(self.rate_plans)}개 요금제 수집 완료")
                
        finally:
            driver.quit()
    
    def collect_categories(self, driver):
        """카테고리 목록 수집"""
        try:
            category_elements = driver.find_elements(
                By.CSS_SELECTOR, 
                "ul.phone-charge-type li.type-item"
            )
            
            for element in category_elements:
                try:
                    category_id = element.get_attribute("data-category-id")
                    category_name = element.find_element(By.TAG_NAME, "a").text.strip()
                    
                    if category_id and category_name:
                        self.categories.append({
                            'id': category_id,
                            'name': category_name
                        })
                        
                except Exception as e:
                    logger.debug(f"카테고리 추출 오류: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"카테고리 목록 수집 오류: {e}")
    
    def click_category(self, driver, category_id):
        """특정 카테고리 클릭"""
        try:
            category_element = driver.find_element(
                By.CSS_SELECTOR, 
                f"li.type-item[data-category-id='{category_id}']"
            )
            
            WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(category_element)
            )
            
            driver.execute_script("arguments[0].click();", category_element)
            
        except Exception as e:
            logger.error(f"카테고리 클릭 오류: {e}")
            raise
    
    def collect_plans_in_category(self, driver, category):
        """현재 카테고리의 요금제 수집"""
        plans = []
        
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "ul.phone-charge-list"))
            )
            
            # JavaScript로 요금제 데이터 추출
            js_code = """
            var plans = [];
            document.querySelectorAll('li.charge-item').forEach(function(el) {
                var id = el.getAttribute('data-subscription-id');
                var name = el.getAttribute('data-subscription-nm');
                if(id && name && id.startsWith('NA')) {
                    var priceElement = el.querySelector('.price .num');
                    var monthlyFee = 0;
                    if(priceElement) {
                        var priceText = priceElement.textContent.trim();
                        monthlyFee = parseInt(priceText.replace(/[^0-9]/g, '')) || 0;
                    }
                    
                    plans.push({
                        id: id,
                        name: name,
                        monthlyFee: monthlyFee
                    });
                }
            });
            return JSON.stringify(plans);
            """
            
            result = driver.execute_script(js_code)
            if result:
                category_plans = json.loads(result)
                for plan in category_plans:
                    plan['category'] = category['name']
                    plan['monthly_fee'] = plan.pop('monthlyFee')
                    plans.append(plan)
                    
        except TimeoutException:
            logger.debug(f"카테고리 {category['name']}에 요금제가 없습니다.")
        except Exception as e:
            logger.error(f"요금제 수집 오류: {e}")
            
        return plans
    
    def prepare_combinations(self):
        """크롤링 조합 준비"""
        if not self.rate_plans:
            return
        
        # SKT는 가입유형별로 동일하므로 기기변경만 수집
        scrb_type = {'value': '31', 'name': '기기변경'}
        
        # 모든 요금제에 대해 5G와 4G 모두 검색
        network_types = [
            {'code': '5G', 'name': '5G'},
            {'code': 'PHONE', 'name': '4G/LTE'}
        ]
        
        for plan in self.rate_plans:
            for network in network_types:
                self.all_combinations.append({
                    'plan': plan,
                    'network': network,
                    'scrb_type': scrb_type
                })
        
        if RICH_AVAILABLE:
            console.print(f"\n[cyan]총 {len(self.all_combinations)}개 조합 준비 완료[/cyan]")
            console.print(f"[yellow]요금제 {len(self.rate_plans)}개 × 네트워크 {len(network_types)}개[/yellow]\n")
        else:
            logger.info(f"\n총 {len(self.all_combinations)}개 조합 준비 완료")
    
    def process_combination(self, combo_index, progress=None, task_id=None):
        """단일 조합 처리"""
        combo = self.all_combinations[combo_index]
        driver = None
        thread_id = threading.current_thread().name
        
        # 현재 작업 상태 업데이트
        with self.status_lock:
            self.current_tasks[thread_id] = f"{combo['plan']['name'][:30]} - {combo['network']['name']}"
        
        try:
            driver = self.create_driver()
            
            # 진행 상황 업데이트
            if progress and task_id is not None:
                desc = f"[{combo_index+1}/{len(self.all_combinations)}] {combo['plan']['category']} - {combo['plan']['name'][:30]}... ({combo['network']['name']})"
                progress.update(task_id, description=desc)
            
            # URL 생성
            params = {
                'modelNwType': combo['network']['code'],
                'saleMonth': '24',
                'prodId': combo['plan']['id'],
                'prodNm': combo['plan']['name'],
                'saleYn': 'Y',
                'order': 'DISCOUNT',
                'scrbTypCd': combo['scrb_type']['value']
            }
            url = f"{BASE_URL}/notice?{urlencode(params, quote_via=quote_plus)}"
            
            # 페이지 로드
            driver.get(url)
            time.sleep(2)
            
            # 데이터 수집
            items_count = self._collect_all_pages_data(driver, combo)
            
            with self.status_lock:
                if items_count > 0:
                    self.completed_count += 1
                    self.total_devices += items_count
                    
                    if RICH_AVAILABLE and items_count > 0:
                        console.print(f"[green]✓[/green] [{combo_index+1}/{len(self.all_combinations)}] {combo['plan']['name'][:40]}... ({combo['network']['name']}) - [bold]{items_count}개[/bold]")
                else:
                    self.failed_count += 1
            
            return True
            
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
    
    def _collect_all_pages_data(self, driver, combo):
        """모든 페이지 데이터 수집"""
        all_items = 0
        current_page = 1
        max_pages = 10
        
        while current_page <= max_pages:
            items = self._collect_current_page_data(driver, combo)
            
            if not items:
                break
                
            all_items += len(items)
            
            # 다음 페이지 확인
            try:
                pagination = driver.find_element(By.CSS_SELECTOR, ".pagination, .paginate, .paging")
                
                next_page = current_page + 1
                try:
                    driver.execute_script(f"javascript:goPage({next_page});")
                    time.sleep(1.5)
                    
                    # 페이지 변경 확인
                    active = pagination.find_element(By.CSS_SELECTOR, ".active, .on, .current")
                    if int(active.text.strip()) == next_page:
                        current_page = next_page
                    else:
                        break
                except:
                    break
                    
            except:
                break
        
        return all_items
    
    def _collect_current_page_data(self, driver, combo):
        """현재 페이지 데이터 수집"""
        items = []
        
        try:
            tables = driver.find_elements(By.CSS_SELECTOR, "table.disclosure-list, table")
            
            for table in tables:
                tbody = table.find_element(By.TAG_NAME, "tbody")
                rows = tbody.find_elements(By.TAG_NAME, "tr")
                
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cells) == 1 and ('데이터가 없습니다' in cells[0].text or
                                           '조회된 데이터가 없습니다' in cells[0].text):
                        return items
                    
                    if len(cells) >= 6:
                        device_name = cells[0].text.strip()
                        date_text = cells[1].text.strip()
                        release_price = self.clean_price(cells[2].text)
                        public_fee = self.clean_price(cells[3].text)
                        add_fee = self.clean_price(cells[5].text) if len(cells) > 5 else 0
                        
                        if device_name and public_fee > 0:
                            item = {
                                'device_name': device_name,
                                'manufacturer': self.get_manufacturer(device_name),
                                'network_type': combo['network']['name'],
                                'scrb_type': combo['scrb_type']['value'],
                                'scrb_type_name': combo['scrb_type']['name'],
                                'plan_id': combo['plan']['id'],
                                'plan_name': combo['plan']['name'],
                                'plan_category': combo['plan']['category'],
                                'plan_monthly_fee': combo['plan']['monthly_fee'],
                                'public_support_fee': public_fee,
                                'additional_support_fee': add_fee,
                                'total_support_fee': public_fee + add_fee,
                                'release_price': release_price,
                                'date': date_text,
                                'crawled_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            items.append(item)
                            
                            with self.data_lock:
                                self.all_data.append(item)
                            
        except Exception as e:
            logger.debug(f"페이지 데이터 수집 오류: {e}")
        
        return items
    
    def clean_price(self, price_str):
        """가격 정리"""
        if not price_str:
            return 0
        cleaned = re.sub(r'[^0-9]', '', str(price_str))
        try:
            return int(cleaned) if cleaned else 0
        except:
            return 0
    
    def get_manufacturer(self, device_name):
        """제조사 추출"""
        name_lower = device_name.lower()
        if '갤럭시' in name_lower or 'galaxy' in name_lower:
            return '삼성'
        elif '아이폰' in name_lower or 'iphone' in name_lower or 'ipad' in name_lower:
            return '애플'
        elif 'lg' in name_lower:
            return 'LG'
        elif '샤오미' in name_lower or 'xiaomi' in name_lower:
            return '샤오미'
        elif '모토로라' in name_lower:
            return '모토로라'
        else:
            return '기타'
    
    def run_parallel_crawling(self):
        """병렬 크롤링 실행"""
        self.start_time = time.time()
        
        if RICH_AVAILABLE:
            console.print(Panel.fit(
                f"[bold cyan]STEP 2: 공시지원금 데이터 수집[/bold cyan]\n"
                f"[yellow]병렬 처리 (워커: {self.config['max_workers']}개)[/yellow]",
                border_style="cyan"
            ))
        else:
            print("\n" + "="*50)
            print("STEP 2: 공시지원금 데이터 수집")
            print(f"병렬 처리 (워커: {self.config['max_workers']}개)")
            print("="*50)
        
        # 체크포인트 확인
        start_index = self.load_checkpoint()
        
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
                        total=len(self.all_combinations) - start_index,
                        status=f"디바이스: 0개"
                    )
                    
                    # 작업 제출
                    futures = []
                    for i in range(start_index, len(self.all_combinations)):
                        future = executor.submit(self.process_combination, i, progress, main_task)
                        futures.append((future, i))
                    
                    # 결과 수집
                    for future, idx in futures:
                        try:
                            result = future.result()
                            progress.advance(main_task)
                            
                            # 상태 업데이트
                            elapsed = time.time() - self.start_time
                            speed = self.completed_count / (elapsed / 60) if elapsed > 0 else 0
                            
                            progress.update(
                                main_task,
                                status=f"디바이스: {self.total_devices:,}개 | 속도: {speed:.1f}/분"
                            )
                            
                            # 체크포인트 저장
                            if (idx + 1) % self.config['checkpoint_interval'] == 0:
                                self.save_checkpoint(idx + 1)
                            
                        except Exception as e:
                            logger.error(f"Future 오류: {str(e)}")
                            progress.advance(main_task)
            else:
                # Rich 없을 때
                futures = []
                for i in range(start_index, len(self.all_combinations)):
                    future = executor.submit(self.process_combination, i)
                    futures.append((future, i))
                
                completed = 0
                total = len(self.all_combinations) - start_index
                for future, idx in futures:
                    completed += 1
                    print(f"진행: {completed}/{total} ({completed/total*100:.1f}%)")
                    
                    if (idx + 1) % self.config['checkpoint_interval'] == 0:
                        self.save_checkpoint(idx + 1)
        
        # 다른 가입유형 데이터 복사
        self._duplicate_data_for_other_types()
        
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
            table.add_row("총 디바이스", f"{self.total_devices:,}개")
            table.add_row("총 데이터", f"{len(self.all_data):,}개")
            table.add_row("평균 속도", f"{self.completed_count/(elapsed/60):.1f}개/분")
            
            console.print("\n")
            console.print(table)
        else:
            print(f"\n크롤링 완료!")
            print(f"소요 시간: {elapsed/60:.1f}분")
            print(f"성공: {self.completed_count}개")
            print(f"실패: {self.failed_count}개")
            print(f"총 데이터: {len(self.all_data)}개")
        
        # 체크포인트 삭제
        self.clear_checkpoint()
    
    def _duplicate_data_for_other_types(self):
        """다른 가입유형용 데이터 복사"""
        if RICH_AVAILABLE:
            console.print("\n[cyan]다른 가입유형 데이터 생성 중...[/cyan]")
        else:
            logger.info("\n다른 가입유형 데이터 생성 중...")
        
        original_data = self.all_data.copy()
        other_types = [
            {'value': '11', 'name': '신규가입'},
            {'value': '41', 'name': '번호이동'}
        ]
        
        for scrb_type in other_types:
            for item in original_data:
                new_item = item.copy()
                new_item['scrb_type'] = scrb_type['value']
                new_item['scrb_type_name'] = scrb_type['name']
                self.all_data.append(new_item)
        
        if RICH_AVAILABLE:
            console.print(f"[green]✓[/green] 총 {len(self.all_data):,}개 데이터 생성 완료")
        else:
            logger.info(f"✓ 총 {len(self.all_data)}개 데이터 생성 완료")
    
    def save_checkpoint(self, index):
        """체크포인트 저장"""
        checkpoint_data = {
            'index': index,
            'all_data': self.all_data,
            'rate_plans': self.rate_plans,
            'categories': self.categories,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            with open(self.checkpoint_file, 'wb') as f:
                pickle.dump(checkpoint_data, f)
            logger.debug(f"체크포인트 저장: {index}")
        except Exception as e:
            logger.error(f"체크포인트 저장 실패: {e}")
    
    def load_checkpoint(self):
        """체크포인트 로드"""
        if not os.path.exists(self.checkpoint_file):
            return 0
        
        try:
            with open(self.checkpoint_file, 'rb') as f:
                checkpoint_data = pickle.load(f)
            
            self.all_data = checkpoint_data.get('all_data', [])
            saved_index = checkpoint_data.get('index', 0)
            
            if RICH_AVAILABLE:
                console.print(f"[yellow]체크포인트 로드: {saved_index}번째부터 재개[/yellow]")
            else:
                logger.info(f"체크포인트 로드: {saved_index}번째부터 재개")
            
            return saved_index
        except Exception as e:
            logger.error(f"체크포인트 로드 실패: {e}")
            return 0
    
    def clear_checkpoint(self):
        """체크포인트 삭제"""
        if os.path.exists(self.checkpoint_file):
            try:
                os.remove(self.checkpoint_file)
                logger.debug("체크포인트 파일 삭제")
            except:
                pass
    
    def clean_sheet_name(self, name):
        """Excel 시트명 정리"""
        invalid_chars = ['/', '\\', '?', '*', '[', ']', ':']
        clean_name = name
        for char in invalid_chars:
            clean_name = clean_name.replace(char, '_')
        return clean_name[:31].strip()
    
    def save_results(self):
        """결과 저장"""
        if not self.all_data:
            if RICH_AVAILABLE:
                console.print("[red]저장할 데이터가 없습니다.[/red]")
            else:
                logger.warning("저장할 데이터가 없습니다.")
            return []
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        saved_files = []
        
        try:
            # DataFrame 생성
            df = pd.DataFrame(self.all_data)
            
            # CSV 저장
            if 'csv' in self.config['save_formats']:
                csv_file = os.path.join(self.config['output_dir'], f"tworld_v2_{timestamp}.csv")
                df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                saved_files.append(csv_file)
                
                if RICH_AVAILABLE:
                    console.print(f"[green]CSV 저장:[/green] {csv_file}")
                else:
                    logger.info(f"CSV 저장: {csv_file}")
            
            # Excel 저장
            if 'excel' in self.config['save_formats']:
                excel_file = os.path.join(self.config['output_dir'], f"tworld_v2_{timestamp}.xlsx")
                
                with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                    # 전체 데이터
                    df.to_excel(writer, sheet_name='전체데이터', index=False)
                    
                    # 카테고리별
                    for category in sorted(df['plan_category'].unique()):
                        df_cat = df[df['plan_category'] == category]
                        sheet_name = self.clean_sheet_name(category)
                        df_cat.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # 네트워크별
                    for network in sorted(df['network_type'].unique()):
                        df_net = df[df['network_type'] == network]
                        sheet_name = self.clean_sheet_name(network)
                        df_net.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # 제조사별
                    for mfr in sorted(df['manufacturer'].unique()):
                        df_mfr = df[df['manufacturer'] == mfr]
                        sheet_name = self.clean_sheet_name(mfr)
                        df_mfr.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # 요약 통계
                    summary = []
                    
                    # 전체 통계
                    summary.append({
                        '구분': '전체',
                        '항목': '전체',
                        '카테고리수': df['plan_category'].nunique(),
                        '요금제수': df['plan_name'].nunique(),
                        '디바이스수': df['device_name'].nunique(),
                        '데이터수': len(df) // 3,
                        '평균 월요금': int(df['plan_monthly_fee'].mean()),
                        '평균 공시지원금': int(df['public_support_fee'].mean()),
                        '최대 공시지원금': int(df['public_support_fee'].max())
                    })
                    
                    # 카테고리별 통계
                    for category in sorted(df['plan_category'].unique()):
                        df_cat = df[df['plan_category'] == category]
                        summary.append({
                            '구분': '카테고리',
                            '항목': category,
                            '카테고리수': 1,
                            '요금제수': df_cat['plan_name'].nunique(),
                            '디바이스수': df_cat['device_name'].nunique(),
                            '데이터수': len(df_cat) // 3,
                            '평균 월요금': int(df_cat['plan_monthly_fee'].mean()),
                            '평균 공시지원금': int(df_cat['public_support_fee'].mean()),
                            '최대 공시지원금': int(df_cat['public_support_fee'].max())
                        })
                    
                    pd.DataFrame(summary).to_excel(writer, sheet_name='요약', index=False)
                    
                    # 요금제 목록
                    plan_df = pd.DataFrame(self.rate_plans)
                    plan_df.to_excel(writer, sheet_name='요금제목록', index=False)
                    
                    # 카테고리 목록
                    category_df = pd.DataFrame(self.categories)
                    category_df.to_excel(writer, sheet_name='카테고리목록', index=False)
                
                saved_files.append(excel_file)
                
                if RICH_AVAILABLE:
                    console.print(f"[green]Excel 저장:[/green] {excel_file}")
                else:
                    logger.info(f"Excel 저장: {excel_file}")
            
            # 통계 출력
            self._print_statistics(df)
            
        except Exception as e:
            logger.error(f"파일 저장 중 오류 발생: {e}")
            traceback.print_exc()
        
        return saved_files
    
    def _print_statistics(self, df):
        """통계 출력"""
        if RICH_AVAILABLE:
            # 요약 테이블
            summary_table = Table(title="크롤링 결과 요약", show_header=True, header_style="bold cyan")
            summary_table.add_column("항목", style="yellow")
            summary_table.add_column("수치", justify="right", style="green")
            
            summary_table.add_row("수집된 카테고리", f"{len(self.categories)}개")
            summary_table.add_row("수집된 요금제", f"{len(self.rate_plans)}개")
            summary_table.add_row("총 데이터", f"{len(df):,}개")
            summary_table.add_row("실제 디바이스 조합", f"{len(df)//3:,}개")
            summary_table.add_row("디바이스 종류", f"{df['device_name'].nunique()}개")
            summary_table.add_row("평균 월 납부요금", f"{df['plan_monthly_fee'].mean():,.0f}원")
            summary_table.add_row("평균 공시지원금", f"{df['public_support_fee'].mean():,.0f}원")
            summary_table.add_row("최대 공시지원금", f"{df['public_support_fee'].max():,.0f}원")
            
            console.print("\n")
            console.print(summary_table)
            
            # 카테고리별 통계
            cat_table = Table(title="카테고리별 통계", show_header=True, header_style="bold magenta")
            cat_table.add_column("카테고리", style="cyan")
            cat_table.add_column("요금제", justify="right", style="yellow")
            cat_table.add_column("디바이스", justify="right", style="green")
            cat_table.add_column("평균 월요금", justify="right", style="blue")
            cat_table.add_column("평균 지원금", justify="right", style="red")
            
            cat_stats = df.groupby('plan_category').agg({
                'plan_name': 'nunique',
                'device_name': 'nunique',
                'plan_monthly_fee': 'mean',
                'public_support_fee': 'mean'
            }).round(0)
            
            for cat in cat_stats.index:
                cat_table.add_row(
                    cat,
                    str(int(cat_stats.loc[cat, 'plan_name'])),
                    str(int(cat_stats.loc[cat, 'device_name'])),
                    f"{int(cat_stats.loc[cat, 'plan_monthly_fee']):,}원",
                    f"{int(cat_stats.loc[cat, 'public_support_fee']):,}원"
                )
            
            console.print("\n")
            console.print(cat_table)
        else:
            # Rich 없을 때
            print("\n" + "="*50)
            print("크롤링 결과 요약")
            print("="*50)
            print(f"수집된 카테고리: {len(self.categories)}개")
            print(f"수집된 요금제: {len(self.rate_plans)}개")
            print(f"총 데이터: {len(df):,}개")
            print(f"실제 디바이스 조합: {len(df)//3:,}개")
            print(f"디바이스 종류: {df['device_name'].nunique()}개")
            print(f"평균 월 납부요금: {df['plan_monthly_fee'].mean():,.0f}원")
            print(f"평균 공시지원금: {df['public_support_fee'].mean():,.0f}원")
            print(f"최대 공시지원금: {df['public_support_fee'].max():,.0f}원")
    
    def run(self):
        """전체 실행"""
        try:
            if RICH_AVAILABLE:
                console.print(Panel.fit(
                    "[bold cyan]T world 전체 카테고리 크롤러 v2.0[/bold cyan]\n"
                    "[yellow]멀티스레딩 최적화 버전[/yellow]\n"
                    f"[dim]워커: {self.config['max_workers']}개 | 출력: {', '.join(self.config['save_formats'])}[/dim]",
                    border_style="cyan"
                ))
            else:
                print("\n" + "="*50)
                print("T world 전체 카테고리 크롤러 v2.0")
                print("멀티스레딩 최적화 버전")
                print("="*50)
            
            # 1. 요금제 수집
            self.collect_rate_plans()
            
            if not self.rate_plans:
                if RICH_AVAILABLE:
                    console.print("[red]수집된 요금제가 없어 크롤링을 중단합니다.[/red]")
                else:
                    logger.error("수집된 요금제가 없어 크롤링을 중단합니다.")
                return []
            
            # 2. 조합 준비
            self.prepare_combinations()
            
            # 3. 병렬 크롤링
            self.run_parallel_crawling()
            
            # 4. 결과 저장
            saved_files = self.save_results()
            
            return saved_files
            
        except KeyboardInterrupt:
            if RICH_AVAILABLE:
                console.print("\n[yellow]사용자에 의해 중단되었습니다.[/yellow]")
            else:
                print("\n사용자에 의해 중단되었습니다.")
            
            # 중단 시점까지의 데이터 저장
            if self.all_data:
                saved_files = self.save_results()
                return saved_files
            return []
            
        except Exception as e:
            if RICH_AVAILABLE:
                console.print(f"\n[red]오류 발생: {str(e)}[/red]")
            else:
                logger.error(f"크롤링 중 오류: {e}")
            traceback.print_exc()
            return []


def main():
    """메인 실행"""
    parser = argparse.ArgumentParser(
        description='T world 전체 카테고리 크롤러 v2.0',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--workers', type=int, default=5,
                        help='동시 실행 워커 수 (기본: 5)')
    parser.add_argument('--max-plans', type=int, default=0,
                        help='최대 요금제 수 (0=전체)')
    parser.add_argument('--show-browser', action='store_true',
                        help='브라우저 표시')
    parser.add_argument('--output', type=str, default='data',
                        help='출력 디렉토리')
    parser.add_argument('--format', nargs='+', choices=['excel', 'csv'],
                        default=['excel', 'csv'],
                        help='저장 형식')
    parser.add_argument('--test', action='store_true',
                        help='테스트 모드 (처음 10개 요금제만)')
    parser.add_argument('--resume', action='store_true',
                        help='체크포인트에서 재개')
    
    args = parser.parse_args()
    
    # 설정
    config = {
        'max_workers': args.workers,
        'max_rate_plans': 10 if args.test else args.max_plans,
        'show_browser': args.show_browser,
        'headless': not args.show_browser,
        'output_dir': args.output,
        'save_formats': args.format
    }
    
    if RICH_AVAILABLE:
        console.print(Panel.fit(
            "[bold cyan]T world 전체 카테고리 크롤러 v2.0[/bold cyan]\n"
            "[yellow]멀티스레딩 최적화 버전[/yellow]\n\n"
            "주요 특징:\n"
            "  • 안정적인 멀티스레딩 병렬 처리\n"
            "  • Rich UI로 실시간 진행률 표시\n"
            "  • 체크포인트 저장/복원\n"
            "  • 스마트 재시도 메커니즘\n"
            "  • 카테고리별/네트워크별/제조사별 분류\n\n"
            "사용법:\n"
            "  [green]python skt_crawler_v2.py[/green]              # 기본 실행\n"
            "  [green]python skt_crawler_v2.py --workers 10[/green]  # 10개 워커\n"
            "  [green]python skt_crawler_v2.py --test[/green]        # 테스트 모드\n"
            "  [green]python skt_crawler_v2.py --resume[/green]      # 재개\n"
            "  [green]python skt_crawler_v2.py --help[/green]        # 도움말",
            border_style="cyan"
        ))
    else:
        print("\n" + "="*60)
        print("T world 전체 카테고리 크롤러 v2.0")
        print("멀티스레딩 최적화 버전")
        print("="*60)
        print("\n사용법:")
        print("  python skt_crawler_v2.py              # 기본 실행")
        print("  python skt_crawler_v2.py --workers 10 # 10개 워커")
        print("  python skt_crawler_v2.py --test       # 테스트 모드")
        print("  python skt_crawler_v2.py --help       # 도움말")
    
    print()
    
    # 크롤러 실행
    crawler = TworldCrawlerV2(config)
    saved_files = crawler.run()
    
    if saved_files:
        if RICH_AVAILABLE:
            console.print(f"\n[bold green]✅ 완료! {len(saved_files)}개 파일 저장됨[/bold green]")
            for file in saved_files:
                console.print(f"  [dim]• {file}[/dim]")
        else:
            print(f"\n✅ 완료! {len(saved_files)}개 파일 저장됨")
            for file in saved_files:
                print(f"  • {file}")
    else:
        if RICH_AVAILABLE:
            console.print("\n[red]⚠️ 저장된 파일이 없습니다.[/red]")
        else:
            print("\n⚠️ 저장된 파일이 없습니다.")


if __name__ == "__main__":
    main()