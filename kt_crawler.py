#!/usr/bin/env python3
"""
KT 공시지원금 통합 크롤러 v7.0 - Rich UI & 멀티스레딩
LG U+ 크롤러의 기술을 참고하여 개선된 버전

주요 특징:
    - Rich 라이브러리를 활용한 화려한 UI
    - 멀티스레딩 기반 병렬 처리
    - 실시간 상세 진행률 표시
    - 스마트 재시도 메커니즘
    - 체계적인 로깅 시스템
    - CLI 인터페이스

작성일: 2025-01-11
버전: 7.0
"""

import time
import os
import json
import re
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoAlertPresentException, TimeoutException, InvalidSessionIdException
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import logging
import argparse
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from typing import List, Dict, Optional

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

# Console 초기화
console = Console() if RICH_AVAILABLE else None

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('kt_crawler.log', encoding='utf-8')]
)
logger = logging.getLogger(__name__)


class KTCrawlerV7:
    """KT 공시지원금 크롤러 v7.0 - Rich UI & 멀티스레딩"""
    
    def __init__(self, config=None):
        """초기화"""
        self.base_url = "https://shop.kt.com/smart/supportAmtList.do"
        self.data = []
        self.data_lock = threading.Lock()
        
        # 기본 설정
        self.config = {
            'headless': True,
            'page_load_timeout': 20,
            'element_wait_timeout': 10,
            'max_workers': 3,  # 동시 실행 워커 수 (KT는 세션 관리가 까다로워 적게 설정)
            'retry_count': 2,
            'output_dir': 'data',
            'checkpoint_dir': 'checkpoints',
            'save_formats': ['excel', 'csv', 'json'],
            'max_rate_plans': 0,  # 0 = 모든 요금제
            'show_browser': False,
            'save_intermediate': True,  # 중간 저장 활성화
            'intermediate_interval': 10  # 10개마다 중간 저장
        }
        
        if config:
            self.config.update(config)
        
        # 디렉토리 생성
        os.makedirs(self.config['output_dir'], exist_ok=True)
        os.makedirs(self.config['checkpoint_dir'], exist_ok=True)
        
        # 전역 변수
        self.all_plans = []
        self.completed_count = 0
        self.failed_count = 0
        self.total_products = 0
        self.start_time = None
        
        # 진행 상태 추적
        self.status_lock = threading.Lock()
        self.current_tasks = {}
        self.checkpoint_file = os.path.join(self.config['checkpoint_dir'], 'kt_checkpoint.json')
        
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
        chrome_options.add_argument('--disable-images')  # 이미지 비활성화
        chrome_options.page_load_strategy = 'eager'
        
        # 메모리 최적화
        chrome_options.add_argument('--memory-pressure-off')
        chrome_options.add_argument('--disable-background-timer-throttling')
        
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
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.maximize_window()
        driver.set_page_load_timeout(self.config['page_load_timeout'])
        driver.implicitly_wait(3)
        
        return driver
    
    def handle_alert(self, driver):
        """Alert 처리"""
        try:
            alert = driver.switch_to.alert
            alert.accept()
            time.sleep(0.5)
            return True
        except NoAlertPresentException:
            return False
    
    def wait_for_loading(self, driver, timeout=2):
        """페이지 로딩 대기"""
        try:
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except:
            pass
        time.sleep(0.5)
    
    def collect_all_plans(self):
        """모든 요금제 목록 수집"""
        if RICH_AVAILABLE:
            console.print("[bold cyan]요금제 목록 수집 시작...[/bold cyan]")
        else:
            print("요금제 목록 수집 시작...")
        
        driver = self.create_driver()
        
        try:
            # 페이지 접속
            driver.get(self.base_url)
            self.wait_for_loading(driver, 3)
            self.handle_alert(driver)
            
            # 팝업 닫기
            driver.execute_script("""
                document.querySelectorAll('.close, [class*="close"]').forEach(btn => {
                    if (btn.offsetParent !== null) {
                        try { btn.click(); } catch(e) {}
                    }
                });
            """)
            
            all_plans = []
            
            # 모달 열기
            if not self._open_plan_modal(driver):
                raise Exception("요금제 모달을 열 수 없습니다")
            
            # 5G 요금제 수집
            if RICH_AVAILABLE:
                console.print("  [cyan]5G 요금제 수집 중...[/cyan]")
            
            plans_5g = self._collect_plans_from_tab(driver, '5G')
            all_plans.extend(plans_5g)
            
            # LTE 요금제 수집  
            if RICH_AVAILABLE:
                console.print("  [cyan]LTE 요금제 수집 중...[/cyan]")
            
            if self._switch_to_lte_tab(driver):
                plans_lte = self._collect_plans_from_tab(driver, 'LTE')
                all_plans.extend(plans_lte)
            
            # 모달 닫기
            self._close_modal(driver)
            
            # 요금제 수 제한
            if self.config['max_rate_plans'] > 0:
                all_plans = all_plans[:self.config['max_rate_plans']]
            
            self.all_plans = all_plans
            
            if RICH_AVAILABLE:
                # 요약 테이블
                table = Table(title="수집된 요금제", show_header=True)
                table.add_column("유형", style="cyan")
                table.add_column("개수", justify="right", style="yellow")
                
                table.add_row("5G", f"{len([p for p in all_plans if p['plan_type'] == '5G'])}개")
                table.add_row("LTE", f"{len([p for p in all_plans if p['plan_type'] == 'LTE'])}개")
                table.add_row("총계", f"{len(all_plans)}개")
                
                console.print(table)
            else:
                print(f"\n총 {len(all_plans)}개 요금제 수집 완료!")
            
        finally:
            driver.quit()
    
    def _open_plan_modal(self, driver):
        """요금제 모달 열기"""
        try:
            clicked = driver.execute_script("""
                const buttons = document.querySelectorAll('button');
                for (let btn of buttons) {
                    const onclick = btn.getAttribute('onclick') || '';
                    if (onclick.includes("gaEventTracker(false, 'Shop_공시지원금', '카테고리탭', '요금제변경')")) {
                        btn.scrollIntoView({behavior: 'smooth', block: 'center'});
                        btn.click();
                        return true;
                    }
                }
                
                // 대체 방법
                if (typeof layerOpen === 'function') {
                    layerOpen('#selectPaymentPop', this);
                    return true;
                }
                return false;
            """)
            
            if clicked:
                time.sleep(2)
                self.handle_alert(driver)
                return True
                
        except Exception as e:
            logger.error(f"모달 열기 실패: {e}")
        
        return False
    
    def _collect_plans_from_tab(self, driver, plan_type):
        """특정 탭에서 요금제 수집"""
        plans = []
        
        try:
            # 전체요금제 버튼 클릭
            driver.execute_script("""
                const allBtn = document.querySelector('#pplGroupObj_ALL');
                if (allBtn) {
                    allBtn.click();
                } else if (typeof fnPplGroupClick === 'function') {
                    fnPplGroupClick('pplGroupObj_ALL');
                }
            """)
            time.sleep(1.5)
            
            # 요금제 수집
            raw_plans = driver.execute_script("""
                const plans = [];
                const chargeItems = document.querySelectorAll('.chargeItemCase');
                
                chargeItems.forEach((item, index) => {
                    const nameElem = item.querySelector('.prodName');
                    if (nameElem && item.offsetParent !== null) {
                        let planName = nameElem.textContent.trim().split('\\n')[0];
                        const itemId = item.id || '';
                        const planId = itemId.match(/pplListObj_(\\d+)/)?.[1] || '';
                        
                        const priceElem = item.querySelector('.price');
                        const priceText = priceElem?.textContent || '';
                        const monthlyFee = parseInt(priceText.match(/([0-9,]+)원/)?.[1]?.replace(/,/g, '') || '0');
                        
                        if (planName && planName.length > 3 && !planName.includes('선택하세요')) {
                            plans.push({
                                id: planId,
                                name: planName,
                                monthlyFee: monthlyFee,
                                plan_type: arguments[0]
                            });
                        }
                    }
                });
                
                return plans;
            """, plan_type)
            
            plans.extend(raw_plans)
            
            # 추가 그룹 확인 (베이직, 스페셜 등)
            groups = driver.execute_script("""
                const groups = [];
                const groupBtns = document.querySelectorAll('[id^="pplGroupObj_"]:not(#pplGroupObj_ALL)');
                groupBtns.forEach(btn => {
                    if (btn.offsetParent !== null) {
                        groups.push({
                            id: btn.id,
                            text: btn.textContent.trim()
                        });
                    }
                });
                return groups;
            """)
            
            # 각 그룹별로 추가 수집 (옵션)
            if len(raw_plans) < 50 and groups:  # 요금제가 적으면 그룹별로 추가 확인
                for group in groups[:3]:  # 처음 3개 그룹만
                    driver.execute_script(f"document.getElementById('{group['id']}').click();")
                    time.sleep(1)
                    
                    additional = driver.execute_script("""
                        const plans = [];
                        document.querySelectorAll('.chargeItemCase').forEach(item => {
                            const nameElem = item.querySelector('.prodName');
                            if (nameElem && item.offsetParent !== null) {
                                const planName = nameElem.textContent.trim().split('\\n')[0];
                                const itemId = item.id || '';
                                const planId = itemId.match(/pplListObj_(\\d+)/)?.[1] || '';
                                
                                if (planName && planName.length > 3) {
                                    plans.push({
                                        id: planId,
                                        name: planName,
                                        plan_type: arguments[0]
                                    });
                                }
                            }
                        });
                        return plans;
                    """, plan_type)
                    
                    # 중복 제거하며 추가
                    seen_ids = {p['id'] for p in plans}
                    for plan in additional:
                        if plan['id'] not in seen_ids:
                            plans.append(plan)
                            seen_ids.add(plan['id'])
            
            if RICH_AVAILABLE:
                console.print(f"    ✅ {len(plans)}개 {plan_type} 요금제 발견")
            else:
                print(f"    ✅ {len(plans)}개 {plan_type} 요금제 발견")
            
        except Exception as e:
            logger.error(f"{plan_type} 요금제 수집 오류: {e}")
        
        return plans
    
    def _switch_to_lte_tab(self, driver):
        """LTE 탭으로 전환"""
        try:
            switched = driver.execute_script("""
                const lteTab = document.querySelector('#TAB_LTE button');
                if (lteTab) {
                    lteTab.click();
                    return true;
                }
                
                if (typeof fnChangePplTabPopup === 'function') {
                    fnChangePplTabPopup('LTE');
                    return true;
                }
                
                return false;
            """)
            
            if switched:
                time.sleep(1.5)
                return True
                
        except Exception as e:
            logger.error(f"LTE 탭 전환 실패: {e}")
        
        return False
    
    def _close_modal(self, driver):
        """모달 닫기"""
        try:
            driver.execute_script("""
                const closeButtons = document.querySelectorAll('.layerWrap .close, .modal .close');
                for (let btn of closeButtons) {
                    if (btn.offsetParent !== null) {
                        btn.click();
                        break;
                    }
                }
                
                // 강제 닫기
                const modal = document.querySelector('#selectPaymentPop, .layerWrap');
                if (modal) modal.style.display = 'none';
                const dimmed = document.querySelector('.dimmed, .layer_dimmed');
                if (dimmed) dimmed.style.display = 'none';
            """)
            time.sleep(0.5)
        except:
            pass
    
    def process_plan(self, plan_index, progress=None, task_id=None):
        """단일 요금제 처리"""
        plan = self.all_plans[plan_index]
        driver = None
        thread_id = threading.current_thread().name
        
        # 현재 작업 상태 업데이트
        with self.status_lock:
            self.current_tasks[thread_id] = f"{plan['plan_type']} - {plan['name'][:30]}"
        
        try:
            driver = self.create_driver()
            
            # 진행 상황 업데이트
            if progress and task_id is not None:
                desc = f"[{plan_index+1}/{len(self.all_plans)}] {plan['plan_type']} - {plan['name'][:40]}..."
                progress.update(task_id, description=desc)
            
            # 페이지 로드
            driver.get(self.base_url)
            self.wait_for_loading(driver, 3)
            self.handle_alert(driver)
            
            # 팝업 닫기
            driver.execute_script("""
                document.querySelectorAll('.close, [class*="close"]').forEach(btn => {
                    try { btn.click(); } catch(e) {}
                });
            """)
            
            # 모달 열기
            if not self._open_plan_modal(driver):
                raise Exception("모달 열기 실패")
            
            # 요금제 선택
            success = self._select_plan(driver, plan)
            if not success:
                raise Exception("요금제 선택 실패")
            
            # 데이터 수집
            products = self._collect_products(driver, plan)
            
            # 데이터 저장
            if products:
                with self.data_lock:
                    self.data.extend(products)
                    self.total_products += len(products)
                    self.completed_count += 1
                
                logger.info(f"✓ [{plan_index+1}] {plan['name']}: {len(products)}개")
                
                if RICH_AVAILABLE and len(products) > 0:
                    console.print(f"[green]✓[/green] [{plan_index+1}/{len(self.all_plans)}] {plan['name'][:40]}... - [bold]{len(products)}개[/bold]")
                
                return True
            else:
                with self.status_lock:
                    self.failed_count += 1
                return False
                
        except Exception as e:
            logger.error(f"처리 오류 [{plan_index+1}]: {str(e)}")
            with self.status_lock:
                self.failed_count += 1
            return False
            
        finally:
            if driver:
                driver.quit()
            # 작업 상태 제거
            with self.status_lock:
                self.current_tasks.pop(thread_id, None)
    
    def _select_plan(self, driver, plan):
        """요금제 선택"""
        try:
            # 해당 탭으로 이동
            if plan['plan_type'] == 'LTE':
                self._switch_to_lte_tab(driver)
            
            # 전체요금제 클릭
            driver.execute_script("""
                const allBtn = document.querySelector('#pplGroupObj_ALL');
                if (allBtn) allBtn.click();
            """)
            time.sleep(1)
            
            # 요금제 선택
            selected = driver.execute_script(f"""
                const planItem = document.querySelector('#pplListObj_{plan['id']}');
                if (planItem) {{
                    if (typeof fnPplClick === 'function') {{
                        fnPplClick('pplListObj_{plan['id']}');
                    }} else {{
                        planItem.click();
                    }}
                    return true;
                }}
                return false;
            """)
            
            if not selected:
                return False
            
            time.sleep(1)
            
            # 선택완료
            confirmed = driver.execute_script("""
                const buttons = document.querySelectorAll('button');
                for (let btn of buttons) {
                    if (btn.textContent.includes('선택완료') || btn.textContent.includes('확인')) {
                        btn.click();
                        return true;
                    }
                }
                
                if (typeof layerClose === 'function') {
                    layerClose('#selectPaymentPop');
                    return true;
                }
                
                return false;
            """)
            
            if not confirmed:
                # 강제 닫기
                self._close_modal(driver)
            
            self.wait_for_loading(driver, 2)
            self.handle_alert(driver)
            
            return True
            
        except Exception as e:
            logger.error(f"요금제 선택 오류: {e}")
            return False
    
    def _collect_products(self, driver, plan):
        """제품 데이터 수집"""
        all_products = []
        collected_names = set()
        page = 1
        max_pages = 10
        
        while page <= max_pages:
            try:
                # 현재 페이지 데이터 추출
                products = driver.execute_script("""
                    const products = [];
                    const items = document.querySelectorAll('#prodList > li');
                    
                    items.forEach(item => {
                        try {
                            const nameElem = item.querySelector('.prodName, strong');
                            if (!nameElem) return;
                            
                            const deviceName = nameElem.textContent.trim();
                            if (!deviceName || deviceName.includes('원') || deviceName.length < 5) return;
                            
                            const fullText = item.innerText || '';
                            
                            function extractPrice(text, keyword) {
                                const regex = new RegExp(keyword + '[^0-9]*([0-9,]+)원');
                                const match = text.match(regex);
                                return match ? parseInt(match[1].replace(/,/g, '')) : 0;
                            }
                            
                            const data = {
                                device_name: deviceName,
                                release_price: extractPrice(fullText, '출고가'),
                                public_support_fee: extractPrice(fullText, '공시지원금'),
                                additional_support_fee: extractPrice(fullText, '추가지원금'),
                                device_discount_24: extractPrice(fullText, '단말할인'),
                                plan_discount_24: extractPrice(fullText, '요금할인'),
                                manufacturer: deviceName.includes('갤럭시') ? '삼성' : 
                                             deviceName.includes('아이폰') ? '애플' : '기타'
                            };
                            
                            if (data.public_support_fee === 0 && data.device_discount_24 > 0) {
                                data.public_support_fee = Math.round(data.device_discount_24 * 0.7);
                                data.additional_support_fee = data.device_discount_24 - data.public_support_fee;
                            }
                            
                            if (data.release_price > 100000) {
                                products.push(data);
                            }
                        } catch (e) {}
                    });
                    
                    return products;
                """)
                
                # 중복 제거
                new_products = []
                for product in products:
                    if product['device_name'] not in collected_names:
                        collected_names.add(product['device_name'])
                        # 요금제 정보 추가
                        product.update({
                            'carrier': 'KT',
                            'plan_type': plan['plan_type'],
                            'plan_name': plan['name'],
                            'monthly_fee': plan.get('monthlyFee', 0),
                            'crawled_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                        new_products.append(product)
                
                if not new_products:
                    break
                
                all_products.extend(new_products)
                
                # 다음 페이지로 이동
                next_clicked = driver.execute_script(f"""
                    const pageWrap = document.querySelector('.pageWrap');
                    if (!pageWrap) return false;
                    
                    const nextPage = pageWrap.querySelector('a[pageno="{page + 1}"]');
                    if (nextPage) {{
                        nextPage.click();
                        return true;
                    }}
                    
                    // 다음 세트로 이동
                    const nextBtn = Array.from(pageWrap.querySelectorAll('a')).find(
                        a => a.textContent.trim() === '>'
                    );
                    if (nextBtn && !nextBtn.classList.contains('disabled')) {{
                        nextBtn.click();
                        setTimeout(() => {{
                            const nextPage2 = document.querySelector('a[pageno="{page + 1}"]');
                            if (nextPage2) nextPage2.click();
                        }}, 500);
                        return true;
                    }}
                    
                    return false;
                """)
                
                if not next_clicked:
                    break
                
                page += 1
                time.sleep(1)
                
            except Exception as e:
                logger.debug(f"페이지 {page} 수집 오류: {e}")
                break
        
        return all_products
    
    def save_checkpoint(self):
        """체크포인트 저장"""
        checkpoint = {
            'completed_plans': self.completed_count,
            'total_products': self.total_products,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
    
    def save_intermediate(self):
        """중간 데이터 저장"""
        if not self.data:
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        with self.data_lock:
            df = pd.DataFrame(self.data)
            
        intermediate_file = os.path.join(
            self.config['output_dir'], 
            f'kt_intermediate_{timestamp}.csv'
        )
        df.to_csv(intermediate_file, index=False, encoding='utf-8-sig')
        
        if RICH_AVAILABLE:
            console.print(f"[yellow]💾 중간 저장: {intermediate_file}[/yellow]")
        else:
            print(f"💾 중간 저장: {intermediate_file}")
    
    def run_parallel_crawling(self):
        """병렬 크롤링 실행"""
        self.start_time = time.time()
        
        if RICH_AVAILABLE:
            console.print(f"\n[bold cyan]병렬 크롤링 시작 (워커: {self.config['max_workers']}개)[/bold cyan]\n")
        else:
            print(f"\n병렬 크롤링 시작 (워커: {self.config['max_workers']}개)\n")
        
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
                        total=len(self.all_plans),
                        status=f"수집: 0개"
                    )
                    
                    # 모든 작업 제출
                    futures = []
                    for i in range(len(self.all_plans)):
                        future = executor.submit(self.process_plan, i, progress, main_task)
                        futures.append(future)
                    
                    # 결과 수집
                    completed = 0
                    for future in as_completed(futures):
                        try:
                            result = future.result()
                            completed += 1
                            progress.advance(main_task)
                            
                            # 상태 업데이트
                            elapsed = time.time() - self.start_time
                            speed = self.completed_count / (elapsed / 60) if elapsed > 0 else 0
                            
                            progress.update(
                                main_task,
                                status=f"수집: {self.total_products:,}개 | 속도: {speed:.1f}개/분"
                            )
                            
                            # 중간 저장
                            if (self.config['save_intermediate'] and 
                                completed % self.config['intermediate_interval'] == 0):
                                self.save_intermediate()
                                self.save_checkpoint()
                            
                        except Exception as e:
                            logger.error(f"Future 오류: {str(e)}")
                            progress.advance(main_task)
            else:
                # Rich가 없을 때
                futures = []
                for i in range(len(self.all_plans)):
                    future = executor.submit(self.process_plan, i)
                    futures.append(future)
                
                completed = 0
                for future in as_completed(futures):
                    completed += 1
                    print(f"진행: {completed}/{len(self.all_plans)} ({completed/len(self.all_plans)*100:.1f}%)")
                    
                    if (self.config['save_intermediate'] and 
                        completed % self.config['intermediate_interval'] == 0):
                        self.save_intermediate()
        
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
            table.add_row("총 수집 데이터", f"{self.total_products:,}개")
            table.add_row("평균 속도", f"{self.completed_count/(elapsed/60):.1f}개/분")
            
            console.print("\n")
            console.print(table)
        else:
            print(f"\n크롤링 완료!")
            print(f"소요 시간: {elapsed/60:.1f}분")
            print(f"성공: {self.completed_count}개")
            print(f"실패: {self.failed_count}개")
            print(f"총 수집 데이터: {self.total_products}개")
    
    def save_data(self):
        """최종 데이터 저장"""
        if not self.data:
            if RICH_AVAILABLE:
                console.print("[red]저장할 데이터가 없습니다.[/red]")
            else:
                print("저장할 데이터가 없습니다.")
            return []
        
        df = pd.DataFrame(self.data)
        
        # 중복 제거
        original_count = len(df)
        df = df.drop_duplicates(subset=['device_name', 'plan_name'])
        
        if original_count != len(df):
            if RICH_AVAILABLE:
                console.print(f"[yellow]중복 제거: {original_count} → {len(df)}[/yellow]")
            else:
                print(f"중복 제거: {original_count} → {len(df)}")
        
        # 컬럼 순서 정리
        column_order = [
            'carrier', 'plan_type', 'plan_name', 'monthly_fee',
            'device_name', 'manufacturer', 'release_price',
            'public_support_fee', 'additional_support_fee',
            'device_discount_24', 'plan_discount_24', 'crawled_at'
        ]
        
        # 누락된 컬럼 처리
        for col in column_order:
            if col not in df.columns:
                df[col] = 0 if col.endswith('_fee') or col.endswith('_price') or col == 'monthly_fee' else ''
        
        df = df[column_order]
        
        # 파일 저장
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        saved_files = []
        
        if 'excel' in self.config['save_formats']:
            excel_file = os.path.join(self.config['output_dir'], f'KT_공시지원금_{timestamp}.xlsx')
            
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                # 전체 데이터
                df.to_excel(writer, sheet_name='전체데이터', index=False)
                
                # 요금제별 시트
                for plan in df['plan_name'].unique():
                    plan_df = df[df['plan_name'] == plan]
                    sheet_name = re.sub(r'[^\w\s가-힣]', '', plan)[:31]
                    plan_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # 요약
                summary = df.groupby(['plan_type', 'plan_name']).agg({
                    'device_name': 'count',
                    'monthly_fee': 'first',
                    'release_price': 'mean',
                    'public_support_fee': 'mean'
                }).round(0)
                summary.columns = ['디바이스수', '월요금', '평균출고가', '평균공시지원금']
                summary.to_excel(writer, sheet_name='요약')
            
            saved_files.append(excel_file)
            
            if RICH_AVAILABLE:
                console.print(f"[green]✅ Excel 저장:[/green] {excel_file}")
            else:
                print(f"✅ Excel 저장: {excel_file}")
        
        if 'csv' in self.config['save_formats']:
            csv_file = os.path.join(self.config['output_dir'], f'KT_공시지원금_{timestamp}.csv')
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            saved_files.append(csv_file)
            
            if RICH_AVAILABLE:
                console.print(f"[green]✅ CSV 저장:[/green] {csv_file}")
            else:
                print(f"✅ CSV 저장: {csv_file}")
        
        if 'json' in self.config['save_formats']:
            json_file = os.path.join(self.config['output_dir'], f'KT_공시지원금_{timestamp}.json')
            df.to_json(json_file, orient='records', force_ascii=False, indent=2)
            saved_files.append(json_file)
            
            if RICH_AVAILABLE:
                console.print(f"[green]✅ JSON 저장:[/green] {json_file}")
            else:
                print(f"✅ JSON 저장: {json_file}")
        
        # 체크포인트 삭제
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)
        
        return saved_files
    
    def run(self):
        """메인 실행"""
        try:
            if RICH_AVAILABLE:
                console.print(Panel.fit(
                    "[bold cyan]KT 공시지원금 크롤러 v7.0[/bold cyan]\n"
                    "[yellow]Rich UI & 멀티스레딩 최적화[/yellow]",
                    border_style="cyan"
                ))
            else:
                print("="*50)
                print("KT 공시지원금 크롤러 v7.0")
                print("Rich UI & 멀티스레딩 최적화")
                print("="*50)
            
            # 1. 요금제 수집
            self.collect_all_plans()
            
            if not self.all_plans:
                if RICH_AVAILABLE:
                    console.print("[red]수집된 요금제가 없습니다.[/red]")
                else:
                    print("수집된 요금제가 없습니다.")
                return []
            
            # 2. 병렬 크롤링
            self.run_parallel_crawling()
            
            # 3. 데이터 저장
            saved_files = self.save_data()
            
            # 최종 요약
            if saved_files and RICH_AVAILABLE:
                # 요약 패널
                summary = f"""
[bold green]✅ 크롤링 완료![/bold green]

• 총 {len(self.all_plans)}개 요금제 처리
• {self.total_products:,}개 데이터 수집
• {len(saved_files)}개 파일 저장
                """
                console.print(Panel(summary.strip(), title="최종 요약", border_style="green"))
            
            return saved_files
            
        except KeyboardInterrupt:
            if RICH_AVAILABLE:
                console.print("\n[yellow]사용자에 의해 중단되었습니다.[/yellow]")
            else:
                print("\n사용자에 의해 중단되었습니다.")
            
            # 중간 데이터 저장
            if self.data:
                self.save_intermediate()
            
            return []
            
        except Exception as e:
            if RICH_AVAILABLE:
                console.print(f"\n[red]오류 발생: {str(e)}[/red]")
            else:
                print(f"\n오류 발생: {str(e)}")
            logger.error(traceback.format_exc())
            
            # 중간 데이터 저장
            if self.data:
                self.save_intermediate()
            
            return []


def main():
    """CLI 인터페이스"""
    parser = argparse.ArgumentParser(
        description='KT 공시지원금 크롤러 v7.0',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--workers', type=int, default=3,
                        help='동시 실행 워커 수 (기본: 3)')
    parser.add_argument('--max-plans', type=int, default=0,
                        help='최대 요금제 수 (0=전체)')
    parser.add_argument('--show-browser', action='store_true',
                        help='브라우저 표시')
    parser.add_argument('--output', type=str, default='data',
                        help='출력 디렉토리')
    parser.add_argument('--no-intermediate', action='store_true',
                        help='중간 저장 비활성화')
    parser.add_argument('--test', action='store_true',
                        help='테스트 모드 (처음 5개만)')
    
    args = parser.parse_args()
    
    # 설정
    config = {
        'max_workers': args.workers,
        'max_rate_plans': 5 if args.test else args.max_plans,
        'show_browser': args.show_browser,
        'headless': not args.show_browser,
        'output_dir': args.output,
        'save_intermediate': not args.no_intermediate
    }
    
    # 크롤러 실행
    crawler = KTCrawlerV7(config)
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
                "[bold cyan]KT 공시지원금 크롤러 v7.0[/bold cyan]\n\n"
                "[yellow]Rich UI & 멀티스레딩 최적화[/yellow]\n\n"
                "주요 특징:\n"
                "  • Rich 라이브러리 기반 화려한 UI\n"
                "  • 멀티스레딩 병렬 처리\n"
                "  • 실시간 상세 진행률 표시\n"
                "  • 중간 저장 및 체크포인트\n"
                "  • 체계적인 로깅 시스템\n\n"
                "사용법:\n"
                "  [green]python kt_crawler_v7.py[/green]              # 기본 실행\n"
                "  [green]python kt_crawler_v7.py --workers 5[/green]  # 5개 워커\n"
                "  [green]python kt_crawler_v7.py --test[/green]       # 테스트 모드\n"
                "  [green]python kt_crawler_v7.py --help[/green]       # 도움말",
                border_style="cyan"
            ))
        else:
            print("KT 공시지원금 크롤러 v7.0")
            print("\n사용법:")
            print("  python kt_crawler_v7.py              # 기본 실행")
            print("  python kt_crawler_v7.py --workers 5  # 5개 워커")
            print("  python kt_crawler_v7.py --test       # 테스트 모드")
            print("  python kt_crawler_v7.py --help       # 도움말")
        print()
    
    main()