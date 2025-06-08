#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
통합 통신사 크롤러 v1.0 - SKT, KT, LG U+ 통합
멀티스레딩 병렬 처리, Rich UI, 실시간 진행률 표시

주요 특징:
    - 3개 통신사 데이터 통합 수집
    - 통일된 데이터 구조
    - 병렬 처리로 빠른 수집
    - 실시간 진행률 표시
    - 하나의 파일로 통합 저장

작성일: 2025-01-11
버전: 1.0
"""

import time
import json
import re
import os
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, NoAlertPresentException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from datetime import datetime
import logging
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import traceback
from typing import List, Dict, Optional, Tuple
from abc import ABC, abstractmethod
from urllib.parse import urlencode, quote_plus
import pickle

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
    handlers=[logging.FileHandler('unified_crawler.log', encoding='utf-8')]
)
logger = logging.getLogger(__name__)

# 기본 디렉토리 설정
DATA_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(DATA_DIR, exist_ok=True)


class BaseCarrierCrawler(ABC):
    """통신사 크롤러 베이스 클래스"""
    
    def __init__(self, carrier_name, config=None):
        self.carrier_name = carrier_name
        self.data = []
        self.data_lock = threading.Lock()
        
        # 기본 설정
        self.config = {
            'headless': True,
            'max_workers': 3,
            'retry_count': 2,
            'page_load_timeout': 20,
            'element_wait_timeout': 10,
            'show_browser': False
        }
        
        if config:
            self.config.update(config)
        
        # 통계
        self.completed_count = 0
        self.failed_count = 0
        self.total_items = 0
        self.status_lock = threading.Lock()
        
    def create_driver(self):
        """Chrome 드라이버 생성"""
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # 성능 최적화
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-images')
        options.add_argument('--window-size=1920,1080')
        
        # 메모리 최적화
        options.add_argument('--memory-pressure-off')
        options.add_argument('--disable-background-timer-throttling')
        
        prefs = {
            "profile.default_content_setting_values.images": 2,
            "profile.default_content_setting_values.notifications": 2,
        }
        options.add_experimental_option("prefs", prefs)
        
        if self.config['headless'] and not self.config['show_browser']:
            options.add_argument('--headless=new')
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(self.config['page_load_timeout'])
        driver.implicitly_wait(3)
        
        return driver
    
    def normalize_data(self, item):
        """데이터 정규화 - 통일된 구조로 변환"""
        normalized = {
            'carrier': self.carrier_name,
            'subscription_type': item.get('subscription_type', ''),
            'network_type': item.get('network_type', ''),
            'plan_category': item.get('plan_category', ''),
            'plan_name': item.get('plan_name', ''),
            'plan_id': item.get('plan_id', ''),
            'monthly_fee': self.clean_price(item.get('monthly_fee', 0)),
            'device_name': item.get('device_name', ''),
            'manufacturer': item.get('manufacturer', ''),
            'release_price': self.clean_price(item.get('release_price', 0)),
            'public_support_fee': self.clean_price(item.get('public_support_fee', 0)),
            'additional_support_fee': self.clean_price(item.get('additional_support_fee', 0)),
            'total_support_fee': self.clean_price(item.get('total_support_fee', 0)),
            'device_discount_24': self.clean_price(item.get('device_discount_24', 0)),
            'plan_discount_24': self.clean_price(item.get('plan_discount_24', 0)),
            'final_price': self.clean_price(item.get('final_price', 0)),
            'date': item.get('date', ''),
            'crawled_at': item.get('crawled_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        }
        
        # 총 지원금이 0이면 계산
        if normalized['total_support_fee'] == 0:
            normalized['total_support_fee'] = normalized['public_support_fee'] + normalized['additional_support_fee']
        
        return normalized
    
    def clean_price(self, price):
        """가격 정리"""
        if isinstance(price, (int, float)):
            return int(price)
        if not price:
            return 0
        cleaned = re.sub(r'[^0-9]', '', str(price))
        try:
            return int(cleaned) if cleaned else 0
        except:
            return 0
    
    @abstractmethod
    def collect_data(self):
        """데이터 수집 - 각 통신사별 구현 필요"""
        pass
    
    @abstractmethod
    def get_carrier_display_name(self):
        """표시용 통신사명"""
        pass


class SKTCrawler(BaseCarrierCrawler):
    """SKT T world 크롤러"""
    
    def __init__(self, config=None):
        super().__init__('SKT', config)
        self.base_url = "https://shop.tworld.co.kr"
        self.rate_plans = []
        self.categories = []
        self.all_combinations = []
        
    def get_carrier_display_name(self):
        return "SKT T world"
    
    def collect_data(self):
        """SKT 데이터 수집"""
        # 1. 요금제 수집
        self._collect_rate_plans()
        
        if not self.rate_plans:
            logger.error("SKT: 수집된 요금제가 없습니다")
            return
        
        # 2. 조합 준비
        self._prepare_combinations()
        
        # 3. 병렬 크롤링
        self._run_parallel_crawling()
    
    def _collect_rate_plans(self):
        """요금제 목록 수집"""
        driver = self.create_driver()
        
        try:
            url = "https://shop.tworld.co.kr/wireless/product/subscription/list"
            driver.get(url)
            time.sleep(5)
            
            # 카테고리 수집
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
                except:
                    continue
            
            # 각 카테고리의 요금제 수집
            for category in self.categories:
                try:
                    # 카테고리 클릭
                    cat_elem = driver.find_element(
                        By.CSS_SELECTOR,
                        f"li.type-item[data-category-id='{category['id']}']"
                    )
                    driver.execute_script("arguments[0].click();", cat_elem)
                    time.sleep(2)
                    
                    # 요금제 수집
                    plans = self._collect_plans_in_category(driver, category)
                    self.rate_plans.extend(plans)
                    
                except Exception as e:
                    logger.error(f"SKT 카테고리 처리 오류: {e}")
            
            # 중복 제거
            unique_plans = {}
            for plan in self.rate_plans:
                unique_plans[plan['id']] = plan
            self.rate_plans = list(unique_plans.values())
            
            if self.config.get('max_rate_plans', 0) > 0:
                self.rate_plans = self.rate_plans[:self.config['max_rate_plans']]
            
            logger.info(f"SKT: {len(self.rate_plans)}개 요금제 수집 완료")
            
        finally:
            driver.quit()
    
    def _collect_plans_in_category(self, driver, category):
        """카테고리 내 요금제 수집"""
        plans = []
        
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "ul.phone-charge-list"))
            )
            
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
                    
        except Exception as e:
            logger.debug(f"요금제 수집 오류: {e}")
            
        return plans
    
    def _prepare_combinations(self):
        """크롤링 조합 준비"""
        scrb_type = {'value': '31', 'name': '기기변경'}
        
        network_types = [
            {'code': '5G', 'name': '5G'},
            {'code': 'PHONE', 'name': 'LTE'}
        ]
        
        for plan in self.rate_plans:
            for network in network_types:
                self.all_combinations.append({
                    'plan': plan,
                    'network': network,
                    'scrb_type': scrb_type
                })
    
    def _run_parallel_crawling(self):
        """병렬 크롤링 실행"""
        with ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            futures = []
            
            for i, combo in enumerate(self.all_combinations):
                future = executor.submit(self._process_combination, i, combo)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"SKT 처리 오류: {e}")
    
    def _process_combination(self, index, combo):
        """단일 조합 처리"""
        driver = None
        
        try:
            driver = self.create_driver()
            
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
            url = f"{self.base_url}/notice?{urlencode(params, quote_via=quote_plus)}"
            
            driver.get(url)
            time.sleep(2)
            
            # 데이터 수집
            items = self._collect_page_data(driver, combo)
            
            if items:
                with self.data_lock:
                    for item in items:
                        normalized = self.normalize_data(item)
                        self.data.append(normalized)
                    self.total_items += len(items)
                    self.completed_count += 1
            else:
                with self.status_lock:
                    self.failed_count += 1
                    
        except Exception as e:
            logger.error(f"SKT 조합 처리 오류: {e}")
            with self.status_lock:
                self.failed_count += 1
                
        finally:
            if driver:
                driver.quit()
    
    def _collect_page_data(self, driver, combo):
        """페이지 데이터 수집"""
        items = []
        
        try:
            tables = driver.find_elements(By.CSS_SELECTOR, "table.disclosure-list, table")
            
            for table in tables:
                tbody = table.find_element(By.TAG_NAME, "tbody")
                rows = tbody.find_elements(By.TAG_NAME, "tr")
                
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cells) >= 6:
                        device_name = cells[0].text.strip()
                        date_text = cells[1].text.strip()
                        release_price = self.clean_price(cells[2].text)
                        public_fee = self.clean_price(cells[3].text)
                        add_fee = self.clean_price(cells[5].text) if len(cells) > 5 else 0
                        
                        if device_name and public_fee > 0:
                            # 다른 가입유형 추가
                            for sub_type in [('11', '신규가입'), ('31', '기기변경'), ('41', '번호이동')]:
                                item = {
                                    'subscription_type': sub_type[1],
                                    'network_type': combo['network']['name'],
                                    'plan_category': combo['plan']['category'],
                                    'plan_name': combo['plan']['name'],
                                    'plan_id': combo['plan']['id'],
                                    'monthly_fee': combo['plan']['monthly_fee'],
                                    'device_name': device_name,
                                    'manufacturer': self._get_manufacturer(device_name),
                                    'release_price': release_price,
                                    'public_support_fee': public_fee,
                                    'additional_support_fee': add_fee,
                                    'total_support_fee': public_fee + add_fee,
                                    'date': date_text,
                                    'crawled_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                }
                                items.append(item)
                            
        except Exception as e:
            logger.debug(f"SKT 페이지 데이터 수집 오류: {e}")
        
        return items
    
    def _get_manufacturer(self, device_name):
        """제조사 추출"""
        name_lower = device_name.lower()
        if '갤럭시' in name_lower or 'galaxy' in name_lower:
            return '삼성'
        elif '아이폰' in name_lower or 'iphone' in name_lower:
            return '애플'
        elif 'lg' in name_lower:
            return 'LG'
        elif '샤오미' in name_lower or 'xiaomi' in name_lower:
            return '샤오미'
        else:
            return '기타'


class KTCrawler(BaseCarrierCrawler):
    """KT 크롤러"""
    
    def __init__(self, config=None):
        super().__init__('KT', config)
        self.base_url = "https://shop.kt.com/smart/supportAmtList.do"
        self.all_plans = []
        
    def get_carrier_display_name(self):
        return "KT"
    
    def collect_data(self):
        """KT 데이터 수집"""
        # 1. 요금제 수집
        self._collect_all_plans()
        
        if not self.all_plans:
            logger.error("KT: 수집된 요금제가 없습니다")
            return
        
        # 2. 병렬 크롤링
        self._run_parallel_crawling()
    
    def _collect_all_plans(self):
        """모든 요금제 수집"""
        driver = self.create_driver()
        
        try:
            driver.get(self.base_url)
            time.sleep(3)
            self._handle_alert(driver)
            
            # 팝업 닫기
            driver.execute_script("""
                document.querySelectorAll('.close, [class*="close"]').forEach(btn => {
                    try { btn.click(); } catch(e) {}
                });
            """)
            
            # 요금제 모달 열기
            driver.execute_script("""
                const buttons = document.querySelectorAll('button');
                for (let btn of buttons) {
                    const onclick = btn.getAttribute('onclick') || '';
                    if (onclick.includes("gaEventTracker(false, 'Shop_공시지원금', '카테고리탭', '요금제변경')")) {
                        btn.click();
                        break;
                    }
                }
            """)
            time.sleep(2)
            
            # 5G 요금제
            plans_5g = self._collect_plans_from_tab(driver, '5G')
            self.all_plans.extend(plans_5g)
            
            # LTE 요금제
            if self._switch_to_lte_tab(driver):
                plans_lte = self._collect_plans_from_tab(driver, 'LTE')
                self.all_plans.extend(plans_lte)
            
            # 모달 닫기
            driver.execute_script("""
                const closeButtons = document.querySelectorAll('.layerWrap .close, .modal .close');
                for (let btn of closeButtons) {
                    if (btn.offsetParent !== null) {
                        btn.click();
                        break;
                    }
                }
            """)
            
            if self.config.get('max_rate_plans', 0) > 0:
                self.all_plans = self.all_plans[:self.config['max_rate_plans']]
            
            logger.info(f"KT: {len(self.all_plans)}개 요금제 수집 완료")
            
        finally:
            driver.quit()
    
    def _handle_alert(self, driver):
        """Alert 처리"""
        try:
            alert = driver.switch_to.alert
            alert.accept()
            time.sleep(0.5)
            return True
        except NoAlertPresentException:
            return False
    
    def _collect_plans_from_tab(self, driver, plan_type):
        """탭에서 요금제 수집"""
        plans = []
        
        try:
            # 전체요금제 클릭
            driver.execute_script("""
                const allBtn = document.querySelector('#pplGroupObj_ALL');
                if (allBtn) allBtn.click();
            """)
            time.sleep(1.5)
            
            raw_plans = driver.execute_script("""
                const plans = [];
                const chargeItems = document.querySelectorAll('.chargeItemCase');
                
                chargeItems.forEach((item) => {
                    const nameElem = item.querySelector('.prodName');
                    if (nameElem && item.offsetParent !== null) {
                        let planName = nameElem.textContent.trim().split('\\n')[0];
                        const itemId = item.id || '';
                        const planId = itemId.match(/pplListObj_(\\d+)/)?.[1] || '';
                        
                        const priceElem = item.querySelector('.price');
                        const priceText = priceElem?.textContent || '';
                        const monthlyFee = parseInt(priceText.match(/([0-9,]+)원/)?.[1]?.replace(/,/g, '') || '0');
                        
                        if (planName && planName.length > 3) {
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
            
        except Exception as e:
            logger.error(f"KT 요금제 수집 오류: {e}")
        
        return plans
    
    def _switch_to_lte_tab(self, driver):
        """LTE 탭 전환"""
        try:
            driver.execute_script("""
                const lteTab = document.querySelector('#TAB_LTE button');
                if (lteTab) {
                    lteTab.click();
                    return true;
                }
                return false;
            """)
            time.sleep(1.5)
            return True
        except:
            return False
    
    def _run_parallel_crawling(self):
        """병렬 크롤링"""
        with ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            futures = []
            
            for i, plan in enumerate(self.all_plans):
                future = executor.submit(self._process_plan, i, plan)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"KT 처리 오류: {e}")
    
    def _process_plan(self, index, plan):
        """단일 요금제 처리"""
        driver = None
        
        try:
            driver = self.create_driver()
            
            driver.get(self.base_url)
            time.sleep(3)
            self._handle_alert(driver)
            
            # 팝업 닫기
            driver.execute_script("""
                document.querySelectorAll('.close, [class*="close"]').forEach(btn => {
                    try { btn.click(); } catch(e) {}
                });
            """)
            
            # 모달 열기 및 요금제 선택
            if self._select_plan(driver, plan):
                # 데이터 수집
                products = self._collect_products(driver, plan)
                
                if products:
                    with self.data_lock:
                        for product in products:
                            normalized = self.normalize_data(product)
                            self.data.append(normalized)
                        self.total_items += len(products)
                        self.completed_count += 1
                else:
                    with self.status_lock:
                        self.failed_count += 1
            else:
                with self.status_lock:
                    self.failed_count += 1
                    
        except Exception as e:
            logger.error(f"KT 요금제 처리 오류: {e}")
            with self.status_lock:
                self.failed_count += 1
                
        finally:
            if driver:
                driver.quit()
    
    def _select_plan(self, driver, plan):
        """요금제 선택"""
        try:
            # 모달 열기
            driver.execute_script("""
                const buttons = document.querySelectorAll('button');
                for (let btn of buttons) {
                    const onclick = btn.getAttribute('onclick') || '';
                    if (onclick.includes("gaEventTracker(false, 'Shop_공시지원금', '카테고리탭', '요금제변경')")) {
                        btn.click();
                        break;
                    }
                }
            """)
            time.sleep(2)
            
            # LTE 탭 전환 필요시
            if plan['plan_type'] == 'LTE':
                self._switch_to_lte_tab(driver)
            
            # 전체요금제
            driver.execute_script("""
                const allBtn = document.querySelector('#pplGroupObj_ALL');
                if (allBtn) allBtn.click();
            """)
            time.sleep(1)
            
            # 요금제 선택
            driver.execute_script(f"""
                const planItem = document.querySelector('#pplListObj_{plan['id']}');
                if (planItem) {{
                    planItem.click();
                }}
            """)
            time.sleep(1)
            
            # 선택완료
            driver.execute_script("""
                const buttons = document.querySelectorAll('button');
                for (let btn of buttons) {
                    if (btn.textContent.includes('선택완료') || btn.textContent.includes('확인')) {
                        btn.click();
                        break;
                    }
                }
            """)
            time.sleep(2)
            self._handle_alert(driver)
            
            return True
            
        except Exception as e:
            logger.error(f"KT 요금제 선택 오류: {e}")
            return False
    
    def _collect_products(self, driver, plan):
        """제품 데이터 수집"""
        all_products = []
        collected_names = set()
        
        try:
            products = driver.execute_script("""
                const products = [];
                const items = document.querySelectorAll('#prodList > li');
                
                items.forEach(item => {
                    const nameElem = item.querySelector('.prodName, strong');
                    if (!nameElem) return;
                    
                    const deviceName = nameElem.textContent.trim();
                    if (!deviceName || deviceName.length < 5) return;
                    
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
                        plan_discount_24: extractPrice(fullText, '요금할인')
                    };
                    
                    if (data.public_support_fee === 0 && data.device_discount_24 > 0) {
                        data.public_support_fee = Math.round(data.device_discount_24 * 0.7);
                        data.additional_support_fee = data.device_discount_24 - data.public_support_fee;
                    }
                    
                    if (data.release_price > 100000) {
                        products.push(data);
                    }
                });
                
                return products;
            """)
            
            # 가입유형 추가
            for product in products:
                if product['device_name'] not in collected_names:
                    collected_names.add(product['device_name'])
                    
                    for sub_type in ['신규가입', '번호이동', '기기변경']:
                        item = product.copy()
                        item.update({
                            'subscription_type': sub_type,
                            'network_type': plan['plan_type'],
                            'plan_name': plan['name'],
                            'plan_id': plan['id'],
                            'monthly_fee': plan.get('monthlyFee', 0),
                            'manufacturer': self._get_manufacturer(product['device_name']),
                            'crawled_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
                        all_products.append(item)
            
        except Exception as e:
            logger.error(f"KT 제품 수집 오류: {e}")
        
        return all_products
    
    def _get_manufacturer(self, device_name):
        """제조사 추출"""
        name_lower = device_name.lower()
        if '갤럭시' in name_lower or 'galaxy' in name_lower:
            return '삼성'
        elif '아이폰' in name_lower or 'iphone' in name_lower:
            return '애플'
        else:
            return '기타'


class LGCrawler(BaseCarrierCrawler):
    """LG U+ 크롤러"""
    
    def __init__(self, config=None):
        super().__init__('LG U+', config)
        self.base_url = "https://www.lguplus.com/mobile/financing-model"
        self.all_combinations = []
        
    def get_carrier_display_name(self):
        return "LG U+"
    
    def collect_data(self):
        """LG U+ 데이터 수집"""
        # 1. 조합 수집
        self._collect_all_combinations()
        
        if not self.all_combinations:
            logger.error("LG U+: 수집된 조합이 없습니다")
            return
        
        # 2. 병렬 크롤링
        self._run_parallel_crawling()
    
    def _collect_all_combinations(self):
        """모든 조합 수집"""
        driver = self.create_driver()
        
        try:
            subscription_types = [
                ('1', '기기변경'),
                ('2', '번호이동'), 
                ('3', '신규가입')
            ]
            
            device_types = [
                ('00', '5G'),
                ('01', 'LTE')
            ]
            
            for sub_value, sub_name in subscription_types:
                for dev_value, dev_name in device_types:
                    rate_plans = self._collect_rate_plans_for_combination(
                        driver, sub_value, sub_name, dev_value, dev_name
                    )
                    
                    if self.config.get('max_rate_plans', 0) > 0:
                        rate_plans = rate_plans[:self.config['max_rate_plans']]
                    
                    for rate_plan in rate_plans:
                        self.all_combinations.append({
                            'sub_value': sub_value,
                            'sub_name': sub_name,
                            'dev_value': dev_value,
                            'dev_name': dev_name,
                            'rate_plan': rate_plan
                        })
            
            logger.info(f"LG U+: {len(self.all_combinations)}개 조합 준비 완료")
            
        finally:
            driver.quit()
    
    def _collect_rate_plans_for_combination(self, driver, sub_value, sub_name, dev_value, dev_name):
        """특정 조합의 요금제 수집"""
        try:
            driver.get(self.base_url)
            time.sleep(2)
            
            # 가입유형 선택
            sub_radio = self._safe_find_element(driver, By.CSS_SELECTOR, f'input[name="가입유형"][id="{sub_value}"]')
            if sub_radio and not sub_radio.is_selected():
                label = driver.find_element(By.CSS_SELECTOR, f'label[for="{sub_value}"]')
                driver.execute_script("arguments[0].click();", label)
                time.sleep(0.5)
            
            # 기기종류 선택
            dev_radio = self._safe_find_element(driver, By.CSS_SELECTOR, f'input[name="기기종류"][id="{dev_value}"]')
            if dev_radio and not dev_radio.is_selected():
                label = driver.find_element(By.CSS_SELECTOR, f'label[for="{dev_value}"]')
                driver.execute_script("arguments[0].click();", label)
                time.sleep(0.5)
            
            # 요금제 모달 열기
            more_btn = self._safe_find_element(driver, By.CSS_SELECTOR, 'button.c-btn-rect-2')
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
            close_btn = self._safe_find_element(driver, By.CSS_SELECTOR, 'button.c-btn-close')
            if close_btn:
                driver.execute_script("arguments[0].click();", close_btn)
                time.sleep(0.5)
            
            return rate_plans
            
        except Exception as e:
            logger.error(f"LG U+ 요금제 수집 오류: {e}")
            return []
    
    def _safe_find_element(self, driver, by, value, timeout=5):
        """안전한 요소 찾기"""
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, value))
            )
            return element
        except:
            return None
    
    def _run_parallel_crawling(self):
        """병렬 크롤링"""
        with ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            futures = []
            
            for i, combo in enumerate(self.all_combinations):
                future = executor.submit(self._process_combination, i, combo)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"LG U+ 처리 오류: {e}")
    
    def _process_combination(self, index, combo):
        """단일 조합 처리"""
        driver = None
        
        try:
            driver = self.create_driver()
            
            driver.get(self.base_url)
            time.sleep(2)
            
            # 옵션 선택
            self._select_options(driver, combo)
            
            # 데이터 추출
            extracted_data = self._extract_data(driver, combo)
            
            if extracted_data:
                with self.data_lock:
                    for item in extracted_data:
                        normalized = self.normalize_data(item)
                        self.data.append(normalized)
                    self.total_items += len(extracted_data)
                    self.completed_count += 1
            else:
                with self.status_lock:
                    self.failed_count += 1
                    
        except Exception as e:
            logger.error(f"LG U+ 조합 처리 오류: {e}")
            with self.status_lock:
                self.failed_count += 1
                
        finally:
            if driver:
                driver.quit()
    
    def _select_options(self, driver, combo):
        """옵션 선택"""
        # 가입유형
        sub_radio = self._safe_find_element(driver, By.CSS_SELECTOR, f'input[name="가입유형"][id="{combo["sub_value"]}"]')
        if sub_radio and not sub_radio.is_selected():
            label = driver.find_element(By.CSS_SELECTOR, f'label[for="{combo["sub_value"]}"]')
            driver.execute_script("arguments[0].click();", label)
            time.sleep(0.5)
        
        # 기기종류
        dev_radio = self._safe_find_element(driver, By.CSS_SELECTOR, f'input[name="기기종류"][id="{combo["dev_value"]}"]')
        if dev_radio and not dev_radio.is_selected():
            label = driver.find_element(By.CSS_SELECTOR, f'label[for="{combo["dev_value"]}"]')
            driver.execute_script("arguments[0].click();", label)
            time.sleep(0.5)
        
        # 요금제 선택
        more_btn = self._safe_find_element(driver, By.CSS_SELECTOR, 'button.c-btn-rect-2')
        if more_btn:
            driver.execute_script("arguments[0].click();", more_btn)
            time.sleep(1)
            
            # 요금제 라디오 선택
            rate_radio = self._safe_find_element(driver, By.CSS_SELECTOR, f'input[id="{combo["rate_plan"]["id"]}"]')
            if rate_radio and not rate_radio.is_selected():
                label = driver.find_element(By.CSS_SELECTOR, f'label[for="{combo["rate_plan"]["id"]}"]')
                driver.execute_script("arguments[0].click();", label)
                time.sleep(0.3)
            
            # 적용 버튼
            apply_btn = self._safe_find_element(driver, By.CSS_SELECTOR, 'button.c-btn-solid-1-m')
            if apply_btn:
                driver.execute_script("arguments[0].click();", apply_btn)
                time.sleep(1.5)
        
        # 제조사 전체 선택
        all_checkbox = self._safe_find_element(driver, By.CSS_SELECTOR, 'input[id="전체"]')
        if all_checkbox and not all_checkbox.is_selected():
            label = driver.find_element(By.CSS_SELECTOR, 'label[for="전체"]')
            driver.execute_script("arguments[0].click();", label)
            time.sleep(1)
    
    def _extract_data(self, driver, combo):
        """데이터 추출"""
        all_data = []
        
        try:
            tables = driver.find_elements(By.CSS_SELECTOR, 'table')
            if not tables:
                return all_data
            
            table = tables[0]
            rows = table.find_elements(By.CSS_SELECTOR, 'tbody tr')
            
            current_device = None
            current_price = None
            current_date = None
            
            for row in rows:
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
                        all_data.append(data)
                
                # 기존 기기의 다른 약정
                elif len(cells) >= 6 and current_device:
                    data = self._create_data_dict(
                        cells, combo, current_device, current_price, current_date
                    )
                    if data:
                        all_data.append(data)
                        
        except Exception as e:
            logger.debug(f"LG U+ 데이터 추출 오류: {e}")
        
        return all_data
    
    def _create_data_dict(self, cells, combo, device, price, date):
        """데이터 딕셔너리 생성"""
        try:
            if len(cells) >= 6:
                return {
                    'subscription_type': combo['sub_name'],
                    'network_type': combo['dev_name'],
                    'plan_name': combo['rate_plan']['name'],
                    'plan_id': combo['rate_plan'].get('value', ''),
                    'device_name': device,
                    'release_price': price,
                    'date': date,
                    'public_support_fee': cells[1].text.strip(),
                    'additional_support_fee': cells[2].text.strip(),
                    'total_support_fee': cells[3].text.strip(),
                    'device_discount_24': cells[4].text.strip(),
                    'final_price': cells[5].text.strip(),
                    'manufacturer': self._get_manufacturer(device),
                    'crawled_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            return None
        except:
            return None
    
    def _get_manufacturer(self, device_name):
        """제조사 추출"""
        name_lower = device_name.lower()
        if '갤럭시' in name_lower or 'galaxy' in name_lower:
            return '삼성'
        elif '아이폰' in name_lower or 'iphone' in name_lower:
            return '애플'
        else:
            return '기타'


class UnifiedCarrierCrawler:
    """통합 통신사 크롤러"""
    
    def __init__(self, config=None):
        self.config = {
            'carriers': ['SKT', 'KT', 'LG'],  # 크롤링할 통신사
            'max_workers': 3,
            'max_rate_plans': 0,  # 0 = 전체
            'output_dir': DATA_DIR,
            'save_formats': ['excel', 'csv', 'json'],
            'show_browser': False,
            'headless': True
        }
        
        if config:
            self.config.update(config)
        
        self.all_data = []
        self.start_time = None
        
    def run(self):
        """전체 크롤링 실행"""
        self.start_time = time.time()
        
        if RICH_AVAILABLE:
            console.print(Panel.fit(
                "[bold cyan]통합 통신사 크롤러 v1.0[/bold cyan]\n"
                "[yellow]SKT, KT, LG U+ 통합 수집[/yellow]\n"
                f"[dim]대상: {', '.join(self.config['carriers'])} | 출력: {', '.join(self.config['save_formats'])}[/dim]",
                border_style="cyan"
            ))
        else:
            print("\n" + "="*60)
            print("통합 통신사 크롤러 v1.0")
            print("SKT, KT, LG U+ 통합 수집")
            print("="*60)
        
        # 각 통신사별 크롤링
        carrier_results = {}
        
        for carrier in self.config['carriers']:
            if carrier not in ['SKT', 'KT', 'LG']:
                continue
            
            if RICH_AVAILABLE:
                console.print(f"\n[bold magenta]{'='*50}[/bold magenta]")
                console.print(f"[bold cyan]{carrier} 크롤링 시작[/bold cyan]")
                console.print(f"[bold magenta]{'='*50}[/bold magenta]\n")
            else:
                print(f"\n{'='*50}")
                print(f"{carrier} 크롤링 시작")
                print(f"{'='*50}\n")
            
            try:
                # 크롤러 생성
                if carrier == 'SKT':
                    crawler = SKTCrawler(self.config)
                elif carrier == 'KT':
                    crawler = KTCrawler(self.config)
                elif carrier == 'LG':
                    crawler = LGCrawler(self.config)
                
                # 데이터 수집
                crawler.collect_data()
                
                # 결과 저장
                if crawler.data:
                    self.all_data.extend(crawler.data)
                    carrier_results[carrier] = {
                        'success': True,
                        'count': len(crawler.data),
                        'completed': crawler.completed_count,
                        'failed': crawler.failed_count
                    }
                else:
                    carrier_results[carrier] = {
                        'success': False,
                        'count': 0,
                        'completed': crawler.completed_count,
                        'failed': crawler.failed_count
                    }
                
                if RICH_AVAILABLE:
                    # 통신사별 결과 표시
                    table = Table(title=f"{carrier} 수집 결과", show_header=True)
                    table.add_column("항목", style="cyan")
                    table.add_column("수치", justify="right", style="yellow")
                    
                    table.add_row("수집 데이터", f"{len(crawler.data):,}개")
                    table.add_row("성공", f"{crawler.completed_count:,}개")
                    table.add_row("실패", f"{crawler.failed_count:,}개")
                    
                    console.print(table)
                else:
                    print(f"\n{carrier} 수집 결과:")
                    print(f"  수집 데이터: {len(crawler.data)}개")
                    print(f"  성공: {crawler.completed_count}개")
                    print(f"  실패: {crawler.failed_count}개")
                
            except Exception as e:
                logger.error(f"{carrier} 크롤링 오류: {e}")
                carrier_results[carrier] = {
                    'success': False,
                    'count': 0,
                    'error': str(e)
                }
        
        # 전체 데이터 저장
        saved_files = self.save_all_data(carrier_results)
        
        # 최종 결과 표시
        elapsed = time.time() - self.start_time
        
        if RICH_AVAILABLE:
            # 최종 통계
            console.print(f"\n[bold magenta]{'='*60}[/bold magenta]")
            
            summary_table = Table(title="크롤링 최종 결과", show_header=True, header_style="bold cyan")
            summary_table.add_column("통신사", style="yellow")
            summary_table.add_column("상태", style="green")
            summary_table.add_column("수집 데이터", justify="right", style="cyan")
            
            for carrier, result in carrier_results.items():
                status = "[green]성공[/green]" if result['success'] else "[red]실패[/red]"
                summary_table.add_row(carrier, status, f"{result['count']:,}개")
            
            summary_table.add_row("", "", "")
            summary_table.add_row("[bold]총계[/bold]", "", f"[bold]{len(self.all_data):,}개[/bold]")
            
            console.print(summary_table)
            
            console.print(f"\n[cyan]소요 시간:[/cyan] {elapsed/60:.1f}분")
            console.print(f"[cyan]저장 파일:[/cyan] {len(saved_files)}개")
            
            for file in saved_files:
                console.print(f"  [dim]• {file}[/dim]")
        else:
            print(f"\n{'='*60}")
            print("크롤링 최종 결과")
            print(f"{'='*60}")
            for carrier, result in carrier_results.items():
                status = "성공" if result['success'] else "실패"
                print(f"{carrier}: {status} - {result['count']}개")
            print(f"\n총 데이터: {len(self.all_data)}개")
            print(f"소요 시간: {elapsed/60:.1f}분")
            print(f"저장 파일: {len(saved_files)}개")
        
        return saved_files
    
    def save_all_data(self, carrier_results):
        """통합 데이터 저장"""
        if not self.all_data:
            if RICH_AVAILABLE:
                console.print("[red]저장할 데이터가 없습니다.[/red]")
            else:
                print("저장할 데이터가 없습니다.")
            return []
        
        # DataFrame 생성
        df = pd.DataFrame(self.all_data)
        
        # 컬럼 순서 정리
        column_order = [
            'carrier', 'subscription_type', 'network_type', 'plan_category',
            'plan_name', 'plan_id', 'monthly_fee', 'device_name', 'manufacturer',
            'release_price', 'public_support_fee', 'additional_support_fee',
            'total_support_fee', 'device_discount_24', 'plan_discount_24',
            'final_price', 'date', 'crawled_at'
        ]
        
        # 존재하는 컬럼만 정렬
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]
        
        # 타임스탬프
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        saved_files = []
        
        # Excel 저장
        if 'excel' in self.config['save_formats']:
            excel_file = os.path.join(self.config['output_dir'], f'통합_통신사_지원금_{timestamp}.xlsx')
            
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                # 전체 데이터
                df.to_excel(writer, sheet_name='전체데이터', index=False)
                
                # 통신사별 시트
                for carrier in df['carrier'].unique():
                    carrier_df = df[df['carrier'] == carrier]
                    carrier_df.to_excel(writer, sheet_name=carrier, index=False)
                
                # 요약 통계
                summary_data = []
                
                # 전체 통계
                summary_data.append({
                    '구분': '전체',
                    '항목': '전체',
                    '디바이스수': df['device_name'].nunique(),
                    '요금제수': df.groupby('carrier')['plan_name'].nunique().sum(),
                    '데이터수': len(df),
                    '평균 월요금': int(df['monthly_fee'].mean()),
                    '평균 공시지원금': int(df['public_support_fee'].mean()),
                    '최대 공시지원금': int(df['public_support_fee'].max())
                })
                
                # 통신사별 통계
                for carrier in sorted(df['carrier'].unique()):
                    carrier_df = df[df['carrier'] == carrier]
                    summary_data.append({
                        '구분': '통신사',
                        '항목': carrier,
                        '디바이스수': carrier_df['device_name'].nunique(),
                        '요금제수': carrier_df['plan_name'].nunique(),
                        '데이터수': len(carrier_df),
                        '평균 월요금': int(carrier_df['monthly_fee'].mean()),
                        '평균 공시지원금': int(carrier_df['public_support_fee'].mean()),
                        '최대 공시지원금': int(carrier_df['public_support_fee'].max())
                    })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='요약', index=False)
            
            saved_files.append(excel_file)
        
        # CSV 저장
        if 'csv' in self.config['save_formats']:
            csv_file = os.path.join(self.config['output_dir'], f'통합_통신사_지원금_{timestamp}.csv')
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            saved_files.append(csv_file)
        
        # JSON 저장
        if 'json' in self.config['save_formats']:
            json_file = os.path.join(self.config['output_dir'], f'통합_통신사_지원금_{timestamp}.json')
            df.to_json(json_file, orient='records', force_ascii=False, indent=2)
            saved_files.append(json_file)
        
        return saved_files


def main():
    """메인 실행"""
    parser = argparse.ArgumentParser(
        description='통합 통신사 크롤러 v1.0',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--carriers', nargs='+', 
                        choices=['SKT', 'KT', 'LG', 'ALL'],
                        default=['ALL'],
                        help='크롤링할 통신사 (기본: ALL)')
    parser.add_argument('--workers', type=int, default=3,
                        help='통신사별 동시 실행 워커 수 (기본: 3)')
    parser.add_argument('--max-plans', type=int, default=0,
                        help='통신사별 최대 요금제 수 (0=전체)')
    parser.add_argument('--show-browser', action='store_true',
                        help='브라우저 표시')
    parser.add_argument('--output', type=str, default='data',
                        help='출력 디렉토리')
    parser.add_argument('--format', nargs='+', 
                        choices=['excel', 'csv', 'json'],
                        default=['excel', 'csv', 'json'],
                        help='저장 형식')
    parser.add_argument('--test', action='store_true',
                        help='테스트 모드 (각 통신사 5개 요금제만)')
    
    args = parser.parse_args()
    
    # 통신사 목록 처리
    if 'ALL' in args.carriers:
        carriers = ['SKT', 'KT', 'LG']
    else:
        carriers = args.carriers
    
    # 설정
    config = {
        'carriers': carriers,
        'max_workers': args.workers,
        'max_rate_plans': 5 if args.test else args.max_plans,
        'show_browser': args.show_browser,
        'headless': not args.show_browser,
        'output_dir': args.output,
        'save_formats': args.format
    }
    
    # 출력 디렉토리 생성
    os.makedirs(config['output_dir'], exist_ok=True)
    
    if RICH_AVAILABLE:
        console.print(Panel.fit(
            "[bold cyan]통합 통신사 크롤러 v1.0[/bold cyan]\n"
            "[yellow]SKT, KT, LG U+ 통합 수집[/yellow]\n\n"
            "주요 특징:\n"
            "  • 3개 통신사 데이터 통합 수집\n"
            "  • 통일된 데이터 구조\n"
            "  • 멀티스레딩 병렬 처리\n"
            "  • 실시간 진행률 표시\n"
            "  • Excel, CSV, JSON 통합 저장\n\n"
            "사용법:\n"
            "  [green]python unified_crawler.py[/green]                 # 전체 통신사\n"
            "  [green]python unified_crawler.py --carriers SKT[/green]  # SKT만\n"
            "  [green]python unified_crawler.py --test[/green]          # 테스트 모드\n"
            "  [green]python unified_crawler.py --help[/green]          # 도움말",
            border_style="cyan"
        ))
    else:
        print("\n" + "="*60)
        print("통합 통신사 크롤러 v1.0")
        print("SKT, KT, LG U+ 통합 수집")
        print("="*60)
        print("\n사용법:")
        print("  python unified_crawler.py                 # 전체 통신사")
        print("  python unified_crawler.py --carriers SKT  # SKT만")
        print("  python unified_crawler.py --test          # 테스트 모드")
        print("  python unified_crawler.py --help          # 도움말")
    
    print()
    
    # 크롤러 실행
    crawler = UnifiedCarrierCrawler(config)
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
    main()