#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
한국 통신사 3사 통합 휴대폰 지원금 크롤러 v1.0
SKT, KT, LG U+ 공시지원금 데이터 통합 수집

주요 특징:
    - 3사 데이터 통합 수집 및 저장
    - Rich UI 기반 진행상황 표시
    - 멀티스레딩 병렬 처리
    - 데이터 정합성 검증
    - 빠른 테스트 모드
    - 상세 디버깅 지원
    - 체크포인트 저장/복원

작성일: 2025-01-11
버전: 1.0
"""

import os
import sys
import time
import json
import logging
import argparse
import threading
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import traceback
from collections import defaultdict
import pickle

# Rich library imports
try:
    from rich.console import Console
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, MofNCompleteColumn
    from rich.table import Table
    from rich.panel import Panel
    from rich.live import Live
    from rich.layout import Layout
    from rich.columns import Columns
    from rich import print as rprint
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Warning: Rich library not installed. Install with: pip install rich")

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed

# Console 초기화
console = Console() if RICH_AVAILABLE else None

# 로깅 설정
def setup_logging(log_file='telecom_unified_crawler.log', level='INFO'):
    """통합 로깅 설정"""
    log_format = '%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'
    
    # 파일 핸들러
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # 콘솔 핸들러 (Rich가 없을 때만)
    handlers = [file_handler]
    if not RICH_AVAILABLE:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(console_handler)
    
    # 루트 로거 설정
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        handlers=handlers
    )
    
    # 개별 로거 반환
    return {
        'main': logging.getLogger('main'),
        'skt': logging.getLogger('skt'),
        'kt': logging.getLogger('kt'),
        'lg': logging.getLogger('lg'),
        'validator': logging.getLogger('validator')
    }

# 로거 초기화
loggers = setup_logging()
logger = loggers['main']


class DataValidator:
    """데이터 정합성 검증 클래스"""
    
    def __init__(self):
        self.logger = loggers['validator']
        self.validation_errors = []
        
    def validate_device_name(self, device_name: str) -> bool:
        """디바이스명 검증"""
        if not device_name or len(device_name) < 3:
            return False
        if device_name.lower() in ['선택하세요', '데이터없음', 'none', 'null']:
            return False
        return True
    
    def validate_price(self, price: any, field_name: str) -> Tuple[bool, int]:
        """가격 데이터 검증"""
        try:
            # 문자열인 경우 숫자만 추출
            if isinstance(price, str):
                price = int(''.join(filter(str.isdigit, price)))
            else:
                price = int(price)
            
            # 가격 범위 검증
            if field_name == '출고가' and (price < 100000 or price > 3000000):
                return False, 0
            elif field_name in ['공시지원금', '추가지원금'] and (price < 0 or price > 2000000):
                return False, 0
            elif field_name == '월요금' and (price < 10000 or price > 200000):
                return False, 0
                
            return True, price
        except:
            return False, 0
    
    def validate_row(self, row: dict) -> Tuple[bool, dict, List[str]]:
        """데이터 행 검증"""
        errors = []
        validated_row = row.copy()
        
        # 필수 필드 확인
        required_fields = ['통신사', '기기명', '출고가', '공시지원금']
        for field in required_fields:
            if field not in row or not row[field]:
                errors.append(f"필수 필드 누락: {field}")
        
        # 디바이스명 검증
        if not self.validate_device_name(row.get('기기명', '')):
            errors.append("유효하지 않은 기기명")
        
        # 가격 필드 검증
        price_fields = {
            '출고가': '출고가',
            '공시지원금': '공시지원금',
            '추가지원금': '추가지원금',
            '월요금': '월요금'
        }
        
        for field, name in price_fields.items():
            if field in row:
                valid, price = self.validate_price(row[field], name)
                if not valid:
                    errors.append(f"유효하지 않은 {name}: {row[field]}")
                else:
                    validated_row[field] = price
        
        # 날짜 검증
        if '크롤링시간' in row:
            try:
                datetime.strptime(row['크롤링시간'], '%Y-%m-%d %H:%M:%S')
            except:
                validated_row['크롤링시간'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return len(errors) == 0, validated_row, errors
    
    def validate_dataset(self, data: List[dict]) -> Tuple[List[dict], Dict]:
        """전체 데이터셋 검증"""
        self.logger.info(f"데이터 검증 시작: {len(data)}개 항목")
        
        valid_data = []
        invalid_count = 0
        error_summary = defaultdict(int)
        
        for i, row in enumerate(data):
            is_valid, validated_row, errors = self.validate_row(row)
            
            if is_valid:
                valid_data.append(validated_row)
            else:
                invalid_count += 1
                for error in errors:
                    error_summary[error] += 1
                    self.logger.debug(f"행 {i}: {error}")
        
        # 검증 결과 요약
        validation_result = {
            'total': len(data),
            'valid': len(valid_data),
            'invalid': invalid_count,
            'error_summary': dict(error_summary),
            'validation_rate': (len(valid_data) / len(data) * 100) if data else 0
        }
        
        self.logger.info(f"검증 완료: {validation_result['valid']}/{validation_result['total']} "
                        f"({validation_result['validation_rate']:.1f}% 유효)")
        
        return valid_data, validation_result


class SKTCrawler:
    """SKT T world 크롤러 (v2.0 기반)"""
    
    def __init__(self, config=None):
        self.logger = loggers['skt']
        self.base_url = "https://shop.tworld.co.kr"
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
            'max_rate_plans': 0,
            'show_browser': False
        }
        
        if config:
            self.config.update(config)
        
        self.completed_count = 0
        self.failed_count = 0
        self.total_devices = 0
        self.all_combinations = []
        
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        
        # 이미지 로딩 비활성화
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
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
    
    def collect_rate_plans(self):
        """요금제 수집"""
        self.logger.info("SKT 요금제 수집 시작...")
        driver = self.setup_driver()
        
        try:
            url = "https://shop.tworld.co.kr/wireless/product/subscription/list"
            driver.get(url)
            time.sleep(5)
            
            # 카테고리 수집
            self.collect_categories(driver)
            
            if not self.categories:
                self.logger.error("카테고리를 찾을 수 없습니다.")
                return
            
            # 각 카테고리별 요금제 수집
            for category in self.categories:
                try:
                    self.click_category(driver, category['id'])
                    time.sleep(2)
                    plans = self.collect_plans_in_category(driver, category)
                    if plans:
                        self.rate_plans.extend(plans)
                except Exception as e:
                    self.logger.error(f"카테고리 처리 오류: {e}")
            
            # 중복 제거
            unique_plans = {}
            for plan in self.rate_plans:
                unique_plans[plan['id']] = plan
            self.rate_plans = list(unique_plans.values())
            
            if self.config['max_rate_plans'] > 0:
                self.rate_plans = self.rate_plans[:self.config['max_rate_plans']]
            
            self.logger.info(f"총 {len(self.rate_plans)}개 요금제 수집 완료")
            
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
                except:
                    continue
                    
        except Exception as e:
            self.logger.error(f"카테고리 목록 수집 오류: {e}")
    
    def click_category(self, driver, category_id):
        """카테고리 클릭"""
        try:
            category_element = driver.find_element(
                By.CSS_SELECTOR, 
                f"li.type-item[data-category-id='{category_id}']"
            )
            driver.execute_script("arguments[0].click();", category_element)
        except Exception as e:
            self.logger.error(f"카테고리 클릭 오류: {e}")
            raise
    
    def collect_plans_in_category(self, driver, category):
        """카테고리별 요금제 수집"""
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
            self.logger.debug(f"요금제 수집 오류: {e}")
            
        return plans
    
    def prepare_combinations(self):
        """크롤링 조합 준비"""
        if not self.rate_plans:
            return
        
        scrb_type = {'value': '31', 'name': '기기변경'}
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
        
        self.logger.info(f"총 {len(self.all_combinations)}개 조합 준비 완료")
    
    def process_combination(self, combo_index):
        """단일 조합 처리"""
        combo = self.all_combinations[combo_index]
        driver = None
        
        try:
            driver = self.setup_driver()
            
            # URL 생성
            from urllib.parse import urlencode, quote_plus
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
            items_count = self._collect_all_pages_data(driver, combo)
            
            with self.data_lock:
                if items_count > 0:
                    self.completed_count += 1
                    self.total_devices += items_count
                else:
                    self.failed_count += 1
            
            return True
            
        except Exception as e:
            self.logger.error(f"처리 오류: {str(e)}")
            with self.data_lock:
                self.failed_count += 1
            return False
            
        finally:
            if driver:
                driver.quit()
    
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
            import re
            tables = driver.find_elements(By.CSS_SELECTOR, "table.disclosure-list, table")
            
            for table in tables:
                tbody = table.find_element(By.TAG_NAME, "tbody")
                rows = tbody.find_elements(By.TAG_NAME, "tr")
                
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cells) == 1 and ('데이터가 없습니다' in cells[0].text):
                        return items
                    
                    if len(cells) >= 6:
                        device_name = cells[0].text.strip()
                        date_text = cells[1].text.strip()
                        release_price = self.clean_price(cells[2].text)
                        public_fee = self.clean_price(cells[3].text)
                        add_fee = self.clean_price(cells[5].text) if len(cells) > 5 else 0
                        
                        if device_name and public_fee > 0:
                            item = {
                                '통신사': 'SKT',
                                '가입유형': combo['scrb_type']['name'],
                                '네트워크': combo['network']['name'],
                                '요금제_카테고리': combo['plan']['category'],
                                '요금제': combo['plan']['name'],
                                '월요금': combo['plan']['monthly_fee'],
                                '기기명': device_name,
                                '제조사': self.get_manufacturer(device_name),
                                '출고가': release_price,
                                '공시지원금': public_fee,
                                '추가지원금': add_fee,
                                '총지원금': public_fee + add_fee,
                                '공시일자': date_text,
                                '크롤링시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            items.append(item)
                            
                            with self.data_lock:
                                self.all_data.append(item)
                            
        except Exception as e:
            self.logger.debug(f"페이지 데이터 수집 오류: {e}")
        
        return items
    
    def clean_price(self, price_str):
        """가격 정리"""
        if not price_str:
            return 0
        import re
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
        elif '아이폰' in name_lower or 'iphone' in name_lower:
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
        self.logger.info("SKT 병렬 크롤링 시작...")
        
        with ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            futures = []
            for i in range(len(self.all_combinations)):
                future = executor.submit(self.process_combination, i)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Future 오류: {str(e)}")
        
        # 다른 가입유형 데이터 복사
        self._duplicate_data_for_other_types()
        
        self.logger.info(f"SKT 크롤링 완료: {len(self.all_data)}개 데이터 수집")
    
    def _duplicate_data_for_other_types(self):
        """다른 가입유형용 데이터 복사"""
        original_data = self.all_data.copy()
        other_types = [
            {'value': '11', 'name': '신규가입'},
            {'value': '41', 'name': '번호이동'}
        ]
        
        for scrb_type in other_types:
            for item in original_data:
                new_item = item.copy()
                new_item['가입유형'] = scrb_type['name']
                self.all_data.append(new_item)
    
    def crawl(self):
        """SKT 크롤링 실행"""
        try:
            # 1. 요금제 수집
            self.collect_rate_plans()
            
            if not self.rate_plans:
                self.logger.error("수집된 요금제가 없습니다.")
                return []
            
            # 2. 조합 준비
            self.prepare_combinations()
            
            # 3. 병렬 크롤링
            self.run_parallel_crawling()
            
            return self.all_data
            
        except Exception as e:
            self.logger.error(f"SKT 크롤링 오류: {e}")
            traceback.print_exc()
            return self.all_data


class KTCrawler:
    """KT 크롤러 (v7.0 기반)"""
    
    def __init__(self, config=None):
        self.logger = loggers['kt']
        self.base_url = "https://shop.kt.com/smart/supportAmtList.do"
        self.data = []
        self.data_lock = threading.Lock()
        
        # 기본 설정
        self.config = {
            'headless': True,
            'page_load_timeout': 20,
            'element_wait_timeout': 10,
            'max_workers': 3,
            'retry_count': 2,
            'max_rate_plans': 0,
            'show_browser': False
        }
        
        if config:
            self.config.update(config)
        
        self.all_plans = []
        self.completed_count = 0
        self.failed_count = 0
        self.total_products = 0
        
    def create_driver(self):
        """Chrome 드라이버 생성"""
        chrome_options = Options()
        
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # 성능 최적화
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-images')
        chrome_options.page_load_strategy = 'eager'
        
        if self.config['headless'] and not self.config.get('show_browser'):
            chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--window-size=1920,1080')
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.maximize_window()
        driver.set_page_load_timeout(self.config['page_load_timeout'])
        driver.implicitly_wait(3)
        
        return driver
    
    def collect_all_plans(self):
        """모든 요금제 목록 수집"""
        self.logger.info("KT 요금제 수집 시작...")
        driver = self.create_driver()
        
        try:
            driver.get(self.base_url)
            time.sleep(3)
            
            # 팝업 처리
            self.handle_alert(driver)
            self._close_popups(driver)
            
            all_plans = []
            
            # 모달 열기
            if not self._open_plan_modal(driver):
                raise Exception("요금제 모달을 열 수 없습니다")
            
            # 5G 요금제 수집
            plans_5g = self._collect_plans_from_tab(driver, '5G')
            all_plans.extend(plans_5g)
            
            # LTE 요금제 수집
            if self._switch_to_lte_tab(driver):
                plans_lte = self._collect_plans_from_tab(driver, 'LTE')
                all_plans.extend(plans_lte)
            
            # 모달 닫기
            self._close_modal(driver)
            
            # 요금제 수 제한
            if self.config['max_rate_plans'] > 0:
                all_plans = all_plans[:self.config['max_rate_plans']]
            
            self.all_plans = all_plans
            self.logger.info(f"총 {len(all_plans)}개 요금제 수집 완료")
            
        finally:
            driver.quit()
    
    def handle_alert(self, driver):
        """Alert 처리"""
        try:
            from selenium.common.exceptions import NoAlertPresentException
            alert = driver.switch_to.alert
            alert.accept()
            time.sleep(0.5)
            return True
        except NoAlertPresentException:
            return False
    
    def _close_popups(self, driver):
        """팝업 닫기"""
        driver.execute_script("""
            document.querySelectorAll('.close, [class*="close"]').forEach(btn => {
                if (btn.offsetParent !== null) {
                    try { btn.click(); } catch(e) {}
                }
            });
        """)
    
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
            self.logger.error(f"모달 열기 실패: {e}")
        
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
            self.logger.info(f"{len(plans)}개 {plan_type} 요금제 발견")
            
        except Exception as e:
            self.logger.error(f"{plan_type} 요금제 수집 오류: {e}")
        
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
            self.logger.error(f"LTE 탭 전환 실패: {e}")
        
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
                
                const modal = document.querySelector('#selectPaymentPop, .layerWrap');
                if (modal) modal.style.display = 'none';
                const dimmed = document.querySelector('.dimmed, .layer_dimmed');
                if (dimmed) dimmed.style.display = 'none';
            """)
            time.sleep(0.5)
        except:
            pass
    
    def process_plan(self, plan_index):
        """단일 요금제 처리"""
        plan = self.all_plans[plan_index]
        driver = None
        
        try:
            driver = self.create_driver()
            
            # 페이지 로드
            driver.get(self.base_url)
            time.sleep(3)
            self.handle_alert(driver)
            self._close_popups(driver)
            
            # 모달 열기
            if not self._open_plan_modal(driver):
                raise Exception("모달 열기 실패")
            
            # 요금제 선택
            success = self._select_plan(driver, plan)
            if not success:
                raise Exception("요금제 선택 실패")
            
            # 데이터 수집
            products = self._collect_products(driver, plan)
            
            if products:
                with self.data_lock:
                    self.data.extend(products)
                    self.total_products += len(products)
                    self.completed_count += 1
                
                self.logger.info(f"✓ {plan['name']}: {len(products)}개")
                return True
            else:
                with self.data_lock:
                    self.failed_count += 1
                return False
                
        except Exception as e:
            self.logger.error(f"처리 오류: {str(e)}")
            with self.data_lock:
                self.failed_count += 1
            return False
            
        finally:
            if driver:
                driver.quit()
    
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
                self._close_modal(driver)
            
            time.sleep(2)
            self.handle_alert(driver)
            
            return True
            
        except Exception as e:
            self.logger.error(f"요금제 선택 오류: {e}")
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
                
                # 중복 제거 및 데이터 변환
                new_products = []
                for product in products:
                    if product['device_name'] not in collected_names:
                        collected_names.add(product['device_name'])
                        
                        # 통합 형식으로 변환
                        unified_product = {
                            '통신사': 'KT',
                            '가입유형': '전체',
                            '네트워크': plan['plan_type'],
                            '요금제_카테고리': plan['plan_type'],
                            '요금제': plan['name'],
                            '월요금': plan.get('monthlyFee', 0),
                            '기기명': product['device_name'],
                            '제조사': product['manufacturer'],
                            '출고가': product['release_price'],
                            '공시지원금': product['public_support_fee'],
                            '추가지원금': product['additional_support_fee'],
                            '총지원금': product['public_support_fee'] + product['additional_support_fee'],
                            '공시일자': datetime.now().strftime('%Y-%m-%d'),
                            '크롤링시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        new_products.append(unified_product)
                
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
                    
                    return false;
                """)
                
                if not next_clicked:
                    break
                
                page += 1
                time.sleep(1)
                
            except Exception as e:
                self.logger.debug(f"페이지 {page} 수집 오류: {e}")
                break
        
        return all_products
    
    def run_parallel_crawling(self):
        """병렬 크롤링 실행"""
        self.logger.info("KT 병렬 크롤링 시작...")
        
        with ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            futures = []
            for i in range(len(self.all_plans)):
                future = executor.submit(self.process_plan, i)
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Future 오류: {str(e)}")
        
        self.logger.info(f"KT 크롤링 완료: {len(self.data)}개 데이터 수집")
    
    def crawl(self):
        """KT 크롤링 실행"""
        try:
            # 1. 요금제 수집
            self.collect_all_plans()
            
            if not self.all_plans:
                self.logger.error("수집된 요금제가 없습니다.")
                return []
            
            # 2. 병렬 크롤링
            self.run_parallel_crawling()
            
            return self.data
            
        except Exception as e:
            self.logger.error(f"KT 크롤링 오류: {e}")
            traceback.print_exc()
            return self.data


class LGCrawler:
    """LG U+ 크롤러 (v3.9 기반)"""
    
    def __init__(self, config=None):
        self.logger = loggers['lg']
        self.base_url = "https://www.lguplus.com/mobile/financing-model"
        self.driver = None
        self.data = []
        self.wait = None
        
        # 기본 설정
        self.config = {
            'headless': True,
            'page_load_timeout': 45,
            'element_wait_timeout': 20,
            'table_wait_timeout': 40,
            'retry_count': 3,
            'delay_between_actions': 2,
            'max_rate_plans': 0,
            'max_pages': 20,
            'show_browser': False
        }
        
        if config:
            self.config.update(config)
        
        self.rate_plan_price_cache = {}
        self.all_rate_plans = defaultdict(dict)
        self.total_tasks = 0
        self.completed_tasks = 0
        
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        chrome_options = Options()
        
        if self.config.get('headless'):
            chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--window-size=1920,1080')
        
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # 성능 최적화
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--enable-javascript')
        chrome_options.add_argument('--allow-running-insecure-content')
        
        # User-Agent 설정
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.set_page_load_timeout(self.config['page_load_timeout'])
        
        if not self.config.get('headless'):
            self.driver.maximize_window()
        
        # JavaScript로 헤드리스 감지 방지
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            '''
        })
        
        self.wait = WebDriverWait(self.driver, self.config['element_wait_timeout'])
        self.logger.info("Chrome 드라이버 설정 완료")
        
    def get_wait_time(self, base_time: float) -> float:
        """대기 시간 조정"""
        if self.config.get('headless'):
            return base_time * 1.5
        return base_time
        
    def wait_for_page_ready(self, timeout: int = 10):
        """페이지 로딩 대기"""
        try:
            wait_timeout = self.get_wait_time(timeout)
            
            WebDriverWait(self.driver, wait_timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            
            time.sleep(self.get_wait_time(0.5))
            
        except Exception as e:
            self.logger.debug(f"페이지 대기 중 타임아웃: {e}")
            
    def check_and_handle_modal(self, max_attempts=3) -> bool:
        """모달 확인 및 처리"""
        for attempt in range(max_attempts):
            try:
                time.sleep(self.get_wait_time(0.3))
                
                modal_handled = self.driver.execute_script("""
                    var modals = document.querySelectorAll('div.modal-content');
                    var handled = false;
                    
                    for (var i = 0; i < modals.length; i++) {
                        var modal = modals[i];
                        if (modal && window.getComputedStyle(modal).display !== 'none') {
                            var confirmBtns = modal.querySelectorAll('button.c-btn-solid-1-m');
                            for (var j = 0; j < confirmBtns.length; j++) {
                                if (confirmBtns[j].offsetParent !== null) {
                                    confirmBtns[j].click();
                                    handled = true;
                                    break;
                                }
                            }
                            
                            if (!handled) {
                                var closeBtns = modal.querySelectorAll('button.c-btn-close');
                                for (var k = 0; k < closeBtns.length; k++) {
                                    if (closeBtns[k].offsetParent !== null) {
                                        closeBtns[k].click();
                                        handled = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    
                    return handled;
                """)
                
                if modal_handled:
                    self.logger.debug("모달 처리 완료")
                    time.sleep(self.get_wait_time(0.5))
                    return True
                    
            except Exception as e:
                self.logger.debug(f"모달 처리 시도 {attempt + 1}/{max_attempts} 실패: {e}")
                
            time.sleep(self.get_wait_time(0.2))
            
        return False
            
    def select_option(self, name: str, value: str) -> bool:
        """라디오 버튼 선택"""
        max_retries = 3
        
        for retry in range(max_retries):
            try:
                self.check_and_handle_modal()
                
                success = self.driver.execute_script("""
                    var name = arguments[0];
                    var value = arguments[1];
                    var radio = null;
                    
                    if (name === "가입유형" || name === "기기종류") {
                        radio = document.querySelector('input[name="' + name + '"][id="' + value + '"]');
                    } else {
                        radio = document.querySelector('input[name="' + name + '"][value="' + value + '"]');
                    }
                    
                    if (radio) {
                        if (!radio.checked) {
                            radio.checked = true;
                            var event = new Event('change', { bubbles: true });
                            radio.dispatchEvent(event);
                            
                            var label = document.querySelector('label[for="' + radio.id + '"]');
                            if (label) {
                                label.click();
                            }
                        }
                        return true;
                    }
                    return false;
                """, name, value)
                
                if success:
                    time.sleep(self.get_wait_time(self.config['delay_between_actions']))
                    self.check_and_handle_modal()
                    self.logger.info(f"{name} 선택: {value}")
                    return True
                    
            except Exception as e:
                if retry < max_retries - 1:
                    self.logger.debug(f"{name} 선택 재시도 ({retry + 1}/{max_retries})")
                    time.sleep(self.get_wait_time(1))
                    continue
                else:
                    self.logger.error(f"옵션 선택 실패 ({name}, {value}): {e}")
                    return False
                    
    def select_all_manufacturers(self) -> bool:
        """제조사 전체 선택"""
        try:
            self.check_and_handle_modal()
            
            success = self.driver.execute_script("""
                var allCheckbox = document.querySelector('input[id="전체"]');
                if (allCheckbox) {
                    if (!allCheckbox.checked) {
                        allCheckbox.checked = true;
                        var event = new Event('change', { bubbles: true });
                        allCheckbox.dispatchEvent(event);
                        
                        var label = document.querySelector('label[for="전체"]');
                        if (label) {
                            label.click();
                        }
                    }
                    return true;
                }
                return false;
            """)
            
            if success:
                time.sleep(self.get_wait_time(self.config['delay_between_actions']))
                self.logger.info("제조사 '전체' 선택 완료")
                self.check_and_handle_modal()
                return True
            else:
                self.logger.error("전체 체크박스를 찾을 수 없습니다")
                return False
                
        except Exception as e:
            self.logger.error(f"제조사 전체 선택 오류: {e}")
            return False
            
    def wait_for_table_ready(self) -> bool:
        """테이블 준비 대기"""
        try:
            self.check_and_handle_modal()
            self.wait_for_page_ready(15)
            
            table_found = self.driver.execute_script("""
                var tables = document.querySelectorAll('table');
                for (var i = 0; i < tables.length; i++) {
                    var rows = tables[i].querySelectorAll('tbody tr');
                    if (rows.length > 0) {
                        return true;
                    }
                }
                return false;
            """)
            
            if table_found:
                time.sleep(self.get_wait_time(1))
                
                row_count = self.driver.execute_script("""
                    var maxRows = 0;
                    var tables = document.querySelectorAll('table');
                    for (var i = 0; i < tables.length; i++) {
                        var rows = tables[i].querySelectorAll('tbody tr');
                        if (rows.length > maxRows) {
                            maxRows = rows.length;
                        }
                    }
                    return maxRows;
                """)
                
                self.logger.info(f"테이블 발견: {row_count}개 행")
                return row_count > 0
            else:
                self.logger.error("테이블을 찾을 수 없습니다")
                return False
                
        except Exception as e:
            self.logger.error(f"테이블 대기 중 오류: {e}")
            return False
    
    def extract_table_data(self, subscription_type: str, device_type: str, rate_plan_name: str = "전체", 
                          rate_plan_id: str = None, monthly_price: str = "0") -> int:
        """테이블 데이터 추출"""
        extracted_count = 0
        
        try:
            if not self.wait_for_table_ready():
                return 0
                
            # JavaScript로 데이터 추출
            extracted_data = self.driver.execute_script("""
                var data = [];
                var tables = document.querySelectorAll('table');
                
                for (var t = 0; t < tables.length; t++) {
                    var rows = tables[t].querySelectorAll('tbody tr');
                    var currentDevice = null;
                    var currentPrice = null;
                    var currentDate = null;
                    
                    for (var i = 0; i < rows.length; i++) {
                        var cells = rows[i].querySelectorAll('td');
                        if (cells.length === 0) continue;
                        
                        var rowData = {};
                        
                        if (cells.length >= 9 && cells[0].getAttribute('rowspan')) {
                            var deviceLink = cells[0].querySelector('a.link');
                            if (deviceLink) {
                                var deviceName = deviceLink.querySelector('span.tit');
                                var modelCode = deviceLink.querySelector('span.txt');
                                currentDevice = (deviceName ? deviceName.textContent.trim() : '') + 
                                              ' (' + (modelCode ? modelCode.textContent.trim() : '') + ')';
                            }
                            
                            currentPrice = cells[1].textContent.trim().replace(/[원,]/g, '');
                            currentDate = cells[2].textContent.trim();
                            
                            rowData.planDuration = cells[3].textContent.trim();
                            rowData.subsidy = cells[4].textContent.trim().replace(/[원,]/g, '');
                            rowData.additionalSubsidy = cells[5].textContent.trim().replace(/[원,]/g, '');
                            rowData.totalSubsidy = cells[6].textContent.trim().replace(/[원,]/g, '');
                            
                            var recommendedElem = cells[7].querySelector('p.fw-b');
                            rowData.recommendedDiscount = recommendedElem ? 
                                recommendedElem.textContent.trim().replace(/[원,]/g, '') : '0';
                            
                            rowData.finalPrice = cells[8].textContent.trim().replace(/[원,]/g, '');
                            
                        } else if (cells.length >= 6) {
                            rowData.planDuration = cells[0].textContent.trim();
                            rowData.subsidy = cells[1].textContent.trim().replace(/[원,]/g, '');
                            rowData.additionalSubsidy = cells[2].textContent.trim().replace(/[원,]/g, '');
                            rowData.totalSubsidy = cells[3].textContent.trim().replace(/[원,]/g, '');
                            
                            var recommendedElem = cells[4].querySelector('p.fw-b');
                            rowData.recommendedDiscount = recommendedElem ? 
                                recommendedElem.textContent.trim().replace(/[원,]/g, '') : '0';
                            
                            rowData.finalPrice = cells[5].textContent.trim().replace(/[원,]/g, '');
                        } else {
                            continue;
                        }
                        
                        if (currentDevice && rowData.subsidy && rowData.finalPrice) {
                            rowData.device = currentDevice;
                            rowData.price = currentPrice;
                            rowData.date = currentDate;
                            data.push(rowData);
                        }
                    }
                }
                
                return data;
            """)
            
            # 추출된 데이터를 통합 형식으로 변환
            for item in extracted_data:
                # 제조사 추출
                device_name = item['device']
                manufacturer = '기타'
                if '갤럭시' in device_name.lower() or 'galaxy' in device_name.lower():
                    manufacturer = '삼성'
                elif '아이폰' in device_name.lower() or 'iphone' in device_name.lower():
                    manufacturer = '애플'
                elif 'lg' in device_name.lower():
                    manufacturer = 'LG'
                
                unified_data = {
                    '통신사': 'LG U+',
                    '가입유형': subscription_type,
                    '네트워크': device_type,
                    '요금제_카테고리': device_type,
                    '요금제': rate_plan_name,
                    '월요금': int(monthly_price) if monthly_price != "0" else 0,
                    '기기명': item['device'],
                    '제조사': manufacturer,
                    '출고가': int(item['price']) if item['price'] else 0,
                    '공시지원금': int(item['subsidy']) if item['subsidy'] else 0,
                    '추가지원금': int(item['additionalSubsidy']) if item['additionalSubsidy'] else 0,
                    '총지원금': int(item['totalSubsidy']) if item['totalSubsidy'] else 0,
                    '공시일자': item['date'],
                    '크롤링시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    '요금제유지기간': item['planDuration'],
                    '추천할인': int(item['recommendedDiscount']) if item['recommendedDiscount'] else 0,
                    '최종구매가': int(item['finalPrice']) if item['finalPrice'] else 0
                }
                
                self.data.append(unified_data)
                extracted_count += 1
                
            self.logger.info(f"페이지에서 {extracted_count}개 데이터 추출")
            return extracted_count
            
        except Exception as e:
            self.logger.error(f"테이블 데이터 추출 오류: {e}")
            return 0
            
    def handle_pagination(self, subscription_type: str, device_type: str, rate_plan_name: str = "전체", 
                         rate_plan_id: str = None, monthly_price: str = "0") -> int:
        """페이지네이션 처리"""
        page = 1
        total_extracted = 0
        max_pages = self.config.get('max_pages', 20)
        consecutive_failures = 0
        
        self.logger.info(f"페이지네이션 시작 (최대 {max_pages}페이지)")
        
        while page <= max_pages:
            try:
                self.logger.info(f"페이지 {page}/{max_pages} 크롤링 중...")
                
                # 현재 페이지 데이터 추출
                extracted = self.extract_table_data(subscription_type, device_type, rate_plan_name, 
                                                  rate_plan_id, monthly_price)
                total_extracted += extracted
                
                if extracted == 0:
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        self.logger.warning("연속 3회 데이터 추출 실패. 페이지네이션 종료")
                        break
                else:
                    consecutive_failures = 0
                    
                if page >= max_pages:
                    self.logger.info(f"최대 페이지({max_pages}) 도달. 페이지네이션 종료")
                    break
                    
                # 다음 페이지 확인
                has_next = self.driver.execute_script("""
                    var pagination = document.querySelector('ul.pagination, div.pagination, nav[aria-label="pagination"]');
                    if (!pagination) return false;
                    
                    var buttons = pagination.querySelectorAll('li');
                    var currentIndex = -1;
                    
                    for (var i = 0; i < buttons.length; i++) {
                        if (buttons[i].classList.contains('active') || 
                            buttons[i].classList.contains('current')) {
                            currentIndex = i;
                            break;
                        }
                    }
                    
                    if (currentIndex >= 0 && currentIndex + 1 < buttons.length) {
                        var nextButton = buttons[currentIndex + 1];
                        if (!nextButton.classList.contains('disabled')) {
                            var clickTarget = nextButton.querySelector('button, a');
                            if (clickTarget) {
                                clickTarget.click();
                                return true;
                            }
                        }
                    }
                    
                    return false;
                """)
                
                if has_next:
                    self.logger.info(f"다음 페이지로 이동 (페이지 {page + 1})")
                    time.sleep(self.get_wait_time(3))
                    self.wait_for_page_ready()
                    page += 1
                else:
                    self.logger.info(f"마지막 페이지 도달 (페이지 {page})")
                    break
                    
            except Exception as e:
                self.logger.error(f"페이지 {page} 처리 중 오류: {e}")
                break
                
        self.logger.info(f"총 {page}개 페이지에서 {total_extracted}개 데이터 수집")
        return total_extracted
    
    def open_rate_plan_modal(self) -> bool:
        """요금제 선택 모달 열기"""
        try:
            self.check_and_handle_modal()
            
            success = self.driver.execute_script("""
                var buttons = document.querySelectorAll('button');
                for (var i = 0; i < buttons.length; i++) {
                    var text = buttons[i].textContent;
                    if (text.includes('더 많은 요금제') || 
                        text.includes('요금제 보기') || 
                        buttons[i].classList.contains('c-btn-rect-2')) {
                        buttons[i].click();
                        return true;
                    }
                }
                return false;
            """)
            
            if not success:
                self.logger.error("요금제 선택 버튼을 찾을 수 없습니다")
                return False
                
            time.sleep(self.get_wait_time(2))
            
            modal_opened = self.driver.execute_script("""
                var modal = document.querySelector('div.modal-content');
                return modal && window.getComputedStyle(modal).display !== 'none';
            """)
            
            if modal_opened:
                self.logger.info("요금제 선택 모달 열기 성공")
                return True
            else:
                self.logger.error("모달이 열리지 않았습니다")
                return False
                
        except Exception as e:
            self.logger.error(f"요금제 모달 열기 실패: {e}")
            return False
            
    def get_all_rate_plans(self) -> List[Dict]:
        """모달에서 모든 요금제 추출"""
        try:
            rate_plans = self.driver.execute_script("""
                var plans = [];
                var sections = document.querySelectorAll('div.c-section');
                
                for (var i = 0; i < sections.length; i++) {
                    var section = sections[i];
                    var sectionTitle = '';
                    var h2 = section.querySelector('h2');
                    if (h2) sectionTitle = h2.textContent.trim();
                    
                    var radios = section.querySelectorAll('input[type="radio"]');
                    for (var j = 0; j < radios.length; j++) {
                        var radio = radios[j];
                        var planId = radio.id;
                        var planValue = radio.value;
                        var label = document.querySelector('label[for="' + planId + '"]');
                        var planName = label ? label.textContent.trim() : '';
                        
                        if (planName) {
                            plans.push({
                                id: planId,
                                value: planValue,
                                name: planName,
                                section: sectionTitle
                            });
                        }
                    }
                }
                
                return plans;
            """)
            
            self.logger.info(f"총 {len(rate_plans)}개의 요금제 발견")
            return rate_plans
            
        except Exception as e:
            self.logger.error(f"요금제 목록 추출 오류: {e}")
            return []
    
    def collect_all_rate_plans(self):
        """모든 조합의 요금제 리스트 수집"""
        self.logger.info("LG U+ 전체 요금제 리스트 수집 중...")
        
        subscription_types = [
            ('1', '기기변경'),
            ('2', '번호이동'),
            ('3', '신규가입')
        ]
        
        device_types = [
            ('00', '5G폰'),
            ('01', 'LTE폰')
        ]
        
        for dev_value, dev_name in device_types:
            self.all_rate_plans[dev_value] = {}
            
            for sub_value, sub_name in subscription_types:
                try:
                    # 페이지 로드
                    self.driver.get(self.base_url)
                    self.wait_for_page_ready()
                    time.sleep(self.get_wait_time(1))
                    
                    # 옵션 선택
                    if not self.select_option('가입유형', sub_value):
                        self.logger.error(f"가입유형 선택 실패: {sub_name}")
                        continue
                        
                    if not self.select_option('기기종류', dev_value):
                        self.logger.error(f"기기종류 선택 실패: {dev_name}")
                        continue
                    
                    # 요금제 모달 열기
                    if self.open_rate_plan_modal():
                        # 요금제 목록 추출
                        rate_plans = self.get_all_rate_plans()
                        
                        # 요금제 개수 제한
                        if self.config['max_rate_plans'] > 0:
                            rate_plans = rate_plans[:self.config['max_rate_plans']]
                            
                        self.all_rate_plans[dev_value][sub_value] = rate_plans
                        self.logger.debug(f"{sub_name} - {dev_name}: {len(rate_plans)}개 요금제 수집")
                        
                        # 모달 닫기
                        self.driver.execute_script("""
                            var closeBtn = document.querySelector('button.c-btn-close');
                            if (closeBtn) closeBtn.click();
                        """)
                        time.sleep(self.get_wait_time(0.5))
                    else:
                        self.logger.warning(f"{sub_name} - {dev_name}: 요금제 모달 열기 실패")
                        self.all_rate_plans[dev_value][sub_value] = []
                        
                except Exception as e:
                    self.logger.error(f"요금제 수집 오류 ({sub_name}, {dev_name}): {e}")
                    self.all_rate_plans[dev_value][sub_value] = []
        
        # 전체 작업 수 계산
        self.total_tasks = sum(
            len(self.all_rate_plans[dev_value][sub_value]) if self.all_rate_plans[dev_value][sub_value] else 1
            for dev_value, dev_name in device_types
            for sub_value, sub_name in subscription_types
        )
        
        self.logger.info(f"요금제 수집 완료! 총 {self.total_tasks}개 작업 예정")
    
    def crawl_all_combinations(self):
        """모든 조합 크롤링"""
        subscription_types = [
            ('1', '기기변경'),
            ('2', '번호이동'),
            ('3', '신규가입')
        ]
        
        device_types = [
            ('00', '5G폰'),
            ('01', 'LTE폰')
        ]
        
        # 전체 요금제 리스트 사전 수집
        self.collect_all_rate_plans()
        
        self.logger.info("LG U+ 요금제별 상세 크롤링 시작")
        self._crawl_with_rate_plans(subscription_types, device_types)
    
    def _crawl_with_rate_plans(self, subscription_types, device_types):
        """요금제별 상세 크롤링"""
        
        for sub_value, sub_name in subscription_types:
            for dev_value, dev_name in device_types:
                # 해당 조합의 요금제 가져오기
                rate_plans = self.all_rate_plans.get(dev_value, {}).get(sub_value, [])
                
                if not rate_plans:
                    self.logger.warning(f"{sub_name} - {dev_name}: 요금제가 없습니다")
                    continue
                
                self.logger.info(f"{sub_name} - {dev_name} ({len(rate_plans)}개 요금제)")
                
                # 각 요금제별로 크롤링
                for i, rate_plan in enumerate(rate_plans):
                    self.logger.info(f"요금제 ({i+1}/{len(rate_plans)}): {rate_plan['name']}")
                    
                    try:
                        # 페이지 새로고침
                        self.driver.get(self.base_url)
                        self.wait_for_page_ready()
                        time.sleep(self.get_wait_time(3))
                        
                        # 옵션 재선택
                        self.select_option('가입유형', sub_value)
                        self.select_option('기기종류', dev_value)
                        
                        # 요금제 선택
                        if self.open_rate_plan_modal():
                            # JavaScript로 요금제 선택
                            selected = self.driver.execute_script("""
                                var radio = document.querySelector('input[id="' + arguments[0] + '"]');
                                if (radio && !radio.checked) {
                                    radio.checked = true;
                                    var event = new Event('change', { bubbles: true });
                                    radio.dispatchEvent(event);
                                    
                                    var label = document.querySelector('label[for="' + arguments[0] + '"]');
                                    if (label) label.click();
                                    
                                    return true;
                                }
                                return false;
                            """, rate_plan["id"])
                            
                            if not selected:
                                self.logger.error(f"요금제 선택 실패: {rate_plan['name']}")
                                continue
                            
                            time.sleep(self.get_wait_time(1))
                            
                            # 적용 버튼 클릭
                            applied = self.driver.execute_script("""
                                var applyBtn = document.querySelector('button.c-btn-solid-1-m');
                                if (applyBtn) {
                                    applyBtn.click();
                                    return true;
                                }
                                return false;
                            """)
                            
                            if not applied:
                                self.logger.error("적용 버튼을 찾을 수 없습니다")
                                continue
                                
                            time.sleep(self.get_wait_time(3))
                            
                        # 제조사 전체 선택
                        if not self.select_all_manufacturers():
                            self.logger.error("제조사 전체 선택 실패")
                            continue
                        
                        # 데이터 로딩 대기
                        time.sleep(self.get_wait_time(3))
                        
                        # 데이터 추출
                        extracted = self.handle_pagination(sub_name, dev_name, rate_plan['name'], 
                                                         rate_plan.get('value'), "0")
                        
                        if extracted > 0:
                            self.logger.info(f"✓ {rate_plan['name']}: {extracted}개 데이터 수집 성공")
                        else:
                            self.logger.warning(f"데이터 추출 실패: {rate_plan['name']}")
                            
                    except Exception as e:
                        self.logger.error(f"요금제별 크롤링 오류: {e}")
    
    def crawl(self):
        """LG U+ 크롤링 실행"""
        try:
            # 드라이버 설정
            self.setup_driver()
            
            # 초기 페이지 로드
            self.logger.info(f"페이지 로딩: {self.base_url}")
            self.driver.get(self.base_url)
            self.wait_for_page_ready()
            time.sleep(self.get_wait_time(3))
            
            # 크롤링 실행
            self.crawl_all_combinations()
            
            self.logger.info(f"LG U+ 크롤링 완료: {len(self.data)}개 데이터 수집")
            return self.data
            
        except Exception as e:
            self.logger.error(f"LG U+ 크롤링 오류: {e}")
            traceback.print_exc()
            return self.data
            
        finally:
            if self.driver:
                self.driver.quit()
                self.logger.info("드라이버 종료")


class UnifiedTelecomCrawler:
    """한국 통신사 3사 통합 크롤러"""
    
    def __init__(self, config=None):
        self.logger = logger
        self.config = {
            'headless': True,
            'test_mode': False,
            'save_formats': ['excel', 'csv', 'json'],
            'output_dir': 'data',
            'checkpoint_dir': 'checkpoints',
            'enable_skt': True,
            'enable_kt': True,
            'enable_lg': True,
            'skt_max_workers': 5,
            'kt_max_workers': 3,
            'skt_max_rate_plans': 0,  # 0 = 전체
            'kt_max_rate_plans': 0,
            'lg_max_rate_plans': 0,
            'lg_max_pages': 20,
            'show_browser': False,
            'debug_mode': False,
            'validate_data': True
        }
        
        if config:
            self.config.update(config)
        
        # 디렉토리 생성
        os.makedirs(self.config['output_dir'], exist_ok=True)
        os.makedirs(self.config['checkpoint_dir'], exist_ok=True)
        
        # 데이터 저장소
        self.all_data = []
        self.data_by_carrier = {
            'SKT': [],
            'KT': [],
            'LG U+': []
        }
        
        # 통계
        self.statistics = {
            'start_time': None,
            'end_time': None,
            'total_data': 0,
            'valid_data': 0,
            'invalid_data': 0,
            'carrier_stats': {},
            'validation_result': None
        }
        
        # 검증기
        self.validator = DataValidator()
        
        # 체크포인트 파일
        self.checkpoint_file = os.path.join(
            self.config['checkpoint_dir'], 
            'unified_checkpoint.pkl'
        )
    
    def run_test_mode(self):
        """빠른 테스트 모드 실행"""
        if RICH_AVAILABLE:
            console.print(Panel.fit(
                "[bold yellow]테스트 모드 실행[/bold yellow]\n"
                "각 통신사별로 최소 데이터만 수집합니다.",
                border_style="yellow"
            ))
        else:
            print("\n" + "="*60)
            print("테스트 모드 실행")
            print("각 통신사별로 최소 데이터만 수집합니다.")
            print("="*60)
        
        # 테스트 설정
        test_config = self.config.copy()
        test_config.update({
            'test_mode': True,
            'skt_max_rate_plans': 2,
            'kt_max_rate_plans': 2,
            'lg_max_rate_plans': 1,
            'lg_max_pages': 1,
            'skt_max_workers': 2,
            'kt_max_workers': 1
        })
        
        results = {}
        
        # SKT 테스트
        if self.config.get('enable_skt', True):
            try:
                if RICH_AVAILABLE:
                    console.print("\n[cyan]SKT 테스트 크롤링...[/cyan]")
                else:
                    print("\nSKT 테스트 크롤링...")
                
                skt_crawler = SKTCrawler(test_config)
                skt_data = skt_crawler.crawl()
                results['SKT'] = {
                    'success': len(skt_data) > 0,
                    'data_count': len(skt_data),
                    'sample': skt_data[0] if skt_data else None
                }
            except Exception as e:
                results['SKT'] = {
                    'success': False,
                    'error': str(e)
                }
        
        # KT 테스트
        if self.config.get('enable_kt', True):
            try:
                if RICH_AVAILABLE:
                    console.print("\n[magenta]KT 테스트 크롤링...[/magenta]")
                else:
                    print("\nKT 테스트 크롤링...")
                
                kt_crawler = KTCrawler(test_config)
                kt_data = kt_crawler.crawl()
                results['KT'] = {
                    'success': len(kt_data) > 0,
                    'data_count': len(kt_data),
                    'sample': kt_data[0] if kt_data else None
                }
            except Exception as e:
                results['KT'] = {
                    'success': False,
                    'error': str(e)
                }
        
        # LG U+ 테스트
        if self.config.get('enable_lg', True):
            try:
                if RICH_AVAILABLE:
                    console.print("\n[green]LG U+ 테스트 크롤링...[/green]")
                else:
                    print("\nLG U+ 테스트 크롤링...")
                
                lg_crawler = LGCrawler(test_config)
                lg_data = lg_crawler.crawl()
                results['LG U+'] = {
                    'success': len(lg_data) > 0,
                    'data_count': len(lg_data),
                    'sample': lg_data[0] if lg_data else None
                }
            except Exception as e:
                results['LG U+'] = {
                    'success': False,
                    'error': str(e)
                }
        
        # 결과 출력
        self._print_test_results(results)
        
        return results
    
    def _print_test_results(self, results):
        """테스트 결과 출력"""
        if RICH_AVAILABLE:
            # 결과 테이블
            table = Table(title="테스트 결과", show_header=True)
            table.add_column("통신사", style="cyan", width=10)
            table.add_column("상태", style="bold", width=10)
            table.add_column("데이터 수", justify="right", style="yellow")
            table.add_column("비고", style="dim")
            
            for carrier, result in results.items():
                if result['success']:
                    status = "[green]성공[/green]"
                    data_count = str(result['data_count'])
                    note = "정상 작동"
                else:
                    status = "[red]실패[/red]"
                    data_count = "0"
                    note = result.get('error', '알 수 없는 오류')[:50]
                
                table.add_row(carrier, status, data_count, note)
            
            console.print("\n")
            console.print(table)
            
            # 샘플 데이터 출력
            for carrier, result in results.items():
                if result['success'] and result.get('sample'):
                    console.print(f"\n[bold]{carrier} 샘플 데이터:[/bold]")
                    sample = result['sample']
                    for key, value in list(sample.items())[:5]:
                        console.print(f"  {key}: {value}")
        else:
            print("\n테스트 결과:")
            print("-" * 60)
            for carrier, result in results.items():
                if result['success']:
                    print(f"{carrier}: 성공 (데이터 {result['data_count']}개)")
                else:
                    print(f"{carrier}: 실패 - {result.get('error', '알 수 없는 오류')}")
    
    def save_checkpoint(self, carrier: str, data: List[dict]):
        """체크포인트 저장"""
        try:
            checkpoint_data = {
                'carrier': carrier,
                'data': data,
                'timestamp': datetime.now().isoformat(),
                'statistics': self.statistics
            }
            
            with open(self.checkpoint_file, 'wb') as f:
                pickle.dump(checkpoint_data, f)
            
            self.logger.debug(f"체크포인트 저장: {carrier}")
        except Exception as e:
            self.logger.error(f"체크포인트 저장 실패: {e}")
    
    def crawl_all_carriers(self):
        """모든 통신사 크롤링"""
        self.statistics['start_time'] = datetime.now()
        
        if RICH_AVAILABLE:
            # 전체 진행상황 표시
            layout = Layout()
            layout.split_column(
                Layout(name="header", size=3),
                Layout(name="progress"),
                Layout(name="status", size=10)
            )
            
            # 헤더
            header_text = "[bold cyan]한국 통신사 3사 통합 크롤링[/bold cyan]"
            layout["header"].update(Panel(header_text, border_style="cyan"))
            
            # 상태 테이블
            status_table = Table(show_header=True, header_style="bold magenta")
            status_table.add_column("통신사", style="cyan", width=12)
            status_table.add_column("상태", style="yellow", width=15)
            status_table.add_column("수집 데이터", justify="right", style="green")
            status_table.add_column("소요 시간", justify="right")
            
            # Progress 관리
            progress_group = []
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeRemainingColumn(),
                console=console
            ) as progress:
                
                # 전체 진행률
                total_task = progress.add_task(
                    "[green]전체 진행률", 
                    total=3 if all([
                        self.config.get('enable_skt'),
                        self.config.get('enable_kt'),
                        self.config.get('enable_lg')
                    ]) else sum([
                        self.config.get('enable_skt', True),
                        self.config.get('enable_kt', True),
                        self.config.get('enable_lg', True)
                    ])
                )
                
                # SKT 크롤링
                if self.config.get('enable_skt', True):
                    start_time = time.time()
                    try:
                        console.print("\n[bold cyan]SKT 크롤링 시작...[/bold cyan]")
                        skt_crawler = SKTCrawler({
                            'headless': self.config['headless'],
                            'max_workers': self.config['skt_max_workers'],
                            'max_rate_plans': self.config['skt_max_rate_plans'],
                            'show_browser': self.config['show_browser']
                        })
                        
                        skt_data = skt_crawler.crawl()
                        self.data_by_carrier['SKT'] = skt_data
                        self.all_data.extend(skt_data)
                        
                        elapsed = time.time() - start_time
                        self.statistics['carrier_stats']['SKT'] = {
                            'data_count': len(skt_data),
                            'elapsed_time': elapsed,
                            'status': 'success'
                        }
                        
                        console.print(f"[green]✓ SKT 완료:[/green] {len(skt_data)}개 데이터 ({elapsed/60:.1f}분)")
                        
                    except Exception as e:
                        self.logger.error(f"SKT 크롤링 오류: {e}")
                        self.statistics['carrier_stats']['SKT'] = {
                            'data_count': 0,
                            'elapsed_time': time.time() - start_time,
                            'status': 'failed',
                            'error': str(e)
                        }
                        console.print(f"[red]✗ SKT 실패:[/red] {str(e)}")
                    
                    progress.advance(total_task)
                
                # KT 크롤링
                if self.config.get('enable_kt', True):
                    start_time = time.time()
                    try:
                        console.print("\n[bold magenta]KT 크롤링 시작...[/bold magenta]")
                        kt_crawler = KTCrawler({
                            'headless': self.config['headless'],
                            'max_workers': self.config['kt_max_workers'],
                            'max_rate_plans': self.config['kt_max_rate_plans'],
                            'show_browser': self.config['show_browser']
                        })
                        
                        kt_data = kt_crawler.crawl()
                        self.data_by_carrier['KT'] = kt_data
                        self.all_data.extend(kt_data)
                        
                        elapsed = time.time() - start_time
                        self.statistics['carrier_stats']['KT'] = {
                            'data_count': len(kt_data),
                            'elapsed_time': elapsed,
                            'status': 'success'
                        }
                        
                        console.print(f"[green]✓ KT 완료:[/green] {len(kt_data)}개 데이터 ({elapsed/60:.1f}분)")
                        
                    except Exception as e:
                        self.logger.error(f"KT 크롤링 오류: {e}")
                        self.statistics['carrier_stats']['KT'] = {
                            'data_count': 0,
                            'elapsed_time': time.time() - start_time,
                            'status': 'failed',
                            'error': str(e)
                        }
                        console.print(f"[red]✗ KT 실패:[/red] {str(e)}")
                    
                    progress.advance(total_task)
                
                # LG U+ 크롤링
                if self.config.get('enable_lg', True):
                    start_time = time.time()
                    try:
                        console.print("\n[bold green]LG U+ 크롤링 시작...[/bold green]")
                        lg_crawler = LGCrawler({
                            'headless': self.config['headless'],
                            'max_rate_plans': self.config['lg_max_rate_plans'],
                            'max_pages': self.config['lg_max_pages'],
                            'show_browser': self.config['show_browser']
                        })
                        
                        lg_data = lg_crawler.crawl()
                        self.data_by_carrier['LG U+'] = lg_data
                        self.all_data.extend(lg_data)
                        
                        elapsed = time.time() - start_time
                        self.statistics['carrier_stats']['LG U+'] = {
                            'data_count': len(lg_data),
                            'elapsed_time': elapsed,
                            'status': 'success'
                        }
                        
                        console.print(f"[green]✓ LG U+ 완료:[/green] {len(lg_data)}개 데이터 ({elapsed/60:.1f}분)")
                        
                    except Exception as e:
                        self.logger.error(f"LG U+ 크롤링 오류: {e}")
                        self.statistics['carrier_stats']['LG U+'] = {
                            'data_count': 0,
                            'elapsed_time': time.time() - start_time,
                            'status': 'failed',
                            'error': str(e)
                        }
                        console.print(f"[red]✗ LG U+ 실패:[/red] {str(e)}")
                    
                    progress.advance(total_task)
        
        else:
            # Rich 없을 때
            print("\n한국 통신사 3사 통합 크롤링 시작...")
            
            if self.config.get('enable_skt', True):
                print("\nSKT 크롤링 중...")
                try:
                    skt_crawler = SKTCrawler(self.config)
                    skt_data = skt_crawler.crawl()
                    self.data_by_carrier['SKT'] = skt_data
                    self.all_data.extend(skt_data)
                    print(f"SKT 완료: {len(skt_data)}개 데이터")
                except Exception as e:
                    print(f"SKT 실패: {e}")
            
            if self.config.get('enable_kt', True):
                print("\nKT 크롤링 중...")
                try:
                    kt_crawler = KTCrawler(self.config)
                    kt_data = kt_crawler.crawl()
                    self.data_by_carrier['KT'] = kt_data
                    self.all_data.extend(kt_data)
                    print(f"KT 완료: {len(kt_data)}개 데이터")
                except Exception as e:
                    print(f"KT 실패: {e}")
            
            if self.config.get('enable_lg', True):
                print("\nLG U+ 크롤링 중...")
                try:
                    lg_crawler = LGCrawler(self.config)
                    lg_data = lg_crawler.crawl()
                    self.data_by_carrier['LG U+'] = lg_data
                    self.all_data.extend(lg_data)
                    print(f"LG U+ 완료: {len(lg_data)}개 데이터")
                except Exception as e:
                    print(f"LG U+ 실패: {e}")
        
        self.statistics['end_time'] = datetime.now()
        self.statistics['total_data'] = len(self.all_data)
    
    def validate_and_clean_data(self):
        """데이터 검증 및 정리"""
        if not self.config.get('validate_data', True):
            self.logger.info("데이터 검증 건너뜀")
            return
        
        if RICH_AVAILABLE:
            console.print("\n[bold yellow]데이터 검증 및 정리 중...[/bold yellow]")
        else:
            print("\n데이터 검증 및 정리 중...")
        
        # 데이터 검증
        valid_data, validation_result = self.validator.validate_dataset(self.all_data)
        
        self.statistics['valid_data'] = len(valid_data)
        self.statistics['invalid_data'] = validation_result['invalid']
        self.statistics['validation_result'] = validation_result
        
        # 검증된 데이터로 교체
        self.all_data = valid_data
        
        # 통신사별 재분류
        self.data_by_carrier = {
            'SKT': [],
            'KT': [],
            'LG U+': []
        }
        
        for item in self.all_data:
            carrier = item.get('통신사', '')
            if carrier in self.data_by_carrier:
                self.data_by_carrier[carrier].append(item)
        
        # 검증 결과 출력
        if RICH_AVAILABLE:
            # 검증 결과 테이블
            table = Table(title="데이터 검증 결과", show_header=True)
            table.add_column("항목", style="cyan")
            table.add_column("수치", justify="right", style="yellow")
            
            table.add_row("전체 데이터", f"{validation_result['total']:,}")
            table.add_row("유효 데이터", f"{validation_result['valid']:,}")
            table.add_row("무효 데이터", f"{validation_result['invalid']:,}")
            table.add_row("검증률", f"{validation_result['validation_rate']:.1f}%")
            
            console.print(table)
            
            if validation_result['error_summary']:
                console.print("\n[dim]주요 오류:[/dim]")
                for error, count in validation_result['error_summary'].items():
                    console.print(f"  • {error}: {count}건")
        else:
            print(f"검증 완료: {validation_result['valid']}/{validation_result['total']} "
                  f"({validation_result['validation_rate']:.1f}% 유효)")
    
    def save_results(self):
        """결과 저장"""
        if not self.all_data:
            self.logger.warning("저장할 데이터가 없습니다.")
            return []
        
        # DataFrame 생성
        df = pd.DataFrame(self.all_data)
        
        # 타임스탬프
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        saved_files = []
        
        # Excel 저장
        if 'excel' in self.config['save_formats']:
            excel_file = os.path.join(
                self.config['output_dir'], 
                f'통신3사_공시지원금_통합_{timestamp}.xlsx'
            )
            
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                # 전체 데이터
                df.to_excel(writer, sheet_name='전체데이터', index=False)
                
                # 통신사별 시트
                for carrier, data in self.data_by_carrier.items():
                    if data:
                        carrier_df = pd.DataFrame(data)
                        sheet_name = carrier.replace(' ', '_')
                        carrier_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # 요약 통계
                summary_data = []
                
                # 전체 통계
                summary_data.append({
                    '구분': '전체',
                    '항목': '전체',
                    '데이터수': len(df),
                    '기기종류': df['기기명'].nunique() if '기기명' in df.columns else 0,
                    '평균출고가': int(df['출고가'].mean()) if '출고가' in df.columns else 0,
                    '평균공시지원금': int(df['공시지원금'].mean()) if '공시지원금' in df.columns else 0,
                    '최대공시지원금': int(df['공시지원금'].max()) if '공시지원금' in df.columns else 0
                })
                
                # 통신사별 통계
                for carrier in ['SKT', 'KT', 'LG U+']:
                    carrier_df = df[df['통신사'] == carrier] if '통신사' in df.columns else pd.DataFrame()
                    if len(carrier_df) > 0:
                        summary_data.append({
                            '구분': '통신사',
                            '항목': carrier,
                            '데이터수': len(carrier_df),
                            '기기종류': carrier_df['기기명'].nunique() if '기기명' in carrier_df.columns else 0,
                            '평균출고가': int(carrier_df['출고가'].mean()) if '출고가' in carrier_df.columns else 0,
                            '평균공시지원금': int(carrier_df['공시지원금'].mean()) if '공시지원금' in carrier_df.columns else 0,
                            '최대공시지원금': int(carrier_df['공시지원금'].max()) if '공시지원금' in carrier_df.columns else 0
                        })
                
                # 가입유형별 통계
                if '가입유형' in df.columns:
                    for sub_type in df['가입유형'].unique():
                        sub_df = df[df['가입유형'] == sub_type]
                        summary_data.append({
                            '구분': '가입유형',
                            '항목': sub_type,
                            '데이터수': len(sub_df),
                            '기기종류': sub_df['기기명'].nunique() if '기기명' in sub_df.columns else 0,
                            '평균출고가': int(sub_df['출고가'].mean()) if '출고가' in sub_df.columns else 0,
                            '평균공시지원금': int(sub_df['공시지원금'].mean()) if '공시지원금' in sub_df.columns else 0,
                            '최대공시지원금': int(sub_df['공시지원금'].max()) if '공시지원금' in sub_df.columns else 0
                        })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='요약통계', index=False)
                
                # 검증 결과
                if self.statistics.get('validation_result'):
                    validation_df = pd.DataFrame([self.statistics['validation_result']])
                    validation_df.to_excel(writer, sheet_name='검증결과', index=False)
            
            saved_files.append(excel_file)
            self.logger.info(f"Excel 저장: {excel_file}")
        
        # CSV 저장
        if 'csv' in self.config['save_formats']:
            csv_file = os.path.join(
                self.config['output_dir'], 
                f'통신3사_공시지원금_통합_{timestamp}.csv'
            )
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            saved_files.append(csv_file)
            self.logger.info(f"CSV 저장: {csv_file}")
        
        # JSON 저장
        if 'json' in self.config['save_formats']:
            json_file = os.path.join(
                self.config['output_dir'], 
                f'통신3사_공시지원금_통합_{timestamp}.json'
            )
            
            json_data = {
                'metadata': {
                    'crawled_at': timestamp,
                    'total_data': len(self.all_data),
                    'statistics': self.statistics
                },
                'data': self.all_data
            }
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            saved_files.append(json_file)
            self.logger.info(f"JSON 저장: {json_file}")
        
        return saved_files
    
    def print_final_summary(self):
        """최종 요약 출력"""
        elapsed_time = (self.statistics['end_time'] - self.statistics['start_time']).total_seconds()
        
        if RICH_AVAILABLE:
            # 최종 요약 패널
            summary_text = f"""
[bold green]✅ 크롤링 완료![/bold green]

[yellow]전체 통계:[/yellow]
• 총 소요시간: {elapsed_time/60:.1f}분
• 총 데이터: {self.statistics['total_data']:,}개
• 유효 데이터: {self.statistics['valid_data']:,}개
• 검증률: {(self.statistics['valid_data']/self.statistics['total_data']*100) if self.statistics['total_data'] > 0 else 0:.1f}%

[cyan]통신사별 결과:[/cyan]"""
            
            for carrier, stats in self.statistics['carrier_stats'].items():
                if stats['status'] == 'success':
                    summary_text += f"\n• {carrier}: {stats['data_count']:,}개 ({stats['elapsed_time']/60:.1f}분)"
                else:
                    summary_text += f"\n• {carrier}: [red]실패[/red]"
            
            console.print(Panel(summary_text.strip(), title="크롤링 완료", border_style="green"))
            
            # 상세 통계 테이블
            if self.all_data:
                df = pd.DataFrame(self.all_data)
                
                # 기기별 TOP 10
                if '기기명' in df.columns:
                    device_table = Table(title="인기 기기 TOP 10", show_header=True)
                    device_table.add_column("순위", style="cyan", width=6)
                    device_table.add_column("기기명", style="yellow")
                    device_table.add_column("데이터수", justify="right", style="green")
                    
                    top_devices = df['기기명'].value_counts().head(10)
                    for i, (device, count) in enumerate(top_devices.items(), 1):
                        device_table.add_row(str(i), device[:40], str(count))
                    
                    console.print("\n")
                    console.print(device_table)
                
                # 요금제별 통계
                if '요금제' in df.columns:
                    plan_stats = df.groupby('요금제').agg({
                        '기기명': 'count',
                        '공시지원금': ['mean', 'max']
                    }).round(0)
                    
                    plan_table = Table(title="요금제별 공시지원금 TOP 10", show_header=True)
                    plan_table.add_column("요금제", style="cyan")
                    plan_table.add_column("데이터수", justify="right", style="yellow")
                    plan_table.add_column("평균지원금", justify="right", style="green")
                    plan_table.add_column("최대지원금", justify="right", style="red")
                    
                    # 평균 지원금 기준 정렬
                    plan_stats.columns = ['count', 'avg_subsidy', 'max_subsidy']
                    top_plans = plan_stats.sort_values('avg_subsidy', ascending=False).head(10)
                    
                    for plan_name, row in top_plans.iterrows():
                        plan_table.add_row(
                            str(plan_name)[:30],
                            str(int(row['count'])),
                            f"{int(row['avg_subsidy']):,}원",
                            f"{int(row['max_subsidy']):,}원"
                        )
                    
                    console.print("\n")
                    console.print(plan_table)
        
        else:
            # Rich 없을 때
            print("\n" + "="*60)
            print("크롤링 완료!")
            print("="*60)
            print(f"총 소요시간: {elapsed_time/60:.1f}분")
            print(f"총 데이터: {self.statistics['total_data']:,}개")
            print(f"유효 데이터: {self.statistics['valid_data']:,}개")
            
            print("\n통신사별 결과:")
            for carrier, stats in self.statistics['carrier_stats'].items():
                if stats['status'] == 'success':
                    print(f"  {carrier}: {stats['data_count']:,}개")
                else:
                    print(f"  {carrier}: 실패")
    
    def run(self):
        """통합 크롤러 실행"""
        try:
            if RICH_AVAILABLE:
                console.print(Panel.fit(
                    "[bold cyan]한국 통신사 3사 통합 휴대폰 지원금 크롤러 v1.0[/bold cyan]\n"
                    "[yellow]SKT, KT, LG U+ 공시지원금 데이터 통합 수집[/yellow]\n\n"
                    "[dim]• Rich UI 기반 진행상황 표시[/dim]\n"
                    "[dim]• 멀티스레딩 병렬 처리[/dim]\n"
                    "[dim]• 데이터 정합성 자동 검증[/dim]\n"
                    "[dim]• 통합 Excel/CSV/JSON 저장[/dim]",
                    border_style="cyan"
                ))
            else:
                print("\n" + "="*70)
                print("한국 통신사 3사 통합 휴대폰 지원금 크롤러 v1.0")
                print("SKT, KT, LG U+ 공시지원금 데이터 통합 수집")
                print("="*70)
            
            # 테스트 모드 확인
            if self.config.get('test_mode'):
                return self.run_test_mode()
            
            # 1. 전체 크롤링 실행
            self.crawl_all_carriers()
            
            # 2. 데이터 검증 및 정리
            if self.all_data:
                self.validate_and_clean_data()
            
            # 3. 결과 저장
            saved_files = []
            if self.all_data:
                saved_files = self.save_results()
            
            # 4. 최종 요약 출력
            self.print_final_summary()
            
            # 5. 저장된 파일 목록 출력
            if saved_files:
                if RICH_AVAILABLE:
                    console.print(f"\n[bold green]저장된 파일 ({len(saved_files)}개):[/bold green]")
                    for file in saved_files:
                        console.print(f"  📁 {file}")
                else:
                    print(f"\n저장된 파일 ({len(saved_files)}개):")
                    for file in saved_files:
                        print(f"  - {file}")
            else:
                if RICH_AVAILABLE:
                    console.print("\n[red]⚠️ 저장된 파일이 없습니다.[/red]")
                else:
                    print("\n⚠️ 저장된 파일이 없습니다.")
            
            return saved_files
            
        except KeyboardInterrupt:
            if RICH_AVAILABLE:
                console.print("\n[yellow]사용자에 의해 중단되었습니다.[/yellow]")
            else:
                print("\n사용자에 의해 중단되었습니다.")
            
            # 중간 데이터라도 저장
            if self.all_data:
                saved_files = self.save_results()
                return saved_files
            return []
            
        except Exception as e:
            if RICH_AVAILABLE:
                console.print(f"\n[red]오류 발생: {str(e)}[/red]")
            else:
                print(f"\n오류 발생: {str(e)}")
            
            if self.config.get('debug_mode'):
                traceback.print_exc()
            
            # 오류 발생 시에도 수집된 데이터 저장
            if self.all_data:
                saved_files = self.save_results()
                return saved_files
            return []


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='한국 통신사 3사 통합 휴대폰 지원금 크롤러',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예제:
  python unified_crawler.py                    # 전체 크롤링 (기본)
  python unified_crawler.py --test             # 빠른 테스트 모드
  python unified_crawler.py --carriers skt kt  # 특정 통신사만
  python unified_crawler.py --no-headless      # GUI 모드
  python unified_crawler.py --debug            # 디버그 모드

데이터는 data/ 폴더에 저장됩니다.
        """
    )
    
    # 기본 옵션
    parser.add_argument('--test', action='store_true',
                        help='빠른 테스트 모드 (각 통신사별 최소 데이터만 수집)')
    parser.add_argument('--no-headless', action='store_true',
                        help='브라우저 GUI 표시 (기본: 헤드리스)')
    parser.add_argument('--debug', action='store_true',
                        help='디버그 모드 활성화')
    
    # 통신사 선택
    parser.add_argument('--carriers', nargs='+',
                        choices=['skt', 'kt', 'lg', 'all'],
                        default=['all'],
                        help='크롤링할 통신사 선택 (기본: all)')
    
    # 크롤링 옵션
    parser.add_argument('--skt-workers', type=int, default=5,
                        help='SKT 동시 실행 워커 수 (기본: 5)')
    parser.add_argument('--kt-workers', type=int, default=3,
                        help='KT 동시 실행 워커 수 (기본: 3)')
    parser.add_argument('--max-rate-plans', type=int, default=0,
                        help='통신사별 최대 요금제 수 (0=전체, 기본: 0)')
    parser.add_argument('--lg-max-pages', type=int, default=20,
                        help='LG U+ 최대 페이지 수 (기본: 20)')
    
    # 저장 옵션
    parser.add_argument('--output', type=str, default='data',
                        help='출력 디렉토리 (기본: data)')
    parser.add_argument('--formats', nargs='+',
                        choices=['excel', 'csv', 'json'],
                        default=['excel', 'csv', 'json'],
                        help='저장 형식 (기본: excel csv json)')
    parser.add_argument('--no-validation', action='store_true',
                        help='데이터 검증 건너뛰기')
    
    args = parser.parse_args()
    
    # 로깅 재설정
    global loggers
    log_level = 'DEBUG' if args.debug else 'INFO'
    loggers = setup_logging(level=log_level)
    
    # 통신사 선택 처리
    enable_skt = 'all' in args.carriers or 'skt' in args.carriers
    enable_kt = 'all' in args.carriers or 'kt' in args.carriers
    enable_lg = 'all' in args.carriers or 'lg' in args.carriers
    
    # 크롤러 설정
    config = {
        'headless': not args.no_headless,
        'test_mode': args.test,
        'debug_mode': args.debug,
        'enable_skt': enable_skt,
        'enable_kt': enable_kt,
        'enable_lg': enable_lg,
        'skt_max_workers': args.skt_workers,
        'kt_max_workers': args.kt_workers,
        'skt_max_rate_plans': args.max_rate_plans if not args.test else 2,
        'kt_max_rate_plans': args.max_rate_plans if not args.test else 2,
        'lg_max_rate_plans': args.max_rate_plans if not args.test else 1,
        'lg_max_pages': args.lg_max_pages if not args.test else 1,
        'output_dir': args.output,
        'save_formats': args.formats,
        'validate_data': not args.no_validation,
        'show_browser': args.no_headless
    }
    
    # 선택된 통신사 출력
    selected_carriers = []
    if enable_skt:
        selected_carriers.append('SKT')
    if enable_kt:
        selected_carriers.append('KT')
    if enable_lg:
        selected_carriers.append('LG U+')
    
    if RICH_AVAILABLE:
        console.print(f"\n[cyan]선택된 통신사:[/cyan] {', '.join(selected_carriers)}")
        if args.test:
            console.print("[yellow]테스트 모드로 실행합니다.[/yellow]")
    else:
        print(f"\n선택된 통신사: {', '.join(selected_carriers)}")
        if args.test:
            print("테스트 모드로 실행합니다.")
    
    # 크롤러 실행
    crawler = UnifiedTelecomCrawler(config)
    
    try:
        saved_files = crawler.run()
        
        if saved_files:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except KeyboardInterrupt:
        if RICH_AVAILABLE:
            console.print("\n[yellow]사용자에 의해 중단되었습니다.[/yellow]")
        else:
            print("\n사용자에 의해 중단되었습니다.")
        sys.exit(1)
        
    except Exception as e:
        if RICH_AVAILABLE:
            console.print(f"\n[red]치명적 오류: {str(e)}[/red]")
        else:
            print(f"\n치명적 오류: {str(e)}")
        
        if args.debug:
            traceback.print_exc()
        
        sys.exit(1)


if __name__ == "__main__":
    # 빠른 시작을 위한 안내
    if len(sys.argv) == 1:
        if RICH_AVAILABLE:
            console.print(Panel(
                "[bold cyan]한국 통신사 3사 통합 휴대폰 지원금 크롤러 v1.0[/bold cyan]\n\n"
                "[yellow]주요 기능:[/yellow]\n"
                "• SKT, KT, LG U+ 3사 데이터 통합 수집\n"
                "• Rich UI 기반 실시간 진행상황 표시\n"
                "• 멀티스레딩 병렬 처리로 빠른 수집\n"
                "• 데이터 정합성 자동 검증\n"
                "• 통합 Excel/CSV/JSON 저장\n\n"
                "[green]사용법:[/green]\n"
                "  python unified_crawler.py              # 전체 크롤링\n"
                "  python unified_crawler.py --test       # 빠른 테스트\n"
                "  python unified_crawler.py --help       # 도움말\n\n"
                "[dim]파일은 data/ 폴더에 저장됩니다.[/dim]",
                border_style="cyan"
            ))
        else:
            print("="*60)
            print("한국 통신사 3사 통합 휴대폰 지원금 크롤러 v1.0")
            print("="*60)
            print("\n주요 기능:")
            print("• SKT, KT, LG U+ 3사 데이터 통합 수집")
            print("• 멀티스레딩 병렬 처리로 빠른 수집")
            print("• 데이터 정합성 자동 검증")
            print("• 통합 Excel/CSV/JSON 저장")
            print("\n사용법:")
            print("  python unified_crawler.py              # 전체 크롤링")
            print("  python unified_crawler.py --test       # 빠른 테스트")
            print("  python unified_crawler.py --help       # 도움말")
            print("\n파일은 data/ 폴더에 저장됩니다.")
        print()
    
    main()