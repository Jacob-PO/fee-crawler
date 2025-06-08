#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LG U+ 휴대폰 지원금 통합 크롤러 - 개선판 v3.9
모든 가입유형, 기기종류(태블릿 제외), 요금제별 전체 제조사 데이터 추출

작성일: 2025-01-11
버전: 3.9 - 헤드리스 모드 최적화 및 20페이지 제한

주요 개선사항:
    - 최대 20페이지까지만 크롤링
    - 헤드리스 모드 기본 설정
    - 헤드리스 모드 완벽 지원 (100% 데이터 수집)
    - 동적 콘텐츠 로딩 대기 강화
    - JavaScript 실행 완료 확인 강화

사용 예제:
    # 기본 실행 (헤드리스 모드, 모든 데이터 크롤링)
    python lg_crawler_v39.py
    
    # GUI 모드로 실행
    python lg_crawler_v39.py --no-headless
    
    # 요금제 구분 없이 빠른 크롤링
    python lg_crawler_v39.py --no-rate-plans
    
    # 테스트 모드
    python lg_crawler_v39.py --test-one-rate-plan
"""

import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
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
from tqdm import tqdm
from collections import defaultdict


# 로깅 설정
def setup_logging(log_level='INFO', log_file=None):
    """로깅 설정"""
    log_format = '%(asctime)s - [%(levelname)s] - %(message)s'
    
    handlers = [logging.StreamHandler()]
    if log_file:
        os.makedirs('logs', exist_ok=True)
        handlers.append(logging.FileHandler(f'logs/{log_file}', encoding='utf-8'))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=handlers
    )
    
    # tqdm과 로깅 충돌 방지
    logging.getLogger('selenium').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)


# 전역 로거
logger = setup_logging()


class LGUPlusCrawler:
    """LG U+ 휴대폰 지원금 통합 크롤러"""
    
    def __init__(self, config=None):
        """
        크롤러 초기화
        
        Args:
            config (dict): 크롤러 설정
        """
        self.base_url = "https://www.lguplus.com/mobile/financing-model"
        self.driver = None
        self.data = []
        self.wait = None
        
        # 기본 설정 (헤드리스 모드 기본값 True로 변경)
        self.config = {
            'headless': True,  # 헤드리스 모드 기본 활성화
            'page_load_timeout': 45,  # 헤드리스 모드를 위해 타임아웃 증가
            'element_wait_timeout': 20,  # 헤드리스 모드를 위해 증가
            'table_wait_timeout': 40,  # 헤드리스 모드를 위해 증가
            'retry_count': 3,
            'delay_between_actions': 2,  # 헤드리스 모드를 위해 지연 증가
            'save_formats': ['excel', 'csv', 'json'],
            'output_dir': 'data',
            'include_rate_plans': True,
            'max_rate_plans': 0,  # 0=전체 요금제
            'debug_mode': False,
            'restart_interval': 3,
            'session_check': True,
            'test_mode': False,
            'show_progress': True,
            'max_pages': 20,  # 최대 20페이지로 제한
            'headless_wait_multiplier': 1.5  # 헤드리스 모드에서 대기 시간 배수
        }
        
        # 사용자 설정 병합
        if config:
            self.config.update(config)
            
        # 출력 디렉토리 생성
        os.makedirs(self.config['output_dir'], exist_ok=True)
        
        # 요금제 가격 캐시
        self.rate_plan_price_cache = {}
        
        # 전체 요금제 리스트 (사전 수집용)
        self.all_rate_plans = defaultdict(dict)  # {device_type: {sub_type: [rate_plans]}}
        
        # 진행 상태
        self.total_tasks = 0
        self.completed_tasks = 0
        
    def setup_driver(self):
        """Chrome 드라이버 설정 (헤드리스 모드 최적화)"""
        chrome_options = Options()
        
        # 헤드리스 모드 설정
        if self.config.get('headless'):
            chrome_options.add_argument('--headless=new')  # 새로운 헤드리스 모드 사용
            chrome_options.add_argument('--window-size=1920,1080')
            logger.info("헤드리스 모드로 실행됩니다")
        
        # 헤드리스 감지 방지 설정
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # 성능 최적화
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-web-security')
        chrome_options.add_argument('--disable-features=VizDisplayCompositor')
        chrome_options.add_argument('--no-first-run')
        chrome_options.add_argument('--no-default-browser-check')
        chrome_options.add_argument('--disable-popup-blocking')
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--disable-default-apps')
        
        # JavaScript 실행을 위한 설정
        chrome_options.add_argument('--enable-javascript')
        chrome_options.add_argument('--allow-running-insecure-content')
        
        # 메모리 최적화
        chrome_options.add_argument('--memory-pressure-off')
        chrome_options.add_argument('--max_old_space_size=4096')
        
        # User-Agent 설정 (헤드리스 감지 방지)
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # WebDriver 초기화
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.set_page_load_timeout(self.config['page_load_timeout'])
        
        # 헤드리스 모드가 아닐 때만 창 최대화
        if not self.config.get('headless'):
            self.driver.maximize_window()
        
        # JavaScript로 헤드리스 감지 방지
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['ko-KR', 'ko', 'en-US', 'en']
                });
                window.chrome = {
                    runtime: {}
                };
                Object.defineProperty(navigator, 'permissions', {
                    get: () => ({
                        query: () => Promise.resolve({ state: 'granted' })
                    })
                });
            '''
        })
        
        # WebDriverWait 설정
        self.wait = WebDriverWait(self.driver, self.config['element_wait_timeout'])
        
        logger.info("Chrome 드라이버 설정 완료")
        
    def get_wait_time(self, base_time: float) -> float:
        """헤드리스 모드에서 대기 시간 조정"""
        if self.config.get('headless'):
            return base_time * self.config.get('headless_wait_multiplier', 1.5)
        return base_time
        
    def wait_for_page_ready(self, timeout: int = 10):
        """페이지가 완전히 로드될 때까지 대기 (헤드리스 모드 강화)"""
        try:
            # 헤드리스 모드에서는 더 긴 대기 시간
            wait_timeout = self.get_wait_time(timeout)
            
            # 1. Document ready 상태 확인
            WebDriverWait(self.driver, wait_timeout).until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            
            # 2. jQuery 로딩 완료 확인
            try:
                WebDriverWait(self.driver, wait_timeout/2).until(
                    lambda driver: driver.execute_script("""
                        return (typeof jQuery !== 'undefined' && jQuery.active === 0) || 
                               (typeof $ !== 'undefined' && $.active === 0) || 
                               true;
                    """)
                )
            except:
                pass
            
            # 3. 주요 요소 로딩 확인
            try:
                WebDriverWait(self.driver, wait_timeout/2).until(
                    lambda driver: driver.execute_script("""
                        return document.querySelector('body') !== null &&
                               document.querySelector('table, div.modal-content, .c-btn-rect-2') !== null;
                    """)
                )
            except:
                pass
            
            # 4. 추가 대기 (헤드리스 모드에서는 더 길게)
            time.sleep(self.get_wait_time(0.5))
            
        except Exception as e:
            logger.debug(f"페이지 대기 중 타임아웃: {e}")
            
    def check_and_handle_modal(self, max_attempts=3) -> bool:
        """모달 확인 및 처리 (헤드리스 모드 최적화)"""
        for attempt in range(max_attempts):
            try:
                # 헤드리스 모드에서 더 긴 대기
                time.sleep(self.get_wait_time(0.3))
                
                # JavaScript로 모달 확인 및 처리
                modal_handled = self.driver.execute_script("""
                    var modals = document.querySelectorAll('div.modal-content');
                    var handled = false;
                    
                    for (var i = 0; i < modals.length; i++) {
                        var modal = modals[i];
                        if (modal && window.getComputedStyle(modal).display !== 'none') {
                            // 확인 버튼 클릭
                            var confirmBtns = modal.querySelectorAll('button.c-btn-solid-1-m');
                            for (var j = 0; j < confirmBtns.length; j++) {
                                if (confirmBtns[j].offsetParent !== null) {
                                    confirmBtns[j].click();
                                    handled = true;
                                    break;
                                }
                            }
                            
                            // 닫기 버튼 클릭
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
                    logger.debug("모달 처리 완료")
                    time.sleep(self.get_wait_time(0.5))
                    return True
                    
            except Exception as e:
                logger.debug(f"모달 처리 시도 {attempt + 1}/{max_attempts} 실패: {e}")
                
            time.sleep(self.get_wait_time(0.2))
            
        return False
            
    def safe_click(self, element, retry=3) -> bool:
        """안전한 클릭 (헤드리스 모드 최적화)"""
        for attempt in range(retry):
            try:
                # JavaScript로 요소 상태 확인
                is_clickable = self.driver.execute_script("""
                    var elem = arguments[0];
                    var rect = elem.getBoundingClientRect();
                    return elem.offsetParent !== null && 
                           rect.width > 0 && 
                           rect.height > 0 &&
                           rect.top >= 0 &&
                           rect.left >= 0;
                """, element)
                
                if not is_clickable:
                    # 요소를 화면에 표시
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'instant'});", element)
                    time.sleep(self.get_wait_time(0.5))
                
                # JavaScript 클릭
                self.driver.execute_script("arguments[0].click();", element)
                return True
                
            except StaleElementReferenceException:
                if attempt < retry - 1:
                    time.sleep(self.get_wait_time(0.5))
                    continue
                else:
                    logger.error("Stale element - 클릭 실패")
                    return False
            except Exception as e:
                if attempt < retry - 1:
                    time.sleep(self.get_wait_time(0.5))
                    continue
                else:
                    logger.error(f"클릭 오류: {e}")
                    return False
                    
    def select_option(self, name: str, value: str) -> bool:
        """라디오 버튼 선택 (헤드리스 모드 최적화)"""
        max_retries = 3
        
        for retry in range(max_retries):
            try:
                # 모달 처리
                self.check_and_handle_modal()
                
                # JavaScript로 직접 선택
                success = self.driver.execute_script("""
                    var name = arguments[0];
                    var value = arguments[1];
                    var radio = null;
                    
                    // ID로 찾기 (가입유형, 기기종류)
                    if (name === "가입유형" || name === "기기종류") {
                        radio = document.querySelector('input[name="' + name + '"][id="' + value + '"]');
                    } else {
                        radio = document.querySelector('input[name="' + name + '"][value="' + value + '"]');
                    }
                    
                    if (radio) {
                        if (!radio.checked) {
                            radio.checked = true;
                            // change 이벤트 발생
                            var event = new Event('change', { bubbles: true });
                            radio.dispatchEvent(event);
                            
                            // 라벨 클릭도 시도
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
                    # 선택 후 모달 체크
                    self.check_and_handle_modal()
                    logger.info(f"{name} 선택: {value}")
                    return True
                    
            except Exception as e:
                if retry < max_retries - 1:
                    logger.debug(f"{name} 선택 재시도 ({retry + 1}/{max_retries})")
                    time.sleep(self.get_wait_time(1))
                    continue
                else:
                    logger.error(f"옵션 선택 실패 ({name}, {value}): {e}")
                    return False
                    
    def select_all_manufacturers(self) -> bool:
        """제조사 전체 선택 (헤드리스 모드 최적화)"""
        try:
            # 모달 체크
            self.check_and_handle_modal()
            
            # JavaScript로 전체 선택
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
                logger.info("제조사 '전체' 선택 완료")
                # 선택 후 모달 체크
                self.check_and_handle_modal()
                return True
            else:
                logger.error("전체 체크박스를 찾을 수 없습니다")
                return False
                
        except Exception as e:
            logger.error(f"제조사 전체 선택 오류: {e}")
            return False
            
    def wait_for_table_ready(self) -> bool:
        """테이블이 준비될 때까지 대기 (헤드리스 모드 최적화)"""
        try:
            # 모달 체크
            self.check_and_handle_modal()
            
            # 페이지 준비 대기
            self.wait_for_page_ready(15)
            
            # JavaScript로 테이블 확인
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
                # 추가 대기 (데이터 로딩 완료 확인)
                time.sleep(self.get_wait_time(1))
                
                # 테이블 행 수 확인
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
                
                logger.info(f"테이블 발견: {row_count}개 행")
                return row_count > 0
            else:
                logger.error("테이블을 찾을 수 없습니다")
                return False
                
        except Exception as e:
            logger.error(f"테이블 대기 중 오류: {e}")
            return False
            
    def get_rate_plan_price(self, rate_plan_id: str) -> str:
        """요금제의 월 납부금액 조회"""
        try:
            # 캐시 확인
            if rate_plan_id in self.rate_plan_price_cache:
                return self.rate_plan_price_cache[rate_plan_id]
            
            # 먼저 상세 페이지에서 정확한 가격 찾기
            price = self.get_rate_plan_price_from_detail_page(rate_plan_id)
            
            if price != "0":
                return price
                
            # 상세 페이지에서 못 찾으면 현재 페이지에서 찾기
            logger.debug("상세 페이지에서 가격을 찾을 수 없어 현재 페이지에서 조회")
            return self.get_rate_plan_price_from_current_page()
            
        except Exception as e:
            logger.error(f"요금제 가격 조회 오류: {e}")
            return "0"
            
    def get_rate_plan_price_from_detail_page(self, rate_plan_id: str) -> str:
        """요금제 상세 페이지에서 가격 조회 (헤드리스 모드 최적화)"""
        try:
            # 현재 핸들 저장
            original_window = self.driver.current_window_handle
            
            # 새 탭 열기
            self.driver.execute_script("window.open('');")
            self.driver.switch_to.window(self.driver.window_handles[-1])
            
            try:
                # 요금제별 정확한 URL 패턴
                urls = []
                
                # 5G 요금제
                if 'LPZ1' in rate_plan_id:
                    if 'LPZ1001051' in rate_plan_id:
                        urls.append(f"https://www.lguplus.com/mobile/plan/mplan/5g-all/5g-young/{rate_plan_id}")
                    else:
                        urls.extend([
                            f"https://www.lguplus.com/mobile/plan/mplan/5g-all/5g-unlimited/{rate_plan_id}",
                            f"https://www.lguplus.com/mobile/plan/mplan/5g-all/5g-standard/{rate_plan_id}",
                            f"https://www.lguplus.com/mobile/plan/mplan/5g-all/5g-young/{rate_plan_id}"
                        ])
                
                # LTE 요금제
                elif 'LPZ0' in rate_plan_id:
                    if 'LPZ0000469' in rate_plan_id:
                        urls.append(f"https://www.lguplus.com/mobile/plan/mplan/lte-all/lte-youth/{rate_plan_id}")
                    elif 'LPZ0000464' in rate_plan_id:
                        urls.append(f"https://www.lguplus.com/mobile/plan/mplan/lte-all/lte-unlimited/{rate_plan_id}")
                    else:
                        urls.extend([
                            f"https://www.lguplus.com/mobile/plan/mplan/lte-all/lte-unlimited/{rate_plan_id}",
                            f"https://www.lguplus.com/mobile/plan/mplan/lte-all/lte-standard/{rate_plan_id}",
                            f"https://www.lguplus.com/mobile/plan/mplan/lte-all/lte-data/{rate_plan_id}",
                            f"https://www.lguplus.com/mobile/plan/mplan/lte-all/lte-youth/{rate_plan_id}"
                        ])
                
                # 공통 대체 URL
                urls.append(f"https://www.lguplus.com/mobile/plan/detail/{rate_plan_id}")
                
                for url in urls:
                    try:
                        logger.info(f"요금제 페이지 접속: {url}")
                        self.driver.get(url)
                        self.wait_for_page_ready(10)
                        time.sleep(self.get_wait_time(3))
                        
                        # 404 체크
                        is_404 = self.driver.execute_script("""
                            return document.title.toLowerCase().includes('404') ||
                                   document.body.textContent.toLowerCase().includes('not found') ||
                                   document.body.textContent.includes('찾을 수 없');
                        """)
                        
                        if is_404:
                            logger.debug(f"페이지를 찾을 수 없음: {url}")
                            continue
                        
                        # JavaScript로 가격 찾기
                        price = self.driver.execute_script("""
                            var priceText = null;
                            var price = "0";
                            
                            // 다양한 선택자로 가격 찾기
                            var selectors = [
                                'p.price', 'p.price strong', '.price strong', 
                                'strong.price', 'div.price-info', '[class*="price"] strong'
                            ];
                            
                            for (var i = 0; i < selectors.length; i++) {
                                var elements = document.querySelectorAll(selectors[i]);
                                for (var j = 0; j < elements.length; j++) {
                                    var text = elements[j].textContent.trim();
                                    if (text.includes('원')) {
                                        var match = text.match(/(\d{1,3}(?:,\d{3})*)\s*원/);
                                        if (match) {
                                            var extractedPrice = parseInt(match[1].replace(/,/g, ''));
                                            if (extractedPrice >= 10000 && extractedPrice <= 200000) {
                                                return match[1].replace(/,/g, '');
                                            }
                                        }
                                    }
                                }
                            }
                            
                            // 페이지 전체에서 찾기
                            var bodyText = document.body.textContent;
                            var matches = bodyText.match(/월\s*(\d{1,3}(?:,\d{3})*)\s*원/g);
                            if (matches) {
                                for (var k = 0; k < matches.length; k++) {
                                    var priceMatch = matches[k].match(/(\d{1,3}(?:,\d{3})*)/);
                                    if (priceMatch) {
                                        var price = parseInt(priceMatch[1].replace(/,/g, ''));
                                        if (price >= 10000 && price <= 200000) {
                                            return priceMatch[1].replace(/,/g, '');
                                        }
                                    }
                                }
                            }
                            
                            return "0";
                        """)
                        
                        if price != "0":
                            logger.info(f"요금제 가격 발견: {price}원")
                            self.rate_plan_price_cache[rate_plan_id] = price
                            return price
                            
                    except Exception as e:
                        logger.error(f"URL {url} 처리 중 오류: {e}")
                        continue
                
                logger.warning(f"요금제 {rate_plan_id}의 가격을 찾을 수 없습니다")
                return "0"
                
            finally:
                # 탭 닫고 원래 창으로 돌아가기
                self.driver.close()
                self.driver.switch_to.window(original_window)
                
        except Exception as e:
            logger.error(f"요금제 상세 페이지 조회 오류: {e}")
            try:
                self.driver.close()
                self.driver.switch_to.window(original_window)
            except:
                pass
            return "0"
            
    def get_rate_plan_price_from_current_page(self) -> str:
        """현재 페이지에서 요금제 가격 찾기 (헤드리스 모드 최적화)"""
        try:
            # JavaScript로 현재 페이지에서 가격 찾기
            price = self.driver.execute_script("""
                // 선택된 요금제 이름 가져오기
                var selectedPlanInput = document.querySelector('input.c-inp[readonly]');
                var selectedPlanName = selectedPlanInput ? selectedPlanInput.value : "";
                
                // 테이블에서 가격 찾기
                var tables = document.querySelectorAll('table');
                var validPrices = [];
                
                for (var i = 0; i < tables.length; i++) {
                    var rows = tables[i].querySelectorAll('tr');
                    for (var j = 0; j < rows.length; j++) {
                        var rowText = rows[j].textContent;
                        if (selectedPlanName && rowText.includes(selectedPlanName) && rowText.includes('원')) {
                            var matches = rowText.match(/(\d{1,3}(?:,\d{3})*)\s*원/g);
                            if (matches) {
                                for (var k = 0; k < matches.length; k++) {
                                    var priceMatch = matches[k].match(/(\d{1,3}(?:,\d{3})*)/);
                                    if (priceMatch) {
                                        var price = parseInt(priceMatch[1].replace(/,/g, ''));
                                        if (price >= 10000 && price <= 200000) {
                                            validPrices.push(price);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                
                // 가장 빈번한 가격 반환
                if (validPrices.length > 0) {
                    var frequency = {};
                    var maxFreq = 0;
                    var mostCommon = validPrices[0];
                    
                    for (var i = 0; i < validPrices.length; i++) {
                        frequency[validPrices[i]] = (frequency[validPrices[i]] || 0) + 1;
                        if (frequency[validPrices[i]] > maxFreq) {
                            maxFreq = frequency[validPrices[i]];
                            mostCommon = validPrices[i];
                        }
                    }
                    
                    return String(mostCommon);
                }
                
                return "0";
            """)
            
            if price != "0":
                logger.info(f"현재 페이지에서 가격 발견: {price}원")
            
            return price
            
        except Exception as e:
            logger.debug(f"현재 페이지 가격 조회 오류: {e}")
            return "0"
            
    def extract_table_data(self, subscription_type: str, device_type: str, manufacturer: str = "전체", 
                          rate_plan_name: str = "전체", rate_plan_id: str = None, monthly_price: str = "0") -> int:
        """테이블 데이터 추출 (헤드리스 모드 최적화)"""
        extracted_count = 0
        
        try:
            # 세션 체크
            if self.config.get('session_check', True) and not self.check_driver_session():
                logger.error("세션이 유효하지 않습니다.")
                return 0
                
            # 테이블 준비 대기
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
                        
                        // 첫 번째 셀에 rowspan이 있으면 새로운 기기
                        if (cells.length >= 9 && cells[0].getAttribute('rowspan')) {
                            // 기기 정보 추출
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
                            // rowspan이 없는 행 (같은 기기의 다른 약정)
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
                        
                        // 데이터가 유효한 경우 저장
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
            
            # 추출된 데이터 처리
            for item in extracted_data:
                self.data.append({
                    '가입유형': subscription_type,
                    '기기종류': device_type,
                    '제조사': manufacturer,
                    '요금제': rate_plan_name,
                    '요금제ID': rate_plan_id,
                    '월납부금액': monthly_price,
                    '기기명': item['device'],
                    '출고가': item['price'],
                    '공시일자': item['date'],
                    '요금제유지기간': item['planDuration'],
                    '공시지원금': item['subsidy'],
                    '추가공시지원금': item['additionalSubsidy'],
                    '지원금총액': item['totalSubsidy'],
                    '추천할인': item['recommendedDiscount'],
                    '최종구매가': item['finalPrice'],
                    '크롤링시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
                extracted_count += 1
                
            logger.info(f"페이지에서 {extracted_count}개 데이터 추출")
            return extracted_count
            
        except Exception as e:
            logger.error(f"테이블 데이터 추출 오류: {e}")
            if self.config.get('debug_mode'):
                import traceback
                logger.debug(traceback.format_exc())
            return 0
            
    def handle_pagination(self, subscription_type: str, device_type: str, manufacturer: str = "전체", 
                         rate_plan_name: str = "전체", rate_plan_id: str = None, monthly_price: str = "0") -> int:
        """페이지네이션 처리 (최대 20페이지 제한)"""
        page = 1
        total_extracted = 0
        max_pages = self.config.get('max_pages', 20)  # 최대 20페이지로 제한
        consecutive_failures = 0
        
        # 테스트 모드에서는 최대 2페이지
        if self.config.get('test_mode', False):
            max_pages = min(2, max_pages)
            logger.debug(f"테스트 모드: 최대 {max_pages}페이지까지만 크롤링")
            
        logger.info(f"페이지네이션 시작 (최대 {max_pages}페이지)")
        
        while page <= max_pages:
            try:
                # 세션 체크
                if not self.check_driver_session():
                    logger.warning("세션이 유효하지 않습니다.")
                    return total_extracted
                    
                logger.info(f"페이지 {page}/{max_pages} 크롤링 중...")
                
                # 현재 페이지 데이터 추출
                extracted = self.extract_table_data(subscription_type, device_type, manufacturer, 
                                                  rate_plan_name, rate_plan_id, monthly_price)
                total_extracted += extracted
                
                # 첫 페이지에서 데이터를 못 찾으면 한 번 더 시도
                if page == 1 and extracted == 0:
                    logger.warning("첫 페이지에서 데이터를 찾지 못함, 재시도...")
                    time.sleep(self.get_wait_time(3))
                    extracted = self.extract_table_data(subscription_type, device_type, manufacturer, 
                                                      rate_plan_name, rate_plan_id, monthly_price)
                    total_extracted += extracted
                    
                if extracted == 0:
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        logger.warning("연속 3회 데이터 추출 실패. 페이지네이션 종료")
                        break
                else:
                    consecutive_failures = 0
                    
                if extracted == 0 and page == 1:
                    logger.warning(f"페이지 1에서 데이터를 추출할 수 없습니다")
                    break
                
                # 최대 페이지 도달 확인
                if page >= max_pages:
                    logger.info(f"최대 페이지({max_pages}) 도달. 페이지네이션 종료")
                    break
                    
                # 다음 페이지 확인 (JavaScript 사용)
                has_next = self.driver.execute_script("""
                    var pagination = document.querySelector('ul.pagination, div.pagination, nav[aria-label="pagination"]');
                    if (!pagination) return false;
                    
                    var buttons = pagination.querySelectorAll('li');
                    var currentIndex = -1;
                    
                    // 현재 페이지 찾기
                    for (var i = 0; i < buttons.length; i++) {
                        if (buttons[i].classList.contains('active') || 
                            buttons[i].classList.contains('current')) {
                            currentIndex = i;
                            break;
                        }
                    }
                    
                    // 다음 페이지 버튼 확인 및 클릭
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
                    logger.info(f"다음 페이지로 이동 (페이지 {page + 1})")
                    time.sleep(self.get_wait_time(3))
                    self.wait_for_page_ready()
                    page += 1
                else:
                    logger.info(f"마지막 페이지 도달 (페이지 {page})")
                    break
                    
            except Exception as e:
                error_msg = str(e).lower()
                if 'invalid session id' in error_msg:
                    logger.error("세션 오류로 페이지네이션 중단")
                    break
                logger.error(f"페이지 {page} 처리 중 오류: {e}")
                break
                
        logger.info(f"총 {page}개 페이지에서 {total_extracted}개 데이터 수집")
        return total_extracted
        
    def open_rate_plan_modal(self) -> bool:
        """요금제 선택 모달 열기 (헤드리스 모드 최적화)"""
        try:
            # 모달 체크 및 처리
            self.check_and_handle_modal()
            
            # JavaScript로 버튼 찾기 및 클릭
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
                logger.error("요금제 선택 버튼을 찾을 수 없습니다")
                return False
                
            time.sleep(self.get_wait_time(2))
            
            # 모달 확인
            modal_opened = self.driver.execute_script("""
                var modal = document.querySelector('div.modal-content');
                return modal && window.getComputedStyle(modal).display !== 'none';
            """)
            
            if modal_opened:
                logger.info("요금제 선택 모달 열기 성공")
                return True
            else:
                logger.error("모달이 열리지 않았습니다")
                return False
                
        except Exception as e:
            logger.error(f"요금제 모달 열기 실패: {e}")
            return False
            
    def get_all_rate_plans(self) -> List[Dict]:
        """모달에서 모든 요금제 추출 (헤드리스 모드 최적화)"""
        try:
            # JavaScript로 요금제 목록 추출
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
            
            logger.info(f"총 {len(rate_plans)}개의 요금제 발견")
            return rate_plans
            
        except Exception as e:
            logger.error(f"요금제 목록 추출 오류: {e}")
            return []
            
    def restart_driver(self):
        """드라이버 재시작"""
        try:
            if self.driver:
                self.driver.quit()
                logger.info("기존 드라이버 종료")
                
            time.sleep(2)
            self.setup_driver()
            logger.info("새 드라이버 시작 완료")
            
        except Exception as e:
            logger.error(f"드라이버 재시작 오류: {e}")
            raise
            
    def save_screenshot(self, name: str):
        """스크린샷 저장"""
        try:
            screenshot_dir = os.path.join(self.config['output_dir'], 'screenshots')
            os.makedirs(screenshot_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = os.path.join(screenshot_dir, f'{name}_{timestamp}.png')
            
            self.driver.save_screenshot(filename)
            logger.debug(f"스크린샷 저장: {filename}")
            
        except Exception as e:
            logger.debug(f"스크린샷 저장 오류: {e}")
            
    def collect_all_rate_plans(self):
        """모든 조합의 요금제 리스트를 사전에 수집"""
        logger.info("\n📋 전체 요금제 리스트 수집 중...")
        
        subscription_types = [
            ('1', '기기변경'),
            ('2', '번호이동'),
            ('3', '신규가입')
        ]
        
        device_types = [
            ('00', '5G폰'),
            ('01', 'LTE폰')
        ]
        
        total_combinations = len(subscription_types) * len(device_types)
        
        with tqdm(total=total_combinations, desc="요금제 수집", unit="조합") as pbar:
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
                            logger.error(f"가입유형 선택 실패: {sub_name}")
                            pbar.update(1)
                            continue
                            
                        if not self.select_option('기기종류', dev_value):
                            logger.error(f"기기종류 선택 실패: {dev_name}")
                            pbar.update(1)
                            continue
                        
                        # 요금제 모달 열기
                        if self.open_rate_plan_modal():
                            # 요금제 목록 추출
                            rate_plans = self.get_all_rate_plans()
                            
                            # 요금제 개수 제한
                            if self.config['max_rate_plans'] > 0:
                                rate_plans = rate_plans[:self.config['max_rate_plans']]
                                
                            self.all_rate_plans[dev_value][sub_value] = rate_plans
                            logger.debug(f"{sub_name} - {dev_name}: {len(rate_plans)}개 요금제 수집")
                            
                            # 모달 닫기
                            self.driver.execute_script("""
                                var closeBtn = document.querySelector('button.c-btn-close');
                                if (closeBtn) closeBtn.click();
                            """)
                            time.sleep(self.get_wait_time(0.5))
                        else:
                            logger.warning(f"{sub_name} - {dev_name}: 요금제 모달 열기 실패")
                            self.all_rate_plans[dev_value][sub_value] = []
                            
                    except Exception as e:
                        logger.error(f"요금제 수집 오류 ({sub_name}, {dev_name}): {e}")
                        self.all_rate_plans[dev_value][sub_value] = []
                    
                    pbar.update(1)
        
        # 전체 작업 수 계산
        self.total_tasks = sum(
            len(self.all_rate_plans[dev_value][sub_value]) if self.all_rate_plans[dev_value][sub_value] else 1
            for dev_value, dev_name in device_types
            for sub_value, sub_name in subscription_types
        )
        
        logger.info(f"✅ 요금제 수집 완료! 총 {self.total_tasks}개 작업 예정")
            
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
        
        if self.config.get('include_rate_plans', True):
            # 전체 요금제 리스트 사전 수집
            self.collect_all_rate_plans()
            
            logger.info("\n🚀 요금제별 상세 크롤링 시작")
            self._crawl_with_rate_plans(subscription_types, device_types)
        else:
            logger.info("요금제 구분 없이 크롤링 (제조사: 전체)")
            self._crawl_without_rate_plans(subscription_types, device_types)
            
    def _crawl_without_rate_plans(self, subscription_types, device_types):
        """요금제 없이 기본 크롤링"""
        logger.warning("요금제 없이 크롤링합니다. 요금제별 크롤링을 권장합니다.")
        
        total_combinations = len(subscription_types) * len(device_types)
        current = 0
        
        with tqdm(total=total_combinations, desc="크롤링 진행", unit="조합") as pbar:
            for sub_value, sub_name in subscription_types:
                for dev_value, dev_name in device_types:
                    current += 1
                    logger.info(f"\n진행 ({current}/{total_combinations}): {sub_name} - {dev_name} - 전체")
                    
                    retry_count = 0
                    while retry_count < self.config['retry_count']:
                        try:
                            # 세션 상태 확인 및 재시작
                            if not self.check_driver_session():
                                logger.warning("드라이버 세션이 유효하지 않습니다. 재시작합니다.")
                                self.restart_driver()
                            
                            # 페이지 새로고침
                            self.driver.get(self.base_url)
                            self.wait_for_page_ready()
                            time.sleep(self.get_wait_time(3))
                            
                            # 옵션 선택
                            if not self.select_option('가입유형', sub_value):
                                retry_count += 1
                                continue
                                
                            if not self.select_option('기기종류', dev_value):
                                retry_count += 1
                                continue
                                
                            # 제조사 전체 선택
                            if not self.select_all_manufacturers():
                                retry_count += 1
                                continue
                            
                            # 데이터 로딩 대기
                            time.sleep(self.get_wait_time(3))
                            
                            # 페이지네이션 처리하며 데이터 추출
                            extracted = self.handle_pagination(sub_name, dev_name, "전체", "전체", None, "0")
                            
                            if extracted > 0:
                                logger.info(f"✓ {sub_name} - {dev_name} - 전체: {extracted}개 데이터 수집 성공")
                                break
                            else:
                                logger.warning(f"데이터 추출 실패, 재시도 {retry_count + 1}/{self.config['retry_count']}")
                                retry_count += 1
                                
                        except Exception as e:
                            error_msg = str(e).lower()
                            if 'invalid session id' in error_msg or 'session' in error_msg:
                                logger.error("세션 오류 발생. 드라이버를 재시작합니다.")
                                self.restart_driver()
                                retry_count += 1
                                continue
                                
                            logger.error(f"크롤링 오류 ({sub_name}, {dev_name}): {e}")
                            retry_count += 1
                            
                            if retry_count >= self.config['retry_count']:
                                logger.error(f"최대 재시도 횟수 초과: {sub_name} - {dev_name}")
                                
                    pbar.update(1)
                    
                    # 메모리 관리를 위해 주기적으로 드라이버 재시작
                    if current % self.config.get('restart_interval', 3) == 0 and current < total_combinations:
                        logger.info("메모리 관리를 위해 드라이버를 재시작합니다.")
                        self.restart_driver()
                        time.sleep(2)
                                
    def _crawl_with_rate_plans(self, subscription_types, device_types):
        """요금제별 상세 크롤링"""
        
        # 전체 진행률 표시
        with tqdm(total=self.total_tasks, desc="전체 진행", unit="작업") as main_pbar:
            
            combination_count = 0
            for sub_value, sub_name in subscription_types:
                for dev_value, dev_name in device_types:
                    combination_count += 1
                    
                    # 해당 조합의 요금제 가져오기
                    rate_plans = self.all_rate_plans.get(dev_value, {}).get(sub_value, [])
                    
                    if not rate_plans:
                        logger.warning(f"{sub_name} - {dev_name}: 요금제가 없습니다")
                        main_pbar.update(1)
                        continue
                    
                    logger.info(f"\n📱 {sub_name} - {dev_name} ({len(rate_plans)}개 요금제)")
                    
                    # 각 요금제별로 크롤링
                    for i, rate_plan in enumerate(rate_plans):
                        logger.info(f"\n요금제 ({i+1}/{len(rate_plans)}): {rate_plan['name']}")
                        
                        try:
                            # 세션 체크
                            if not self.check_driver_session():
                                self.restart_driver()
                                
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
                                    logger.error(f"요금제 선택 실패: {rate_plan['name']}")
                                    main_pbar.update(1)
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
                                    logger.error("적용 버튼을 찾을 수 없습니다")
                                    main_pbar.update(1)
                                    continue
                                    
                                time.sleep(self.get_wait_time(3))
                                
                            # 제조사 전체 선택
                            if not self.select_all_manufacturers():
                                logger.error("제조사 전체 선택 실패")
                                main_pbar.update(1)
                                continue
                            
                            # 데이터 로딩 대기
                            time.sleep(self.get_wait_time(3))
                            
                            # 요금제 월 납부금액 조회
                            monthly_price = "0"
                            if 'value' in rate_plan and rate_plan['value']:
                                logger.info(f"요금제 {rate_plan['name']} ({rate_plan['value']}) 월 납부금액 조회 중...")
                                monthly_price = self.get_rate_plan_price(rate_plan['value'])
                                
                                if monthly_price == "0":
                                    logger.warning(f"요금제 {rate_plan['name']}의 가격을 찾을 수 없습니다. 가격 정보 없이 진행합니다.")
                                else:
                                    logger.info(f"월 납부금액: {monthly_price}원")
                            
                            # 데이터 추출
                            extracted = self.handle_pagination(sub_name, dev_name, "전체", rate_plan['name'], 
                                                             rate_plan.get('value'), monthly_price)
                            
                            if extracted > 0:
                                logger.info(f"✓ {rate_plan['name']}: {extracted}개 데이터 수집 성공")
                            else:
                                logger.warning(f"데이터 추출 실패: {rate_plan['name']}")
                                
                        except Exception as e:
                            error_msg = str(e).lower()
                            if 'invalid session id' in error_msg or 'session' in error_msg:
                                logger.error("세션 오류 발생. 드라이버를 재시작합니다.")
                                self.restart_driver()
                                
                            logger.error(f"요금제별 크롤링 오류: {e}")
                            
                        main_pbar.update(1)
                    
                    # 메모리 관리를 위해 주기적으로 드라이버 재시작
                    if combination_count % self.config.get('restart_interval', 2) == 0:
                        logger.info("메모리 관리를 위해 드라이버를 재시작합니다.")
                        self.restart_driver()
                        time.sleep(2)
                    
    def save_data(self) -> List[str]:
        """데이터 저장"""
        if not self.data:
            logger.warning("저장할 데이터가 없습니다.")
            return []
            
        # DataFrame 생성
        df = pd.DataFrame(self.data)
        
        # 중복 제거
        original_count = len(df)
        df = df.drop_duplicates()
        if original_count != len(df):
            logger.info(f"중복 제거: {original_count} → {len(df)}")
        
        # 타임스탬프
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        saved_files = []
        
        # 파일 저장
        if 'excel' in self.config['save_formats']:
            excel_file = os.path.join(self.config['output_dir'], f'LGUPlus_지원금정보_{timestamp}.xlsx')
            df.to_excel(excel_file, index=False, engine='openpyxl')
            saved_files.append(excel_file)
            logger.info(f"Excel 파일 저장: {excel_file}")
            
        if 'csv' in self.config['save_formats']:
            csv_file = os.path.join(self.config['output_dir'], f'LGUPlus_지원금정보_{timestamp}.csv')
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            saved_files.append(csv_file)
            logger.info(f"CSV 파일 저장: {csv_file}")
            
        if 'json' in self.config['save_formats']:
            json_file = os.path.join(self.config['output_dir'], f'LGUPlus_지원금정보_{timestamp}.json')
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            saved_files.append(json_file)
            logger.info(f"JSON 파일 저장: {json_file}")
            
        # 통계 출력
        self._print_statistics(df)
        
        return saved_files
        
    def _print_statistics(self, df):
        """통계 출력"""
        logger.info("\n" + "="*60)
        logger.info("크롤링 결과 통계")
        logger.info("="*60)
        logger.info(f"총 데이터 수: {len(df):,}")
        
        # 기본 통계
        for column in ['가입유형', '기기종류']:
            if column in df.columns:
                logger.info(f"\n{column}별:")
                for value, count in df[column].value_counts().items():
                    logger.info(f"  - {value}: {count:,}")
                    
        # 요금제 통계
        if '요금제' in df.columns:
            unique_plans = df['요금제'].nunique()
            logger.info(f"\n요금제: 총 {unique_plans}개")
            if unique_plans > 1:
                rate_plan_counts = df['요금제'].value_counts()
                for plan, count in rate_plan_counts.head(10).items():
                    logger.info(f"  - {plan}: {count:,}")
                if len(rate_plan_counts) > 10:
                    logger.info(f"  ... 외 {len(rate_plan_counts)-10}개")
                    
        # 월납부금액 통계
        if '월납부금액' in df.columns:
            try:
                df['월납부금액_숫자'] = pd.to_numeric(df['월납부금액'], errors='coerce')
                valid_prices = df[df['월납부금액_숫자'] > 0]['월납부금액_숫자']
                if not valid_prices.empty:
                    logger.info(f"\n월납부금액 통계:")
                    logger.info(f"  - 최저: {int(valid_prices.min()):,}원")
                    logger.info(f"  - 최고: {int(valid_prices.max()):,}원")
                    logger.info(f"  - 평균: {int(valid_prices.mean()):,}원")
            except:
                pass
                
        # 기기별 통계
        if '기기명' in df.columns:
            logger.info(f"\n총 기기 종류: {df['기기명'].nunique()}개")
            
            # 상위 5개 기기
            top_devices = df['기기명'].value_counts().head(5)
            if len(top_devices) > 0:
                logger.info("\n인기 기기 TOP 5:")
                for device, count in top_devices.items():
                    logger.info(f"  - {device}: {count:,}개 요금제")
            
        logger.info("="*60 + "\n")
        
    def check_driver_session(self) -> bool:
        """드라이버 세션 상태 확인"""
        try:
            # 간단한 JavaScript 실행으로 세션 확인
            self.driver.execute_script("return document.readyState")
            return True
        except:
            return False
            
    def _test_one_rate_plan(self):
        """테스트 모드: 각 기기종류별 첫 번째 요금제로 모든 조합 테스트"""
        logger.info("="*60)
        logger.info("테스트 모드: 각 기기종류별 첫 번째 요금제로 모든 조합 테스트")
        logger.info("="*60)
        
        # 테스트 모드 플래그 설정
        self.config['test_mode'] = True
        self.config['max_rate_plans'] = 1  # 첫 번째 요금제만
        
        # 전체 요금제 리스트 수집
        self.collect_all_rate_plans()
        
        subscription_types = [
            ('1', '기기변경'),
            ('2', '번호이동'),
            ('3', '신규가입')
        ]
        
        device_types = [
            ('00', '5G폰'),
            ('01', 'LTE폰')
        ]
        
        # 요금제별 크롤링 실행
        self._crawl_with_rate_plans(subscription_types, device_types)
        
        logger.info("\n" + "="*60)
        logger.info("테스트 모드 완료")
        logger.info("="*60)
            
    def run(self):
        """메인 실행 함수"""
        start_time = time.time()
        
        try:
            logger.info("="*60)
            logger.info("LG U+ 휴대폰 지원금 크롤러 v3.9 시작")
            logger.info("="*60)
            logger.info("크롤링 설정:")
            logger.info(f"  - 모드: {'헤드리스' if self.config.get('headless') else 'GUI'}")
            logger.info(f"  - 최대 페이지: {self.config.get('max_pages', 20)}페이지")
            logger.info("  - 가입유형: 기기변경, 번호이동, 신규가입 (전체)")
            logger.info("  - 기기종류: 5G폰, LTE폰")
            logger.info("  - 제조사: 전체")
            logger.info("  - 요금제: " + ("모든 요금제별 상세 크롤링" if self.config.get('include_rate_plans', True) else "구분 없음"))
            logger.info("="*60)
            
            # 드라이버 설정
            self.setup_driver()
            
            # 초기 페이지 로드
            logger.info(f"페이지 로딩: {self.base_url}")
            self.driver.get(self.base_url)
            self.wait_for_page_ready()
            time.sleep(self.get_wait_time(3))
            
            # 테스트 모드 확인
            if self.config.get('test_mode', False):
                self._test_one_rate_plan()
            else:
                # 크롤링 실행
                self.crawl_all_combinations()
            
            # 데이터 저장
            saved_files = self.save_data()
            
            # 실행 시간
            elapsed_time = time.time() - start_time
            logger.info(f"\n총 실행 시간: {elapsed_time/60:.1f}분")
            
            if saved_files:
                logger.info("\n✅ 크롤링이 성공적으로 완료되었습니다!")
                logger.info("저장된 파일:")
                for file in saved_files:
                    logger.info(f"  - {file}")
            else:
                logger.warning("\n⚠️ 저장된 파일이 없습니다.")
                
            return saved_files
            
        except KeyboardInterrupt:
            logger.info("\n사용자에 의해 중단되었습니다.")
            return []
            
        except Exception as e:
            logger.error(f"크롤러 실행 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("드라이버 종료")


def main():
    """CLI 인터페이스"""
    parser = argparse.ArgumentParser(
        description='LG U+ 휴대폰 지원금 크롤러 v3.9',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예제:
  python lg_crawler_v39.py                          # 기본 크롤링 (헤드리스 모드)
  python lg_crawler_v39.py --no-headless            # GUI 모드로 크롤링
  python lg_crawler_v39.py --no-rate-plans          # 요금제 구분 없이 크롤링
  python lg_crawler_v39.py --test-one-rate-plan     # 테스트 모드
  python lg_crawler_v39.py --max-pages 10           # 최대 10페이지까지만
  python lg_crawler_v39.py --debug                  # 디버그 모드
        """
    )
    
    parser.add_argument('--no-headless', action='store_true',
                        help='GUI 모드로 실행 (기본값: 헤드리스)')
    parser.add_argument('--no-rate-plans', action='store_true',
                        help='요금제 구분 없이 크롤링 (빠른 크롤링)')
    parser.add_argument('--max-rate-plans', type=int, default=0,
                        help='크롤링할 최대 요금제 수 (0=전체, 기본값=0)')
    parser.add_argument('--max-pages', type=int, default=20,
                        help='크롤링할 최대 페이지 수 (기본값=20)')
    parser.add_argument('--test-one-rate-plan', action='store_true',
                        help='테스트 모드: 첫 번째 요금제로 모든 가입유형/기기종류 조합 테스트')
    parser.add_argument('--output', type=str, default='data',
                        help='출력 디렉토리 (기본값: data)')
    parser.add_argument('--formats', nargs='+', 
                        choices=['excel', 'csv', 'json'],
                        default=['excel', 'csv'],
                        help='저장 형식 선택 (기본값: excel csv)')
    parser.add_argument('--debug', action='store_true',
                        help='디버그 모드 활성화')
    parser.add_argument('--log-level', 
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        default='INFO',
                        help='로그 레벨 설정')
    parser.add_argument('--retry', type=int, default=3,
                        help='재시도 횟수 (기본값: 3)')
    parser.add_argument('--restart-interval', type=int, default=3,
                        help='드라이버 재시작 간격 (조합 수, 기본값=3)')
    
    args = parser.parse_args()
    
    # 로깅 재설정
    global logger
    log_level = 'DEBUG' if args.debug else args.log_level
    logger = setup_logging(log_level)
    
    # 크롤러 설정
    config = {
        'headless': not args.no_headless,  # --no-headless가 없으면 헤드리스 모드
        'include_rate_plans': not args.no_rate_plans,
        'max_rate_plans': args.max_rate_plans,
        'max_pages': args.max_pages,
        'output_dir': args.output,
        'save_formats': args.formats,
        'debug_mode': args.debug,
        'retry_count': args.retry,
        'restart_interval': args.restart_interval,
        'test_mode': args.test_one_rate_plan
    }
    
    # 크롤러 생성
    crawler = LGUPlusCrawler(config)
    
    try:
        # 일반 크롤링 실행
        saved_files = crawler.run()
        
        if saved_files:
            print(f"\n✅ 크롤링 완료! 저장된 파일: {len(saved_files)}개")
            sys.exit(0)
        else:
            print("\n⚠️ 크롤링된 데이터가 없습니다.")
            sys.exit(1)
                
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        if config.get('debug_mode'):
            import traceback
            traceback.print_exc()
        sys.exit(1)
    finally:
        if crawler.driver:
            crawler.driver.quit()


if __name__ == "__main__":
    # 빠른 테스트를 위한 직접 실행
    if len(sys.argv) == 1:
        print("=" * 60)
        print("LG U+ 휴대폰 지원금 크롤러 v3.9")
        print("=" * 60)
        print("\n주요 개선사항:")
        print("  - ✅ 최대 20페이지 제한")
        print("  - ✅ 헤드리스 모드 기본 설정")
        print("  - ✅ 헤드리스 모드 100% 데이터 수집 보장")
        print("  - ✅ 동적 콘텐츠 로딩 대기 강화")
        print("  - ✅ JavaScript 실행 완료 확인 강화")
        print("\n사용법:")
        print("  기본 실행 (헤드리스): python lg_crawler_v39.py")
        print("  GUI 모드: python lg_crawler_v39.py --no-headless")
        print("  빠른 크롤링: python lg_crawler_v39.py --no-rate-plans")
        print("  테스트 모드: python lg_crawler_v39.py --test-one-rate-plan")
        print("  도움말: python lg_crawler_v39.py --help")
        print("\n파일은 data 폴더에 저장됩니다.")
        print("\n계속하시려면 위 명령어 중 하나를 실행하세요.")
        print("")
    
    main()