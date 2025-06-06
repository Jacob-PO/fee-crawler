import time
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from src.crawler import BaseCrawler
from src.parser import TworldParser
from src.logger import setup_logger
from config import (CHROME_OPTIONS, HEADLESS, IMPLICIT_WAIT, EXPLICIT_WAIT, 
                   MAX_RETRIES, RETRY_DELAY, DATA_DIR)

logger = setup_logger(__name__)

class TworldCrawler(BaseCrawler):
    """T world 전용 크롤러"""
    
    def __init__(self):
        super().__init__()
        self.driver = None
        self.parser = TworldParser()
    
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        options = Options()
        
        # 기본 옵션 추가
        for option in CHROME_OPTIONS:
            options.add_argument(option)
        
        if HEADLESS:
            options.add_argument('--headless')
        
        # 추가 옵션
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # 이미지 로딩 비활성화 (속도 향상)
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values": {
                "notifications": 2,
                "geolocation": 2
            }
        }
        options.add_experimental_option("prefs", prefs)
        
        try:
            # ChromeDriverManager를 사용한 자동 드라이버 설치
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
            self.driver.implicitly_wait(IMPLICIT_WAIT)
            logger.info("Chrome 드라이버 설정 완료 (Selenium Manager 사용)")
        except Exception as e:
            logger.error(f"Chrome 드라이버 설정 실패: {e}")
            raise
    
    def fetch_data(self, url=None, **kwargs):
        """데이터 가져오기"""
        if url is None:
            url = "https://m.shop.tworld.co.kr/notice?modelNwType=5G&saleMonth=24&dcMthdCd=10&prodId=NA00007790&prodNm=5GX+%ED%94%84%EB%9D%BC%EC%9E%84&saleYn=N&scrbTypCd=31"
        
        self.crawl_info['start_time'] = datetime.now()
        retry_count = 0
        
        while retry_count < MAX_RETRIES:
            try:
                self.setup_driver()
                logger.info(f"페이지 로딩 시작: {url}")
                
                # 페이지 로드
                self.driver.get(url)
                
                # 페이지 로딩 대기
                self._wait_for_page_load()
                
                # 동적 콘텐츠 로딩을 위한 스크롤
                self._scroll_page()
                
                # 추가 대기
                time.sleep(2)
                
                # HTML 가져오기
                page_source = self.driver.page_source
                
                # 디버깅을 위해 HTML 저장
                self._save_debug_html(page_source)
                
                # 스크린샷도 저장
                self._save_debug_screenshot()
                
                # 파싱
                self.data = self.parser.parse(page_source)
                
                # 데이터 검증
                self.validate_and_clean_data()
                
                if self.data:
                    logger.info(f"크롤링 성공: {len(self.data)}개 데이터 수집")
                    break
                else:
                    logger.warning("데이터를 찾을 수 없습니다. 재시도...")
                    retry_count += 1
                    time.sleep(RETRY_DELAY)
                    
            except TimeoutException:
                logger.error(f"페이지 로딩 타임아웃 (시도 {retry_count + 1}/{MAX_RETRIES})")
                retry_count += 1
                time.sleep(RETRY_DELAY)
            except WebDriverException as e:
                logger.error(f"WebDriver 오류: {e}")
                retry_count += 1
                time.sleep(RETRY_DELAY)
            except Exception as e:
                logger.error(f"크롤링 중 오류 발생: {e}")
                self.crawl_info['errors'].append(str(e))
                retry_count += 1
                time.sleep(RETRY_DELAY)
            finally:
                if self.driver:
                    self.driver.quit()
                    self.driver = None
        
        self.crawl_info['end_time'] = datetime.now()
        
        if not self.data:
            logger.error("모든 재시도 실패. 데이터를 수집할 수 없습니다.")
    
    def _wait_for_page_load(self):
        """페이지 로딩 대기"""
        wait = WebDriverWait(self.driver, EXPLICIT_WAIT)
        
        # 여러 조건 중 하나라도 만족하면 진행
        conditions = [
            EC.presence_of_element_located((By.CLASS_NAME, "cont-area")),
            EC.presence_of_element_located((By.CLASS_NAME, "disclosure-list")),
            EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='support']")),
            EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='fee']")),
            EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='price']")),
            EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='disclosure']")),
            EC.presence_of_element_located((By.TAG_NAME, "table")),
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='card']")),
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='item']"))
        ]
        
        for condition in conditions:
            try:
                wait.until(condition)
                logger.info("페이지 요소 로딩 완료")
                return
            except TimeoutException:
                continue
        
        logger.warning("특정 요소를 찾을 수 없지만 계속 진행")
    
    def _scroll_page(self):
        """페이지 스크롤하여 동적 콘텐츠 로드"""
        logger.info("페이지 스크롤 시작")
        
        # 현재 높이
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        scroll_count = 0
        max_scrolls = 5
        
        while scroll_count < max_scrolls:
            # 페이지 끝까지 스크롤
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            
            # 중간 위치로도 스크롤 (일부 사이트는 중간 위치에서 로드)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(0.5)
            
            # 새로운 높이 확인
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            
            if new_height == last_height:
                break
                
            last_height = new_height
            scroll_count += 1
        
        # 맨 위로 스크롤
        self.driver.execute_script("window.scrollTo(0, 0);")
        logger.info(f"스크롤 완료 (총 {scroll_count}회)")
    
    def _save_debug_html(self, page_source):
        """디버깅을 위해 HTML 저장"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'debug_page_{timestamp}.html'
        filepath = os.path.join(DATA_DIR, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(page_source)
            logger.info(f"디버그 HTML 저장: {filepath}")
            
            # HTML에서 주요 정보 추출하여 로깅
            self._analyze_html_content(page_source)
            
        except Exception as e:
            logger.error(f"디버그 HTML 저장 실패: {e}")
    
    def _save_debug_screenshot(self):
        """디버깅을 위해 스크린샷 저장"""
        if self.driver:
            try:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'debug_screenshot_{timestamp}.png'
                filepath = os.path.join(DATA_DIR, filename)
                self.driver.save_screenshot(filepath)
                logger.info(f"디버그 스크린샷 저장: {filepath}")
            except Exception as e:
                logger.error(f"스크린샷 저장 실패: {e}")
    
    def _analyze_html_content(self, html_content):
        """HTML 내용 분석하여 주요 정보 로깅"""
        try:
            # 공시지원금 관련 텍스트 찾기
            import re
            
            # 680,000원 같은 실제 지원금 찾기
            fees = re.findall(r'([1-9]\d{2},\d{3})\s*원', html_content)
            if fees:
                logger.debug(f"HTML에서 발견된 금액들: {fees[:10]}")  # 처음 10개만
            
            # 공시지원금 텍스트 찾기
            if '공시지원금' in html_content:
                # 공시지원금 주변 텍스트
                patterns = re.findall(r'.{0,100}공시지원금.{0,100}', html_content)
                if patterns:
                    logger.debug(f"공시지원금 주변 텍스트 샘플: {patterns[0][:200]}")
            
            # 추가지원금 텍스트 찾기
            if '추가지원금' in html_content:
                patterns = re.findall(r'.{0,100}추가지원금.{0,100}', html_content)
                if patterns:
                    logger.debug(f"추가지원금 주변 텍스트 샘플: {patterns[0][:200]}")
                    
        except Exception as e:
            logger.error(f"HTML 분석 중 오류: {e}")
    
    def take_screenshot(self, filename="screenshot.png"):
        """스크린샷 저장"""
        if self.driver:
            try:
                filepath = os.path.join(DATA_DIR, filename)
                self.driver.save_screenshot(filepath)
                logger.info(f"스크린샷 저장: {filepath}")
                return filepath
            except Exception as e:
                logger.error(f"스크린샷 저장 실패: {e}")
        return None
    
    def get_page_info(self):
        """현재 페이지 정보 가져오기"""
        if self.driver:
            try:
                info = {
                    'url': self.driver.current_url,
                    'title': self.driver.title,
                    'window_size': self.driver.get_window_size(),
                    'cookies': self.driver.get_cookies()
                }
                
                # JavaScript로 추가 정보 가져오기
                js_info = self.driver.execute_script("""
                    return {
                        'readyState': document.readyState,
                        'documentHeight': document.documentElement.scrollHeight,
                        'viewportHeight': window.innerHeight,
                        'elementsCount': document.getElementsByTagName('*').length
                    };
                """)
                
                info.update(js_info)
                return info
            except Exception as e:
                logger.error(f"페이지 정보 가져오기 실패: {e}")
                return None
        return None