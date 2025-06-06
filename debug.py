#!/usr/bin/env python3
"""T world 크롤러 디버깅 도구"""
import time
import json
import argparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import os
from config import DATA_DIR
from src.logger import setup_logger

logger = setup_logger(__name__)

class TworldDebugger:
    """T world 페이지 디버거"""
    
    def __init__(self):
        self.driver = None
    
    def setup_driver(self, headless=False):
        """드라이버 설정 - Selenium 4.6+ 자동 드라이버 관리"""
        options = Options()
        if not headless:
            options.add_argument('--window-size=1920,1080')
        else:
            options.add_argument('--headless')
        
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        try:
            # Selenium 4.6+ 자동 드라이버 관리
            self.driver = webdriver.Chrome(options=options)
            logger.info("Chrome 드라이버 설정 완료 (Selenium Manager 사용)")
        except Exception as e:
            logger.error(f"Chrome 드라이버 설정 실패: {e}")
            logger.info("Chrome 브라우저가 설치되어 있는지 확인하세요.")
            raise
    
    def analyze_page(self, url):
        """페이지 구조 분석"""
        logger.info(f"페이지 분석 시작: {url}")
        
        try:
            self.setup_driver(headless=False)
            self.driver.get(url)
            time.sleep(5)
            
            # 기본 정보
            print("\n" + "="*50)
            print("📋 페이지 기본 정보")
            print("="*50)
            print(f"제목: {self.driver.title}")
            print(f"URL: {self.driver.current_url}")
            
            # JavaScript 변수 확인
            self._check_js_variables()
            
            # HTML 구조 분석
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            self._analyze_html_structure(soup)
            
            # 데이터 패턴 찾기
            self._find_data_patterns(soup)
            
            # 스크린샷 및 HTML 저장
            self._save_debug_files()
            
        except Exception as e:
            logger.error(f"페이지 분석 중 오류: {e}")
        finally:
            if self.driver:
                self.driver.quit()
    
    def _check_js_variables(self):
        """JavaScript 전역 변수 확인"""
        print("\n" + "="*50)
        print("🔧 JavaScript 전역 변수")
        print("="*50)
        
        js_code = """
        var results = {};
        var keywords = ['support', 'fee', 'disclosure', 'price', 'plan', 'product'];
        
        for (var prop in window) {
            for (var keyword of keywords) {
                if (prop.toLowerCase().includes(keyword)) {
                    try {
                        var value = window[prop];
                        var type = typeof value;
                        results[prop] = {
                            type: type,
                            preview: type === 'object' ? JSON.stringify(value).substring(0, 100) : String(value).substring(0, 100)
                        };
                    } catch(e) {}
                }
            }
        }
        return results;
        """
        
        try:
            variables = self.driver.execute_script(js_code)
            if variables:
                for name, info in variables.items():
                    print(f"\n변수명: {name}")
                    print(f"타입: {info['type']}")
                    print(f"미리보기: {info['preview']}...")
            else:
                print("관련 전역 변수를 찾을 수 없습니다.")
        except Exception as e:
            print(f"JavaScript 변수 확인 실패: {e}")
    
    def _analyze_html_structure(self, soup):
        """HTML 구조 분석"""
        print("\n" + "="*50)
        print("🏗️ HTML 구조 분석")
        print("="*50)
        
        # 주요 클래스 찾기
        print("\n📌 공시지원금 관련 클래스:")
        classes_found = set()
        
        for tag in soup.find_all(class_=True):
            classes = tag.get('class', [])
            for cls in classes:
                cls_lower = cls.lower()
                if any(keyword in cls_lower for keyword in 
                      ['support', 'fee', 'disclosure', 'price', '공시', '지원금']):
                    classes_found.add(cls)
        
        for cls in sorted(classes_found):
            print(f"  .{cls}")
        
        # 테이블 구조 확인
        tables = soup.find_all('table')
        if tables:
            print(f"\n📊 테이블 수: {len(tables)}")
            for i, table in enumerate(tables[:3]):
                headers = [th.get_text(strip=True) for th in table.find_all('th')]
                if headers:
                    print(f"  테이블 {i+1} 헤더: {headers}")
    
    def _find_data_patterns(self, soup):
        """데이터 패턴 찾기"""
        print("\n" + "="*50)
        print("💰 금액 정보")
        print("="*50)
        
        import re
        text = soup.get_text()
        
        # 금액 패턴
        prices = re.findall(r'([0-9]{1,3}(?:,[0-9]{3})*)\s*원', text)
        unique_prices = sorted(set(prices), 
                             key=lambda x: int(x.replace(',', '')), 
                             reverse=True)
        
        print("\n발견된 금액 (상위 10개):")
        for i, price in enumerate(unique_prices[:10], 1):
            print(f"  {i}. {price}원")
        
        # 요금제 패턴
        print("\n📱 요금제 정보:")
        plans = re.findall(r'((?:5G|LTE)[^\s]*(?:\s*[가-힣]+)*(?:\s*[0-9]+)?)', text)
        unique_plans = sorted(set([p.strip() for p in plans if len(p.strip()) < 30]))
        
        for plan in unique_plans[:10]:
            print(f"  - {plan}")
    
    def _save_debug_files(self):
        """디버그 파일 저장"""
        print("\n" + "="*50)
        print("💾 디버그 파일 저장")
        print("="*50)
        
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        
        # 스크린샷
        screenshot_path = os.path.join(DATA_DIR, f'debug_screenshot_{timestamp}.png')
        self.driver.save_screenshot(screenshot_path)
        print(f"✓ 스크린샷: {screenshot_path}")
        
        # HTML 소스
        html_path = os.path.join(DATA_DIR, f'debug_source_{timestamp}.html')
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(self.driver.page_source)
        print(f"✓ HTML 소스: {html_path}")
        
        # 페이지 정보 JSON
        info = {
            'url': self.driver.current_url,
            'title': self.driver.title,
            'timestamp': timestamp,
            'window_size': self.driver.get_window_size(),
            'cookies': self.driver.get_cookies()
        }
        
        json_path = os.path.join(DATA_DIR, f'debug_info_{timestamp}.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(info, f, ensure_ascii=False, indent=2)
        print(f"✓ 페이지 정보: {json_path}")
    
    def find_api_calls(self, url):
        """API 호출 찾기"""
        logger.info("API 엔드포인트 탐색 시작")
        
        try:
            from seleniumwire import webdriver as sw_webdriver
            
            options = {
                'disable_encoding': True,
                'request_storage': 'memory'
            }
            
            chrome_options = Options()
            chrome_options.add_argument('--window-size=1920,1080')
            
            driver = sw_webdriver.Chrome(
                seleniumwire_options=options,
                options=chrome_options
            )
            
            try:
                driver.get(url)
                time.sleep(10)
                
                print("\n" + "="*50)
                print("🌐 네트워크 요청 분석")
                print("="*50)
                
                api_calls = []
                
                for request in driver.requests:
                    if request.response and request.response.status_code == 200:
                        url_lower = request.url.lower()
                        
                        # API 관련 요청 필터
                        if any(keyword in url_lower for keyword in 
                              ['api', 'ajax', 'json', 'support', 'fee', 'disclosure']):
                            
                            api_calls.append({
                                'url': request.url,
                                'method': request.method,
                                'status': request.response.status_code,
                                'content_type': request.response.headers.get('Content-Type', ''),
                                'size': len(request.response.body) if request.response.body else 0
                            })
                
                if api_calls:
                    print(f"\n발견된 API 호출: {len(api_calls)}개")
                    for i, call in enumerate(api_calls, 1):
                        print(f"\n[{i}] {call['method']} {call['url']}")
                        print(f"    상태: {call['status']}")
                        print(f"    타입: {call['content_type']}")
                        print(f"    크기: {call['size']} bytes")
                else:
                    print("\nAPI 호출을 찾을 수 없습니다.")
                    
            except Exception as e:
                logger.error(f"API 탐색 중 오류: {e}")
            finally:
                driver.quit()
                
        except ImportError:
            logger.error("selenium-wire가 설치되어 있지 않습니다.")
            logger.info("API 탐색을 위해 다음 명령어로 설치하세요: pip install selenium-wire")

def main():
    parser = argparse.ArgumentParser(description='T world 크롤러 디버깅 도구')
    parser.add_argument('command', choices=['analyze', 'api'], 
                       help='실행할 명령 (analyze: 페이지 분석, api: API 찾기)')
    parser.add_argument('--url', type=str, 
                       default="https://m.shop.tworld.co.kr/notice?modelNwType=5G&saleMonth=24&dcMthdCd=10&prodId=NA00007790",
                       help='분석할 URL')
    
    args = parser.parse_args()
    
    debugger = TworldDebugger()
    
    if args.command == 'analyze':
        debugger.analyze_page(args.url)
    elif args.command == 'api':
        debugger.find_api_calls(args.url)

if __name__ == "__main__":
    main()