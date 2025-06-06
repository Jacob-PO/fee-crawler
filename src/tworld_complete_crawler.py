import time
import json
import re
import os
import requests
from urllib.parse import urlencode, urlparse, parse_qs, quote_plus
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from tqdm import tqdm
from src.tworld_crawler import TworldCrawler
from src.logger import setup_logger
from config import BASE_URL, MAX_RETRIES, RETRY_DELAY, DATA_DIR, CHROME_OPTIONS, HEADLESS

logger = setup_logger(__name__)

class TworldCompleteCrawler(TworldCrawler):
    """T world 전체 모델/요금제 크롤러"""
    
    def __init__(self):
        super().__init__()
        self.all_data = []
        self.crawled_combinations = set()
        self.rate_plans = []  # 요금제 목록
        self.devices = []     # 디바이스 목록
        self.manufacturers = []  # 제조사 목록
        self.current_page_data = []  # 현재 페이지 데이터 추적

    def _collect_rate_plans(self):
        """요금제 목록 수집"""
        url = "https://shop.tworld.co.kr/wireless/product/subscription/list"
        try:
            self.setup_driver()
            logger.info(f"요금제 페이지 접속: {url}")
            self.driver.get(url)
            time.sleep(5)

            # 1) 자바스크립트 변수에서 요금제 추출 시도
            try:
                plans_json = self.driver.execute_script(
                    "return window._this && _this.products ? JSON.stringify(_this.products) : null;"
                )
                if plans_json:
                    for item in json.loads(plans_json):
                        pid = item.get('prodId')
                        pname = item.get('prodNm')
                        if pid and pname:
                            self.rate_plans.append({'id': pid, 'name': pname})
            except Exception as e:
                logger.debug(f"JS에서 요금제 추출 실패: {e}")

            # 2) HTML 내에서 정규식으로 추출
            if not self.rate_plans:
                html = self.driver.page_source
                plans = re.findall(
                    r'"prodId"\s*:\s*"([^"]+)".*?"prodNm"\s*:\s*"([^"]+)"',
                    html,
                    re.DOTALL,
                )
                for pid, pname in plans:
                    self.rate_plans.append({'id': pid, 'name': pname})

            # 3) DOM 요소에서 추출
            if not self.rate_plans:
                elems = self.driver.find_elements(By.CSS_SELECTOR, '[data-plan-id]')
                for elem in elems:
                    pid = elem.get_attribute('data-plan-id')
                    pname = elem.text.strip()
                    if pid and pname:
                        self.rate_plans.append({'id': pid, 'name': pname})

        except Exception as e:
            logger.error(f"요금제 수집 오류: {e}")
        finally:
            if self.driver:
                self.driver.quit()
                self.driver = None

        if not self.rate_plans:
            logger.warning("요금제 수집 실패 - requests fallback 시도")
            self._collect_rate_plans_fallback(url)

        if not self.rate_plans:
            logger.warning("requests fallback 실패 - 네트워크 캡처 시도")
            self._collect_rate_plans_network(url)

        if self.rate_plans:
            unique = {(p['id'], p['name']) for p in self.rate_plans}
            self.rate_plans = [{'id': i, 'name': n} for i, n in unique]
            logger.info(f"총 {len(self.rate_plans)}개 요금제 수집 완료")
        else:
            logger.warning("요금제 수집 실패")

    def _collect_rate_plans_fallback(self, url):
        """Selenium 실패 시 requests로 요금제 재수집"""
        try:
            ua = next((opt.split('=', 1)[1] for opt in CHROME_OPTIONS if opt.startswith('--user-agent=')), 'Mozilla/5.0')
            headers = {'User-Agent': ua}
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            plans = re.findall(
                r'"prodId"\s*:\s*"([^"]+)".*?"prodNm"\s*:\s*"([^"]+)",?',
                resp.text,
                re.DOTALL,
            )
            for pid, pname in plans:
                self.rate_plans.append({'id': pid, 'name': pname})
            if self.rate_plans:
                logger.info(f"requests fallback: {len(self.rate_plans)}개 요금제 수집 성공")
        except Exception as e:
            logger.error(f"requests fallback 실패: {e}")

    def _collect_rate_plans_network(self, url):
        """selenium-wire로 네트워크 요청에서 요금제 추출"""
        try:
            from seleniumwire import webdriver as sw_webdriver

            options = {"disable_encoding": True, "request_storage": "memory"}

            chrome_options = webdriver.ChromeOptions()
            for opt in CHROME_OPTIONS:
                chrome_options.add_argument(opt)

            if HEADLESS:
                chrome_options.add_argument('--headless')

            driver = sw_webdriver.Chrome(options=chrome_options,
                                         seleniumwire_options=options)

            driver.get(url)
            time.sleep(5)

            for request in driver.requests:
                if request.response and request.response.status_code == 200:
                    ctype = request.response.headers.get('Content-Type', '')
                    if 'application/json' in ctype:
                        try:
                            body = request.response.body.decode('utf-8')
                            data = json.loads(body)
                        except Exception:
                            continue

                        if isinstance(data, list):
                            for item in data:
                                pid = item.get('prodId')
                                pname = item.get('prodNm')
                                if pid and pname:
                                    self.rate_plans.append({'id': pid, 'name': pname})
                        elif isinstance(data, dict):
                            items = data.get('products') or data.get('list') or data.get('plans')
                            if isinstance(items, list):
                                for item in items:
                                    pid = item.get('prodId')
                                    pname = item.get('prodNm')
                                    if pid and pname:
                                        self.rate_plans.append({'id': pid, 'name': pname})
            if self.rate_plans:
                logger.info(f"네트워크 캡처로 {len(self.rate_plans)}개 요금제 수집")
        except Exception as e:
            logger.error(f"네트워크 캡처 실패: {e}")
        finally:
            try:
                driver.quit()
            except Exception:
                pass

    def _build_notice_url(self, network_type, scrb_type, plan):
        params = {
            'modelNwType': network_type,
            'saleMonth': '24',
            'prodId': plan['id'],
            'prodNm': plan['name'],
            'saleYn': 'Y',
            'order': 'DISCOUNT',
            'scrbTypCd': scrb_type['value']
        }
        return f"https://shop.tworld.co.kr/notice?{urlencode(params, quote_via=quote_plus)}"
        
    def fetch_complete_data(self):
        """전체 데이터 수집 메인 메서드"""
        self.crawl_info['start_time'] = time.time()
        
        try:
            logger.info("=" * 50)
            logger.info("STEP 1: 요금제 목록 수집")
            logger.info("=" * 50)
            self._collect_rate_plans()

            logger.info("=" * 50)
            logger.info("STEP 2: 공시지원금 페이지에서 전체 데이터 수집")
            logger.info("=" * 50)
            self._collect_all_from_notice_page()

            self._finalize_results()
            
        except Exception as e:
            logger.error(f"전체 크롤링 중 오류: {e}")
            self.crawl_info['errors'].append(str(e))
        
        finally:
            self.crawl_info['end_time'] = time.time()
            elapsed = self.crawl_info['end_time'] - self.crawl_info['start_time']
            logger.info(f"\n전체 크롤링 완료!")
            logger.info(f"수집 데이터: {len(self.all_data)}개")
            logger.info(f"소요 시간: {elapsed/60:.1f}분")
    
    def _collect_all_from_notice_page(self):
        """공시지원금 페이지에서 모든 데이터 수집"""
        try:
            if not self.rate_plans:
                logger.warning("수집된 요금제가 없습니다.")
                return

            scrb_types = [
                {'value': '31', 'name': '기기변경'},
                {'value': '11', 'name': '신규가입'},
                {'value': '41', 'name': '번호이동'}
            ]

            network_types = ['5G', 'LTE']

            for plan in self.rate_plans:
                for network_type in network_types:
                    for scrb_type in scrb_types:
                        url = self._build_notice_url(network_type, scrb_type, plan)
                        logger.info(f"\n요금제: {plan['name']} ({network_type} / {scrb_type['name']})")
                        self.setup_driver()
                        logger.info(f"공시지원금 페이지 접속: {url}")
                        self.driver.get(url)
                        time.sleep(5)

                        self._save_debug_html(self.driver.page_source, "notice_main")

                        self._collect_all_pages_data(network_type, scrb_type, plan)

                        if self.driver:
                            self.driver.quit()
                            self.driver = None

                        time.sleep(2)
            
        except Exception as e:
            logger.error(f"데이터 수집 중 오류: {e}")
        finally:
            if self.driver:
                self.driver.quit()
                self.driver = None
    
    def _collect_all_pages_data(self, network_type, scrb_type, plan):
        """모든 페이지의 데이터 수집 (개선된 페이지네이션)"""
        try:
            current_page = 1
            total_pages = self._get_total_pages()

            if total_pages <= 1:
                logger.info(f"    페이지 1/1 크롤링")
                self._collect_page_data(network_type, scrb_type, plan)
                return

            logger.info(f"    총 {total_pages}개 페이지 발견")

            with tqdm(total=total_pages, desc=f"    {scrb_type['name']} 페이지 크롤링") as pbar:
                while current_page <= total_pages:
                    logger.debug(f"    페이지 {current_page}/{total_pages} 크롤링")

                    # 현재 페이지 데이터 수집
                    before_count = len(self.all_data)
                    self._collect_page_data(network_type, scrb_type, plan)
                    after_count = len(self.all_data)
                    
                    items_collected = after_count - before_count
                    if items_collected > 0:
                        logger.debug(f"      {items_collected}개 항목 수집")
                    
                    pbar.update(1)
                    
                    if current_page < total_pages:
                        if not self._go_to_next_page(current_page + 1):
                            logger.warning(f"    페이지 {current_page + 1} 이동 실패")
                            break
                        time.sleep(3)
                        current_page += 1
                    else:
                        break
                    
        except Exception as e:
            logger.error(f"전체 페이지 데이터 수집 오류: {e}")
    
    def _get_total_pages(self):
        """총 페이지 수 확인"""
        try:
            page_source = self.driver.page_source
            match = re.search(r'"lastPage"\s*:\s*(\d+)', page_source)
            if match:
                return int(match.group(1))

            # 페이지네이션 영역 찾기
            pagination_selectors = [
                "div.pagination",
                "ul.pagination",
                "div.paging",
                "div[class*='page']"
            ]

            pagination = None
            for selector in pagination_selectors:
                try:
                    pagination = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if pagination:
                        break
                except:
                    continue

            if not pagination:
                logger.debug("페이지네이션을 찾을 수 없습니다.")
                return 1
            
            # 페이지 번호 추출
            page_numbers = []
            
            # 숫자 링크 찾기
            number_links = pagination.find_elements(By.CSS_SELECTOR, "a")
            for link in number_links:
                text = link.text.strip()
                if text.isdigit():
                    page_numbers.append(int(text))
            
            # 'Last' 또는 '>>' 버튼 확인
            last_buttons = pagination.find_elements(By.CSS_SELECTOR, "a[class*='last'], a[title*='마지막']")
            if last_buttons:
                try:
                    # Last 버튼의 onclick이나 href에서 페이지 번호 추출
                    for btn in last_buttons:
                        onclick = btn.get_attribute('onclick')
                        href = btn.get_attribute('href')
                        
                        # onclick에서 페이지 번호 찾기
                        if onclick:
                            match = re.search(r'page[=\(](\d+)', onclick)
                            if match:
                                page_numbers.append(int(match.group(1)))
                        
                        # href에서 페이지 번호 찾기
                        if href:
                            match = re.search(r'page=(\d+)', href)
                            if match:
                                page_numbers.append(int(match.group(1)))
                except:
                    pass
            
            return max(page_numbers) if page_numbers else 1
            
        except Exception as e:
            logger.debug(f"총 페이지 수 확인 오류: {e}")
            return 1
    
    def _go_to_next_page(self, page_number):
        """다음 페이지로 이동"""
        try:
            # 1. 직접 페이지 번호 클릭 시도
            page_link_selectors = [
                f"a:contains('{page_number}')",
                f"a[text()='{page_number}']",
                f"//a[text()='{page_number}']",
                f"a[href*='page={page_number}']"
            ]
            
            for selector in page_link_selectors:
                try:
                    if selector.startswith('//'):
                        # XPath
                        element = self.driver.find_element(By.XPATH, selector)
                    elif ':contains' in selector:
                        # jQuery 스타일 - JavaScript로 처리
                        js_code = f"""
                        var links = document.querySelectorAll('a');
                        for(var i=0; i<links.length; i++) {{
                            if(links[i].textContent.trim() === '{page_number}') {{
                                return links[i];
                            }}
                        }}
                        return null;
                        """
                        element = self.driver.execute_script(js_code)
                    else:
                        # CSS Selector
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if element:
                        # 스크롤하여 요소가 보이도록 함
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                        time.sleep(0.5)
                        
                        # 클릭 시도
                        try:
                            element.click()
                        except:
                            # JavaScript로 클릭
                            self.driver.execute_script("arguments[0].click();", element)
                        
                        logger.debug(f"페이지 {page_number}로 이동 성공")
                        return True
                        
                except:
                    continue
            
            # 2. 'Next' 버튼 클릭 시도
            next_selectors = [
                "a.next",
                "a[class*='next']",
                "a[title*='다음']",
                "a:contains('>')",
                "a:contains('다음')"
            ]
            
            for selector in next_selectors:
                try:
                    if ':contains' in selector:
                        # JavaScript로 처리
                        text_to_find = selector.split("'")[1]
                        js_code = f"""
                        var links = document.querySelectorAll('a');
                        for(var i=0; i<links.length; i++) {{
                            if(links[i].textContent.includes('{text_to_find}')) {{
                                return links[i];
                            }}
                        }}
                        return null;
                        """
                        element = self.driver.execute_script(js_code)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if element and element.is_enabled():
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                        time.sleep(0.5)
                        
                        try:
                            element.click()
                        except:
                            self.driver.execute_script("arguments[0].click();", element)
                        
                        logger.debug(f"Next 버튼으로 페이지 {page_number}로 이동")
                        return True
                        
                except:
                    continue
            
            logger.warning(f"페이지 {page_number}로 이동할 수 없습니다.")
            return False
            
        except Exception as e:
            logger.error(f"페이지 이동 오류: {e}")
            return False
    
    def _is_driver_alive(self):
        """드라이버가 살아있는지 확인"""
        try:
            if self.driver:
                # 간단한 JavaScript 실행으로 확인
                self.driver.execute_script("return true")
                return True
        except:
            return False
        return False

    def _get_plan_info(self):
        """현재 페이지에서 요금제 정보 추출"""
        try:
            page_source = self.driver.page_source
            match = re.search(r'"prodNm":"([^"]+)".*?"basicCharge":(\d+)', page_source)
            if match:
                plan_name = match.group(1)
                plan_price = int(match.group(2))
                return plan_name, plan_price
        except Exception:
            pass
        return "알수없음", 0
    
    def _select_network_type(self, network_type):
        """네트워크 타입 선택"""
        try:
            # select 요소 찾기
            select_element = self.driver.find_element(By.ID, "model-nw-type")
            select = Select(select_element)
            select.select_by_value(network_type)
            logger.debug(f"네트워크 타입 선택: {network_type}")
        except:
            try:
                # 버튼 방식
                button = self.driver.find_element(
                    By.XPATH, f"//button[contains(text(), '{network_type}')]"
                )
                button.click()
                logger.debug(f"네트워크 타입 버튼 클릭: {network_type}")
            except Exception as e:
                logger.error(f"네트워크 타입 선택 실패: {e}")
    
    def _select_scrb_type(self, scrb_type):
        """가입 유형 선택"""
        try:
            # select 요소 찾기 (여러 ID 시도)
            select_ids = ["scrb-typ-cd", "scrbTypCd", "subscription-type"]
            
            for select_id in select_ids:
                try:
                    select_element = self.driver.find_element(By.ID, select_id)
                    select = Select(select_element)
                    select.select_by_value(scrb_type)
                    logger.debug(f"가입 유형 선택: {scrb_type}")
                    return
                except:
                    continue
                    
        except Exception as e:
            logger.error(f"가입 유형 선택 실패: {e}")
    
    def _collect_page_data(self, network_type, scrb_type, plan):
        """현재 페이지의 데이터 수집"""
        try:
            # 드라이버 체크
            if not self._is_driver_alive():
                logger.error("드라이버가 종료되었습니다.")
                return

            plan_name, plan_price = plan['name'], 0
            _, parsed_price = self._get_plan_info()
            if parsed_price:
                plan_price = parsed_price

            # 테이블 찾기
            tables = self.driver.find_elements(By.CLASS_NAME, "disclosure-list")
            if not tables:
                tables = self.driver.find_elements(By.TAG_NAME, "table")
            
            page_data_count = 0
            
            for table in tables:
                try:
                    # tbody 찾기
                    tbody = table.find_element(By.TAG_NAME, "tbody")
                    rows = tbody.find_elements(By.TAG_NAME, "tr")
                    
                    for idx, row in enumerate(rows):
                        try:
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if len(cells) >= 6:  # 최소 필요한 셀 수
                                # 데이터 추출
                                name_elem = None
                                option_elem = None
                                try:
                                    name_elem = cells[0].find_element(By.CSS_SELECTOR, "h4.device")
                                except Exception:
                                    pass
                                try:
                                    option_elem = cells[0].find_element(By.CSS_SELECTOR, "span.option")
                                except Exception:
                                    pass

                                device_name = name_elem.text.strip() if name_elem else cells[0].text.strip()
                                capacity = option_elem.text.strip() if option_elem else ""

                                date_text = cells[1].text.strip() if len(cells) > 1 else ""
                                release_price = self.clean_price(cells[2].text.strip()) if len(cells) > 2 else 0
                                public_fee = self.clean_price(cells[3].text.strip()) if len(cells) > 3 else 0

                                # 추가지원금 위치 확인 (테이블 구조에 따라 조정)
                                add_fee = 0
                                if len(cells) > 5:
                                    add_fee = self.clean_price(cells[5].text.strip())
                                elif len(cells) > 4:
                                    add_fee = self.clean_price(cells[4].text.strip())
                                
                                if device_name and public_fee > 0:
                                    # 중복 체크 (동일 디바이스, 네트워크, 가입유형)
                                    duplicate_key = f"{device_name}_{network_type}_{scrb_type['value']}"

                                    data_item = {
                                        'device_name': device_name,
                                        'manufacturer': self._get_manufacturer_from_name(device_name),
                                        'network_type': network_type,
                                        'scrb_type': scrb_type['value'],
                                        'scrb_type_name': scrb_type['name'],
                                        'plan_name': plan_name,
                                        'plan_price': plan_price,
                                        'capacity': capacity,
                                        'public_support_fee': public_fee,
                                        'additional_support_fee': add_fee,
                                        'total_support_fee': public_fee + add_fee,
                                        'release_price': release_price,
                                        'date': date_text,
                                        'crawled_at': time.strftime('%Y-%m-%d %H:%M:%S')
                                    }
                                    
                                    self.all_data.append(data_item)
                                    page_data_count += 1
                                    logger.debug(f"      수집: {device_name} - 공시지원금: {public_fee:,}원")
                                
                        except Exception as e:
                            logger.debug(f"행 {idx+1} 파싱 오류: {e}")
                            continue
                            
                except Exception as e:
                    logger.debug(f"테이블 파싱 오류: {e}")
                    continue
            
            if page_data_count > 0:
                logger.info(f"      현재 페이지에서 {page_data_count}개 항목 수집")
                    
        except Exception as e:
            logger.error(f"페이지 데이터 수집 오류: {e}")
    
    def clean_price(self, price_str):
        """가격 문자열을 숫자로 변환"""
        if not price_str:
            return 0
        
        # 문자열로 변환
        price_str = str(price_str)
        
        # 숫자가 아닌 문자 제거
        cleaned = re.sub(r'[^0-9]', '', price_str)
        
        try:
            return int(cleaned) if cleaned else 0
        except ValueError:
            return 0
    
    def _get_manufacturer_from_name(self, device_name):
        """디바이스명에서 제조사 추출"""
        name_lower = device_name.lower()
        
        if '갤럭시' in name_lower or 'galaxy' in name_lower:
            return '삼성'
        elif '아이폰' in name_lower or 'iphone' in name_lower or 'ipad' in name_lower:
            return '애플'
        elif 'lg' in name_lower or '엘지' in name_lower:
            return 'LG'
        elif '샤오미' in name_lower or 'xiaomi' in name_lower or 'redmi' in name_lower:
            return '샤오미'
        elif '모토로라' in name_lower or 'motorola' in name_lower:
            return '모토로라'
        else:
            return '기타'
    
    def _save_debug_html(self, html_content, prefix="debug"):
        """디버깅을 위한 HTML 저장"""
        try:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            filename = f"{prefix}_{timestamp}.html"
            filepath = os.path.join(DATA_DIR, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.debug(f"HTML 저장: {filepath}")
        except Exception as e:
            logger.error(f"HTML 저장 실패: {e}")
    
    def _finalize_results(self):
        """결과 정리"""
        self.data = self.all_data
        self.validate_and_clean_data()
        
        # 통계 출력
        logger.info("\n" + "=" * 50)
        logger.info("크롤링 통계")
        logger.info("=" * 50)
        
        if not self.data:
            logger.warning("수집된 데이터가 없습니다.")
            return
        
        # 제조사별 통계
        mfr_stats = {}
        for item in self.data:
            mfr = item.get('manufacturer', '기타')
            mfr_stats[mfr] = mfr_stats.get(mfr, 0) + 1
        
        logger.info("\n제조사별 데이터:")
        for mfr, count in sorted(mfr_stats.items()):
            logger.info(f"  {mfr}: {count}개")
        
        # 네트워크별 통계
        network_stats = {}
        for item in self.data:
            network = item.get('network_type', 'Unknown')
            network_stats[network] = network_stats.get(network, 0) + 1
        
        logger.info("\n네트워크별 데이터:")
        for network, count in sorted(network_stats.items()):
            logger.info(f"  {network}: {count}개")
        
        # 가입유형별 통계
        scrb_stats = {}
        for item in self.data:
            scrb = item.get('scrb_type_name', 'Unknown')
            scrb_stats[scrb] = scrb_stats.get(scrb, 0) + 1
        
        logger.info("\n가입유형별 데이터:")
        for scrb, count in sorted(scrb_stats.items()):
            logger.info(f"  {scrb}: {count}개")
        
        # 최고/최저 공시지원금
        if self.data:
            valid_data = [d for d in self.data if d.get('public_support_fee', 0) > 0]
            if valid_data:
                max_support = max(valid_data, key=lambda x: x.get('public_support_fee', 0))
                min_support = min(valid_data, key=lambda x: x.get('public_support_fee', 0))
                
                logger.info(f"\n최고 공시지원금:")
                logger.info(f"  {max_support['device_name']} ({max_support['network_type']} / {max_support['scrb_type_name']})")
                logger.info(f"  = {max_support['public_support_fee']:,}원")
                
                logger.info(f"\n최저 공시지원금:")
                logger.info(f"  {min_support['device_name']} ({min_support['network_type']} / {min_support['scrb_type_name']})")
                logger.info(f"  = {min_support['public_support_fee']:,}원")
    
    def save_to_excel_with_sheets(self, filename=None):
        """시트별로 정리된 Excel 파일 저장"""
        if not self.data:
            logger.warning("저장할 데이터가 없습니다.")
            return None
        
        import pandas as pd
        from datetime import datetime
        
        if filename is None:
            filename = f"tworld_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        filepath = os.path.join(DATA_DIR, filename)
        
        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # 1. 전체 데이터 시트
                df_all = pd.DataFrame(self.data)
                
                # 컬럼 순서 정리
                columns_order = [
                    'device_name', 'manufacturer', 'network_type', 'scrb_type_name',
                    'plan_name', 'public_support_fee', 'additional_support_fee',
                    'total_support_fee', 'release_price', 'date', 'crawled_at'
                ]
                
                # 존재하는 컬럼만 선택
                columns_to_use = [col for col in columns_order if col in df_all.columns]
                df_all = df_all[columns_to_use]
                
                # 정렬
                df_all = df_all.sort_values(['network_type', 'scrb_type_name', 'public_support_fee'], 
                                          ascending=[True, True, False])
                
                df_all.to_excel(writer, sheet_name='전체데이터', index=False)
                
                # 2. 제조사별 시트
                if 'manufacturer' in df_all.columns:
                    for manufacturer in sorted(df_all['manufacturer'].unique()):
                        df_mfr = df_all[df_all['manufacturer'] == manufacturer]
                        if not df_mfr.empty:
                            sheet_name = f"{manufacturer}"[:31]  # Excel 시트명 제한
                            df_mfr.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # 3. 네트워크별 시트
                if 'network_type' in df_all.columns:
                    for network in sorted(df_all['network_type'].unique()):
                        df_net = df_all[df_all['network_type'] == network]
                        if not df_net.empty:
                            sheet_name = f"{network}_전체"[:31]
                            df_net.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # 4. 가입유형별 시트
                if 'scrb_type_name' in df_all.columns:
                    for scrb_type in sorted(df_all['scrb_type_name'].unique()):
                        df_scrb = df_all[df_all['scrb_type_name'] == scrb_type]
                        if not df_scrb.empty:
                            sheet_name = f"{scrb_type}"[:31]
                            df_scrb.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # 5. 요약 통계 시트
                summary_data = []
                
                # 제조사별 평균 공시지원금
                if 'manufacturer' in df_all.columns and 'public_support_fee' in df_all.columns:
                    mfr_avg = df_all.groupby('manufacturer')['public_support_fee'].agg(['mean', 'count', 'max', 'min'])
                    for mfr, row in mfr_avg.iterrows():
                        summary_data.append({
                            '구분': '제조사별',
                            '항목': mfr,
                            '평균 공시지원금': int(row['mean']),
                            '최대 공시지원금': int(row['max']),
                            '최소 공시지원금': int(row['min']),
                            '데이터 수': int(row['count'])
                        })
                
                # 네트워크별 평균 공시지원금
                if 'network_type' in df_all.columns and 'public_support_fee' in df_all.columns:
                    net_avg = df_all.groupby('network_type')['public_support_fee'].agg(['mean', 'count', 'max', 'min'])
                    for net, row in net_avg.iterrows():
                        summary_data.append({
                            '구분': '네트워크별',
                            '항목': net,
                            '평균 공시지원금': int(row['mean']),
                            '최대 공시지원금': int(row['max']),
                            '최소 공시지원금': int(row['min']),
                            '데이터 수': int(row['count'])
                        })
                
                # 가입유형별 평균 공시지원금
                if 'scrb_type_name' in df_all.columns and 'public_support_fee' in df_all.columns:
                    scrb_avg = df_all.groupby('scrb_type_name')['public_support_fee'].agg(['mean', 'count', 'max', 'min'])
                    for scrb, row in scrb_avg.iterrows():
                        summary_data.append({
                            '구분': '가입유형별',
                            '항목': scrb,
                            '평균 공시지원금': int(row['mean']),
                            '최대 공시지원금': int(row['max']),
                            '최소 공시지원금': int(row['min']),
                            '데이터 수': int(row['count'])
                        })
                
                if summary_data:
                    df_summary = pd.DataFrame(summary_data)
                    df_summary.to_excel(writer, sheet_name='요약통계', index=False)
                
                # 각 시트 포맷팅
                for sheet_name in writer.sheets:
                    worksheet = writer.sheets[sheet_name]
                    
                    # 컬럼 너비 자동 조정
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        
                        adjusted_width = min(max_length + 2, 50)
                        worksheet.column_dimensions[column_letter].width = adjusted_width
                    
                    # 헤더 스타일 적용
                    from openpyxl.styles import Font, PatternFill, Alignment
                    header_font = Font(bold=True)
                    header_fill = PatternFill(start_color="D3D3D3", end_color="D3D3D3", fill_type="solid")
                    center_alignment = Alignment(horizontal='center', vertical='center')
                    
                    for cell in worksheet[1]:
                        cell.font = header_font
                        cell.fill = header_fill
                        cell.alignment = center_alignment
            
            logger.info(f"Excel 파일 저장 완료: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Excel 저장 실패: {e}")
            return None