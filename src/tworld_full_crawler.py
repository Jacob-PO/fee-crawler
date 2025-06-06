import time
import json
from urllib.parse import urlencode, urlparse, parse_qs
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from src.tworld_crawler import TworldCrawler
from src.logger import setup_logger
from config import BASE_URL, MAX_RETRIES, RETRY_DELAY

logger = setup_logger(__name__)

class TworldFullCrawler(TworldCrawler):
    """T world 전체 상품 크롤러"""
    
    def __init__(self):
        super().__init__()
        self.all_data = []
        self.crawled_urls = set()
        self.filter_options = {
            'network_types': [],  # 5G, LTE
            'sale_months': [],    # 24, 30, 36
            'dc_methods': [],     # 10, 20 등
            'scrb_types': [],     # 31(기기변경), 11(신규가입), 41(번호이동)
            'manufacturers': [],   # 삼성, 애플 등
        }
    
    def fetch_all_data(self):
        """모든 데이터 가져오기"""
        self.crawl_info['start_time'] = time.time()
        
        try:
            # 1. 필터 옵션 수집
            self._collect_filter_options()
            
            # 2. 모든 조합으로 크롤링
            total_combinations = (len(self.filter_options['network_types']) * 
                                len(self.filter_options['sale_months']) * 
                                len(self.filter_options['scrb_types']))
            
            logger.info(f"총 {total_combinations}개 조합 크롤링 시작")
            
            count = 0
            for network_type in self.filter_options['network_types']:
                for sale_month in self.filter_options['sale_months']:
                    for scrb_type in self.filter_options['scrb_types']:
                        count += 1
                        logger.info(f"[{count}/{total_combinations}] {network_type}, {sale_month}개월, 가입유형:{scrb_type}")
                        
                        # URL 생성
                        params = {
                            'modelNwType': network_type,
                            'saleMonth': sale_month,
                            'dcMthdCd': '10',  # 공시지원금
                            'scrbTypCd': scrb_type,
                            'saleYn': 'N'
                        }
                        
                        url = f"{BASE_URL}/notice?" + urlencode(params)
                        
                        # 이미 크롤링한 URL이면 스킵
                        if url in self.crawled_urls:
                            logger.debug(f"이미 크롤링한 URL 스킵: {url}")
                            continue
                        
                        # 크롤링
                        self._crawl_page(url)
                        self.crawled_urls.add(url)
                        
                        # 페이지네이션 처리
                        self._handle_pagination(url)
                        
                        # 잠시 대기 (서버 부하 방지)
                        time.sleep(2)
            
            # 3. 결과 정리
            self._finalize_results()
            
        except Exception as e:
            logger.error(f"전체 크롤링 중 오류: {e}")
            self.crawl_info['errors'].append(str(e))
        
        finally:
            self.crawl_info['end_time'] = time.time()
            elapsed = self.crawl_info['end_time'] - self.crawl_info['start_time']
            logger.info(f"전체 크롤링 완료: {len(self.all_data)}개 데이터, 소요시간: {elapsed:.2f}초")
    
    def _collect_filter_options(self):
        """필터 옵션 수집"""
        logger.info("필터 옵션 수집 시작")
        
        try:
            self.setup_driver()
            self.driver.get(f"{BASE_URL}/notice")
            time.sleep(3)
            
            # 네트워크 타입 (5G, LTE)
            try:
                network_select = self.driver.find_element(By.ID, "model-nw-type")
                options = network_select.find_elements(By.TAG_NAME, "option")
                self.filter_options['network_types'] = [opt.get_attribute('value') for opt in options if opt.get_attribute('value')]
                logger.info(f"네트워크 타입: {self.filter_options['network_types']}")
            except:
                self.filter_options['network_types'] = ['5G', 'LTE']
            
            # 약정 기간
            try:
                month_select = self.driver.find_element(By.ID, "sale-month")
                options = month_select.find_elements(By.TAG_NAME, "option")
                self.filter_options['sale_months'] = [opt.get_attribute('value') for opt in options if opt.get_attribute('value')]
                logger.info(f"약정 기간: {self.filter_options['sale_months']}")
            except:
                self.filter_options['sale_months'] = ['24', '30', '36']
            
            # 가입 유형
            try:
                scrb_select = self.driver.find_element(By.ID, "scrb-typ-cd")
                options = scrb_select.find_elements(By.TAG_NAME, "option")
                self.filter_options['scrb_types'] = [opt.get_attribute('value') for opt in options if opt.get_attribute('value')]
                logger.info(f"가입 유형: {self.filter_options['scrb_types']}")
            except:
                self.filter_options['scrb_types'] = ['31', '11', '41']  # 기기변경, 신규가입, 번호이동
            
        except Exception as e:
            logger.error(f"필터 옵션 수집 실패: {e}")
            # 기본값 사용
            self.filter_options = {
                'network_types': ['5G', 'LTE'],
                'sale_months': ['24'],
                'scrb_types': ['31', '11', '41']
            }
        finally:
            if self.driver:
                self.driver.quit()
                self.driver = None
    
    def _crawl_page(self, url):
        """단일 페이지 크롤링"""
        retry_count = 0
        
        while retry_count < MAX_RETRIES:
            try:
                self.setup_driver()
                logger.debug(f"페이지 로딩: {url}")
                self.driver.get(url)
                
                # 페이지 로딩 대기
                self._wait_for_page_load()
                time.sleep(2)
                
                # 데이터 파싱
                page_source = self.driver.page_source
                self.parser.data = []  # 파서 초기화
                parsed_data = self.parser.parse(page_source)
                
                if parsed_data:
                    # URL 파라미터 추가
                    parsed_url = urlparse(url)
                    params = parse_qs(parsed_url.query)
                    
                    for item in parsed_data:
                        item['network_type'] = params.get('modelNwType', [''])[0]
                        item['sale_month'] = params.get('saleMonth', [''])[0]
                        item['scrb_type'] = params.get('scrbTypCd', [''])[0]
                        item['crawled_url'] = url
                        
                        # 중복 체크
                        if not self._is_duplicate_in_all(item):
                            self.all_data.append(item)
                    
                    logger.info(f"페이지에서 {len(parsed_data)}개 데이터 수집")
                    break
                else:
                    logger.warning("데이터를 찾을 수 없습니다.")
                    retry_count += 1
                    
            except Exception as e:
                logger.error(f"페이지 크롤링 오류: {e}")
                retry_count += 1
                time.sleep(RETRY_DELAY)
            
            finally:
                if self.driver:
                    self.driver.quit()
                    self.driver = None
    
    def _handle_pagination(self, base_url):
        """페이지네이션 처리"""
        try:
            self.setup_driver()
            self.driver.get(base_url)
            self._wait_for_page_load()
            time.sleep(2)
            
            # 페이지 버튼 찾기
            page_numbers = []
            try:
                # 페이지네이션 영역 찾기
                pagination = self.driver.find_element(By.CLASS_NAME, "pagination")
                page_links = pagination.find_elements(By.TAG_NAME, "a")
                
                for link in page_links:
                    text = link.text.strip()
                    if text.isdigit():
                        page_numbers.append(int(text))
                
                max_page = max(page_numbers) if page_numbers else 1
                logger.info(f"총 {max_page}페이지 발견")
                
                # 2페이지부터 크롤링
                for page in range(2, max_page + 1):
                    page_url = f"{base_url}&page={page}"
                    if page_url not in self.crawled_urls:
                        logger.info(f"페이지 {page}/{max_page} 크롤링")
                        self._crawl_page(page_url)
                        self.crawled_urls.add(page_url)
                        time.sleep(2)
                        
            except NoSuchElementException:
                logger.debug("페이지네이션이 없습니다.")
                
        except Exception as e:
            logger.error(f"페이지네이션 처리 오류: {e}")
        finally:
            if self.driver:
                self.driver.quit()
                self.driver = None
    
    def _is_duplicate_in_all(self, new_item):
        """전체 데이터에서 중복 체크"""
        for item in self.all_data:
            if (item.get('device_name') == new_item.get('device_name') and
                item.get('plan_name') == new_item.get('plan_name') and
                item.get('public_support_fee') == new_item.get('public_support_fee') and
                item.get('network_type') == new_item.get('network_type') and
                item.get('scrb_type') == new_item.get('scrb_type')):
                return True
        return False
    
    def _finalize_results(self):
        """결과 정리"""
        # 기존 data 속성에 전체 데이터 할당
        self.data = self.all_data
        
        # 데이터 검증 및 정리
        self.validate_and_clean_data()
        
        # 통계 출력
        self._print_statistics()
    
    def _print_statistics(self):
        """크롤링 통계 출력"""
        if not self.data:
            logger.info("수집된 데이터가 없습니다.")
            return
        
        # 네트워크별 통계
        network_stats = {}
        for item in self.data:
            network = item.get('network_type', 'Unknown')
            if network not in network_stats:
                network_stats[network] = 0
            network_stats[network] += 1
        
        # 디바이스별 통계
        device_stats = {}
        for item in self.data:
            device = item.get('device_name', 'Unknown')
            if device not in device_stats:
                device_stats[device] = 0
            device_stats[device] += 1
        
        # 가입유형별 통계
        scrb_stats = {}
        scrb_names = {'31': '기기변경', '11': '신규가입', '41': '번호이동'}
        for item in self.data:
            scrb = item.get('scrb_type', 'Unknown')
            scrb_name = scrb_names.get(scrb, scrb)
            if scrb_name not in scrb_stats:
                scrb_stats[scrb_name] = 0
            scrb_stats[scrb_name] += 1
        
        logger.info("\n=== 크롤링 통계 ===")
        logger.info(f"총 데이터 수: {len(self.data)}개")
        logger.info(f"크롤링한 URL 수: {len(self.crawled_urls)}개")
        
        logger.info("\n네트워크별:")
        for network, count in network_stats.items():
            logger.info(f"  {network}: {count}개")
        
        logger.info("\n가입유형별:")
        for scrb, count in scrb_stats.items():
            logger.info(f"  {scrb}: {count}개")
        
        logger.info("\n디바이스별 (상위 10개):")
        sorted_devices = sorted(device_stats.items(), key=lambda x: x[1], reverse=True)
        for device, count in sorted_devices[:10]:
            logger.info(f"  {device}: {count}개")
    
    def save_all_formats(self):
        """모든 형식으로 저장"""
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        
        # CSV
        csv_file = self.save_to_csv(f"tworld_all_products_{timestamp}.csv")
        
        # Excel
        excel_file = self.save_to_excel(f"tworld_all_products_{timestamp}.xlsx")
        
        # JSON
        json_file = self.save_to_json(f"tworld_all_products_{timestamp}.json")
        
        return {
            'csv': csv_file,
            'excel': excel_file,
            'json': json_file
        }