import re
import json
from bs4 import BeautifulSoup
from src.utils import clean_price, extract_plan_name, extract_device_name
from src.logger import setup_logger

logger = setup_logger(__name__)

class TworldParser:
    """T world HTML 파서"""
    
    def __init__(self):
        self.data = []
        self.device_name = None

    def _parse_json_data(self, html_content):
        """스크립트 내 JSON 배열 파싱"""
        try:
            match = re.search(r"_this\.products\s*=\s*parseObject\((\[.*?\])\);", html_content, re.S)
            if match:
                products = json.loads(match.group(1))
                parsed = []
                for item in products:
                    public_fee = int(item.get('telecomSaleAmt', 0))
                    add_fee = int(item.get('twdSaleAmt', 0))
                    parsed.append({
                        'device_name': item.get('productNm'),
                        'plan_name': item.get('prodNm'),
                        'public_support_fee': public_fee,
                        'additional_support_fee': add_fee,
                        'total_support_fee': public_fee + add_fee
                    })
                return parsed
        except Exception as e:
            logger.debug(f"JSON 데이터 파싱 오류: {e}")
        return []
    
    def parse(self, html_content):
        """HTML 파싱 메인 메서드"""
        # 스크립트 내부 JSON 데이터 우선 파싱
        json_items = self._parse_json_data(html_content)
        if json_items:
            self.data = json_items
            logger.info(f"스크립트 JSON에서 {len(self.data)}개 데이터 파싱")
            return self.data

        soup = BeautifulSoup(html_content, 'html.parser')

        # 디바이스명 추출
        self.device_name = self._extract_device_info(soup)
        logger.info(f"디바이스명: {self.device_name}")

        # 메인 파싱 - depth-num 클래스를 사용한 정확한 파싱
        self._parse_disclosure_data(soup)

        # 테이블에서 추가 데이터 파싱
        self._parse_table_data(soup)

        logger.info(f"총 {len(self.data)}개 데이터 파싱 완료")

        return self.data
    
    def _extract_device_info(self, soup):
        """디바이스 정보 추출"""
        # URL 파라미터에서 추출 시도
        import urllib.parse
        
        # 스크립트에서 URL 찾기
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string and 'prodNm' in script.string:
                match = re.search(r'prodNm["\s:=]+["\'](.*?)["\']', script.string)
                if match:
                    device_name = urllib.parse.unquote(match.group(1))
                    if device_name:
                        return device_name
        
        # 페이지 제목이나 h2에서 찾기
        h2_tags = soup.find_all('h2', class_='h-page')
        for h2 in h2_tags:
            text = h2.get_text(strip=True)
            if '갤럭시' in text or 'Galaxy' in text:
                return text
        
        # 기본값
        return "갤럭시 S25"
    
    def _parse_disclosure_data(self, soup):
        """공시지원금 데이터 파싱 - depth-num 클래스 사용"""
        # 공시지원금이 포함된 모든 요소 찾기
        disclosure_sections = soup.find_all('div', class_='tooltip-ly disclosure')
        
        for section in disclosure_sections:
            # 각 섹션에서 데이터 추출
            data_item = {}
            
            # 공시지원금 찾기
            public_support = section.find(lambda tag: tag.name == 'span' and 
                                        tag.get_text(strip=True).startswith('공시지원금'))
            if public_support:
                # 다음 형제 요소에서 금액 찾기
                price_elem = public_support.find_next_sibling('span', class_='d-price')
                if price_elem:
                    depth_num = price_elem.find('span', class_='depth-num')
                    if depth_num:
                        data_item['public_support_fee'] = clean_price(depth_num.get_text(strip=True))
                        logger.debug(f"공시지원금 발견: {depth_num.get_text(strip=True)}")
            
            # 추가지원금 찾기
            additional_support = section.find(lambda tag: tag.name == 'span' and 
                                            tag.get_text(strip=True).startswith('추가지원금'))
            if additional_support:
                price_elem = additional_support.find_next_sibling('span', class_='d-price')
                if price_elem:
                    depth_num = price_elem.find('span', class_='depth-num')
                    if depth_num:
                        data_item['additional_support_fee'] = clean_price(depth_num.get_text(strip=True))
                        logger.debug(f"추가지원금 발견: {depth_num.get_text(strip=True)}")
            
            # 요금제명 찾기 (주변 텍스트에서)
            parent = section.parent
            plan_name = None
            if parent:
                # 부모 요소에서 요금제명 찾기
                text = parent.get_text()
                plan_name = extract_plan_name(text)
            
            # 데이터가 유효한 경우 추가
            if 'public_support_fee' in data_item:
                if 'additional_support_fee' not in data_item:
                    data_item['additional_support_fee'] = 0
                
                data_item['device_name'] = self.device_name
                data_item['plan_name'] = plan_name or "5GX 프라임"
                data_item['total_support_fee'] = data_item['public_support_fee'] + data_item['additional_support_fee']
                
                # 중복 체크 후 추가
                if not self._is_duplicate(data_item):
                    self.data.append(data_item)
                    logger.info(f"데이터 추가: {data_item}")
    
    def _parse_table_data(self, soup):
        """테이블에서 추가 데이터 파싱"""
        # disclosure-list 테이블 찾기
        tables = soup.find_all('table', class_='disclosure-list')
        
        for table in tables:
            # 헤더 인덱스 찾기
            headers = []
            thead = table.find('thead')
            if thead:
                th_tags = thead.find_all('th')
                headers = [th.get_text(strip=True) for th in th_tags]
                
                # 컬럼 인덱스 찾기
                try:
                    product_idx = next(i for i, h in enumerate(headers) if '상품명' in h)
                    date_idx = next(i for i, h in enumerate(headers) if '공시일' in h)
                    price_idx = next(i for i, h in enumerate(headers) if '출시' in h and '가격' in h)
                    public_idx = next(i for i, h in enumerate(headers) if '공시지원금' in h and '전환' not in h)
                    add_idx = next(i for i, h in enumerate(headers) if '추가' in h and '지원금' in h)
                except StopIteration:
                    logger.warning("테이블 헤더를 찾을 수 없습니다")
                    continue
            
            # tbody의 각 행 처리
            tbody = table.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) > max(public_idx, add_idx):
                        try:
                            # 상품명
                            product_name = cells[product_idx].get_text(strip=True) if product_idx < len(cells) else ""
                            
                            # 공시일
                            date_text = cells[date_idx].get_text(strip=True) if date_idx < len(cells) else ""
                            
                            # 출시가격
                            release_price = clean_price(cells[price_idx].get_text(strip=True)) if price_idx < len(cells) else 0
                            
                            # 공시지원금
                            public_fee = clean_price(cells[public_idx].get_text(strip=True)) if public_idx < len(cells) else 0
                            
                            # 추가지원금
                            add_fee = clean_price(cells[add_idx].get_text(strip=True)) if add_idx < len(cells) else 0
                            
                            # 유효한 데이터인지 확인
                            if public_fee > 0 and public_fee < 10000000:  # 천만원 미만
                                data_item = {
                                    'device_name': product_name or self.device_name,
                                    'plan_name': "5GX 프라임",  # 기본값
                                    'public_support_fee': public_fee,
                                    'additional_support_fee': add_fee,
                                    'total_support_fee': public_fee + add_fee,
                                    'release_price': release_price,
                                    'date': date_text
                                }
                                
                                if not self._is_duplicate(data_item):
                                    self.data.append(data_item)
                                    logger.info(f"테이블 데이터 추가: {product_name}, 공시지원금: {public_fee:,}원")
                        
                        except Exception as e:
                            logger.error(f"행 파싱 오류: {e}")
                            continue
    
    def _is_duplicate(self, new_item):
        """중복 데이터 체크"""
        for item in self.data:
            if (item.get('device_name') == new_item.get('device_name') and
                item.get('public_support_fee') == new_item.get('public_support_fee') and
                item.get('additional_support_fee') == new_item.get('additional_support_fee')):
                return True
        return False
    
    def _clean_and_validate_data(self):
        """데이터 정리 및 검증"""
        cleaned_data = []
        
        for item in self.data:
            # 비정상적인 값 필터링
            if item['public_support_fee'] > 10000000:  # 천만원 이상
                logger.warning(f"비정상적인 공시지원금: {item['public_support_fee']}")
                continue
            
            if item['additional_support_fee'] > 10000000:  # 천만원 이상
                logger.warning(f"비정상적인 추가지원금: {item['additional_support_fee']}")
                continue
            
            # 날짜 형식의 숫자 필터링 (20250525 같은)
            if 20000000 < item['public_support_fee'] < 21000000:
                logger.warning(f"날짜로 의심되는 값 제외: {item['public_support_fee']}")
                continue
            
            cleaned_data.append(item)
        
        self.data = cleaned_data
