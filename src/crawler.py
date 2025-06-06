from abc import ABC, abstractmethod
from datetime import datetime
import pandas as pd
import json
import os
from config import DATA_DIR, TIMESTAMP_FORMAT
from src.utils import validate_data, save_json_backup
from src.logger import setup_logger

logger = setup_logger(__name__)

class BaseCrawler(ABC):
    """크롤러 베이스 클래스"""
    
    def __init__(self):
        self.data = []
        self.timestamp = datetime.now()
        self.crawl_info = {
            'start_time': None,
            'end_time': None,
            'total_items': 0,
            'valid_items': 0,
            'errors': []
        }
    
    @abstractmethod
    def fetch_data(self, **kwargs):
        """데이터 가져오기 (구현 필요)"""
        pass
    
    def validate_and_clean_data(self):
        """데이터 검증 및 정리"""
        valid_data = []
        
        for item in self.data:
            if validate_data(item):
                # timestamp 추가
                item['timestamp'] = self.timestamp
                item['crawled_at'] = self.timestamp.strftime('%Y-%m-%d %H:%M:%S')
                valid_data.append(item)
            else:
                logger.warning(f"유효하지 않은 데이터: {item}")
        
        self.crawl_info['total_items'] = len(self.data)
        self.crawl_info['valid_items'] = len(valid_data)
        self.data = valid_data
        
        logger.info(f"데이터 검증 완료: {len(valid_data)}/{len(self.data)} 항목")
    
    def save_to_csv(self, filename=None):
        """CSV 파일로 저장"""
        if not self.data:
            logger.warning("저장할 데이터가 없습니다.")
            return None
        
        if filename is None:
            filename = f"tworld_fee_{self.timestamp.strftime(TIMESTAMP_FORMAT)}.csv"
        
        filepath = os.path.join(DATA_DIR, filename)
        
        try:
            df = pd.DataFrame(self.data)
            # 컬럼 순서 정리
            columns = ['timestamp', 'device_name', 'plan_name', 'public_support_fee', 
                      'additional_support_fee', 'total_support_fee', 'crawled_at']
            df = df[columns]
            
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"CSV 저장 완료: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"CSV 저장 실패: {e}")
            return None
    
    def save_to_excel(self, filename=None):
        """Excel 파일로 저장"""
        if not self.data:
            logger.warning("저장할 데이터가 없습니다.")
            return None
        
        if filename is None:
            filename = f"tworld_fee_{self.timestamp.strftime(TIMESTAMP_FORMAT)}.xlsx"
        
        filepath = os.path.join(DATA_DIR, filename)
        
        try:
            df = pd.DataFrame(self.data)
            columns = ['timestamp', 'device_name', 'plan_name', 'public_support_fee', 
                      'additional_support_fee', 'total_support_fee', 'crawled_at']
            df = df[columns]
            
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='공시지원금', index=False)
                
                # 워크시트 포맷팅
                worksheet = writer.sheets['공시지원금']
                
                # 컬럼 너비 조정
                column_widths = {
                    'A': 20, 'B': 25, 'C': 30, 'D': 15, 'E': 15, 'F': 15, 'G': 20
                }
                for col, width in column_widths.items():
                    worksheet.column_dimensions[col].width = width
                
                # 숫자 포맷 적용
                for row in range(2, len(df) + 2):
                    for col in ['D', 'E', 'F']:
                        cell = worksheet[f'{col}{row}']
                        cell.number_format = '#,##0'
            
            logger.info(f"Excel 저장 완료: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Excel 저장 실패: {e}")
            return None
    
    def save_to_json(self, filename=None):
        """JSON 파일로 저장"""
        if not self.data:
            logger.warning("저장할 데이터가 없습니다.")
            return None
        
        if filename is None:
            filename = f"tworld_fee_{self.timestamp.strftime(TIMESTAMP_FORMAT)}.json"
        
        filepath = os.path.join(DATA_DIR, filename)
        
        try:
            output_data = {
                'crawl_info': self.crawl_info,
                'data': self.data
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"JSON 저장 완료: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"JSON 저장 실패: {e}")
            return None
    
    def get_summary(self):
        """크롤링 결과 요약"""
        if not self.data:
            return "수집된 데이터가 없습니다."
        
        df = pd.DataFrame(self.data)
        
        summary = f"""
=== 크롤링 결과 요약 ===
수집 시간: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
총 데이터 수: {len(self.data)}개

디바이스별 데이터:
{df['device_name'].value_counts().to_string()}

평균 지원금:
- 공시지원금: {df['public_support_fee'].mean():,.0f}원
- 추가지원금: {df['additional_support_fee'].mean():,.0f}원
- 총 지원금: {df['total_support_fee'].mean():,.0f}원

최대 지원금:
- 공시지원금: {df['public_support_fee'].max():,}원
- 추가지원금: {df['additional_support_fee'].max():,}원
- 총 지원금: {df['total_support_fee'].max():,}원
"""
        return summary