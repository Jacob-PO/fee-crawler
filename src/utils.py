import re
from datetime import datetime
import json
import os
from src.logger import setup_logger

logger = setup_logger(__name__)

def clean_price(price_str):
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
        logger.warning(f"가격 변환 실패: {price_str}")
        return 0

def format_price(price):
    """숫자를 한국식 가격 포맷으로 변환"""
    return f"{price:,}원"

def extract_device_name(text):
    """텍스트에서 디바이스명 추출"""
    # 일반적인 디바이스명 패턴
    patterns = [
        r'(갤럭시|Galaxy)\s*[A-Z0-9]+',
        r'(아이폰|iPhone)\s*[0-9]+',
        r'[A-Z0-9]+\s*(프라임|울트라|플러스|프로)',
        r'5GX?\s*[가-힣]+'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0).strip()
    
    return None

def extract_plan_name(text):
    """텍스트에서 요금제명 추출"""
    # 요금제 패턴
    patterns = [
        r'5G\s*[가-힣]+\s*[0-9]+',
        r'LTE\s*[가-힣]+\s*[0-9]+',
        r'(5G|LTE)\s*[가-힣]+',
        r'[가-힣]+\s*[0-9]+\s*(요금제)?'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0).strip()
    
    return None

def validate_data(data_item):
    """데이터 유효성 검증"""
    required_fields = ['device_name', 'plan_name', 'public_support_fee', 'additional_support_fee']
    
    for field in required_fields:
        if field not in data_item:
            return False
    
    # 가격이 0보다 큰지 확인
    if data_item['public_support_fee'] <= 0 and data_item['additional_support_fee'] <= 0:
        return False
    
    return True

def save_json_backup(data, filename):
    """JSON 백업 저장"""
    try:
        backup_path = os.path.join('data', f'{filename}_backup.json')
        with open(backup_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        logger.info(f"JSON 백업 저장: {backup_path}")
    except Exception as e:
        logger.error(f"JSON 백업 저장 실패: {e}")