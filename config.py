import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# 프로젝트 경로
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# 디렉토리 생성
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# T world 설정
BASE_URL = "https://m.shop.tworld.co.kr"
NOTICE_URL = f"{BASE_URL}/notice"

# 기본 파라미터
DEFAULT_PARAMS = {
    "modelNwType": "5G",
    "saleMonth": "24",
    "dcMthdCd": "10",
    "saleYn": "N"
}

# Selenium 설정
CHROME_OPTIONS = [
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--disable-blink-features=AutomationControlled',
    '--disable-gpu',
    '--window-size=1920,1080',
    '--start-maximized',
    '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

HEADLESS = os.getenv("HEADLESS", "False").lower() == "true"
IMPLICIT_WAIT = 10
EXPLICIT_WAIT = 20

# 로깅 설정
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.path.join(LOG_DIR, f"crawler_{datetime.now().strftime('%Y%m%d')}.log")

# 재시도 설정
MAX_RETRIES = 3
RETRY_DELAY = 2

# 출력 설정
OUTPUT_FORMATS = ["csv", "excel", "json"]
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"