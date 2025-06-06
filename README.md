# T world 공시지원금 크롤러 v2.0

T world 사이트에서 공시지원금과 추가지원금 정보를 자동으로 수집하는 크롤러입니다.

## 🚀 주요 기능

- 🔍 **스마트 파싱**: 다양한 HTML 구조 자동 감지
- 📊 **다중 출력 형식**: CSV, Excel, JSON 지원
- 🔄 **자동 재시도**: 실패 시 자동 재시도 기능
- 📸 **스크린샷**: 크롤링 페이지 캡처
- 🐛 **디버깅 도구**: 페이지 구조 분석 도구 내장
- 📝 **상세 로깅**: 컬러 로그 및 파일 저장
- 🎯 **자동 드라이버 관리**: Selenium Manager로 Chrome 드라이버 자동 설치

## 📋 요구사항

- Python 3.7+
- Chrome 브라우저 (최신 버전 권장)
- ~~Chrome 드라이버~~ → **Selenium 4.6+ 자동 관리!**

## 🛠️ 설치

### 1. 프로젝트 클론
```bash
git clone <repository-url>
cd fee-crawler
2. 가상환경 생성 (권장)
bash# Windows
python -m venv venv
venv\Scripts\activate

# Mac/Linux
python3 -m venv venv
source venv/bin/activate
3. 의존성 설치
bashpip install -r requirements.txt

💡 참고: Selenium 4.6 이상 버전이 설치되며, Chrome 드라이버는 자동으로 다운로드됩니다!

📖 사용법
기본 사용
bashpython main.py
옵션 사용
bash# 특정 URL 크롤링
python main.py --url "https://m.shop.tworld.co.kr/notice?..."

# CSV만 저장
python main.py --output csv

# 스크린샷 포함
python main.py --screenshot

# 디버그 모드
python main.py --debug
디버깅 도구
bash# 페이지 구조 분석
python debug.py analyze --url "YOUR_URL"

# API 엔드포인트 찾기 (selenium-wire 필요)
python debug.py api --url "YOUR_URL"
📁 출력 파일
크롤링 결과는 data/ 폴더에 저장됩니다:

CSV: tworld_fee_YYYYMMDD_HHMMSS.csv
Excel: tworld_fee_YYYYMMDD_HHMMSS.xlsx
JSON: tworld_fee_YYYYMMDD_HHMMSS.json

데이터 구조
json{
  "device_name": "갤럭시 S25",
  "plan_name": "5G 프리미어 플러스",
  "public_support_fee": 516000,
  "additional_support_fee": 645400,
  "total_support_fee": 1161400,
  "timestamp": "2024-12-09 10:30:00"
}
🔧 설정
.env 파일로 설정 가능:
env# 헤드리스 모드
HEADLESS=False

# 로그 레벨
LOG_LEVEL=INFO

# 재시도 횟수
MAX_RETRIES=3
📊 결과 예시
╔══════════════════════════════════════════╗
║     T world 공시지원금 크롤러 v2.0      ║
║         Fee Crawler for T world          ║
╚══════════════════════════════════════════╝

=== 크롤링 결과 요약 ===
수집 시간: 2024-12-09 10:30:00
총 데이터 수: 15개

디바이스별 데이터:
갤럭시 S25    10
아이폰 15      5

평균 지원금:
- 공시지원금: 450,000원
- 추가지원금: 550,000원
- 총 지원금: 1,000,000원
🐛 문제 해결
크롤링 실패 시

Chrome 브라우저가 최신 버전인지 확인
디버그 모드로 실행: python main.py --debug
페이지 구조 분석: python debug.py analyze
logs/ 폴더의 로그 파일 확인

Chrome 드라이버 관련

Selenium 4.6+는 드라이버를 자동으로 관리합니다!
수동 설치 불필요
Chrome 브라우저만 설치되어 있으면 OK

일반적인 오류

Chrome not found: Chrome 브라우저 설치 필요
TimeoutException: 인터넷 연결 확인, 페이지 로딩 시간 증가
No data found: 페이지 구조 변경 가능성, 디버그 도구로 확인

📜 라이센스
이 프로젝트는 교육 및 개인 사용 목적으로만 사용하세요.
상업적 사용이나 과도한 요청은 금지됩니다.
🤝 기여
버그 리포트나 기능 제안은 Issues를 통해 제출해주세요.
⚠️ 주의사항

robots.txt 준수
적절한 요청 간격 유지
서버 부하를 주지 않도록 주의
수집한 데이터의 저작권 확인

📚 추가 리소스

Selenium 공식 문서
Selenium Manager 가이드
BeautifulSoup 문서

📞 문의
문제가 있으시면 Issues를 통해 문의해주세요.

## .gitignore
Python
pycache/
*.py[cod]
*$py.class
*.so
.Python
venv/
env/
ENV/
.venv/
크롤링 데이터
data/.csv
data/.xlsx
data/.json
data/.png
data/*.html
로그
logs/*.log
*.log
IDE
.vscode/
.idea/
*.swp
*.swo
.DS_Store
환경 설정
.env
디버그 파일
debug_*
error_*
Chrome Driver
chromedriver
chromedriver.exe

## .env.example
로깅 설정
LOG_LEVEL=INFO
Selenium 설정
HEADLESS=False
크롤링 설정
MAX_RETRIES=3
RETRY_DELAY=2
타임아웃 설정
IMPLICIT_WAIT=10
EXPLICIT_WAIT=20

## run.sh
```bash
#!/bin/bash
# T world 크롤러 실행 스크립트

echo "T world 공시지원금 크롤러 v2.0"
echo "================================"

# 가상환경 활성화
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# 기본 실행
python main.py "$@"