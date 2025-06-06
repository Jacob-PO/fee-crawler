#!/usr/bin/env python3
"""T world 크롤러 설치 스크립트"""
import os
import sys
import subprocess

def check_python_version():
    """Python 버전 확인"""
    if sys.version_info < (3, 7):
        print("❌ Python 3.7 이상이 필요합니다.")
        print(f"현재 버전: {sys.version}")
        return False
    print(f"✅ Python {sys.version.split()[0]} 확인")
    return True

def create_directories():
    """필요한 디렉토리 생성"""
    dirs = ['data', 'logs']
    for dir_name in dirs:
        os.makedirs(dir_name, exist_ok=True)
        print(f"✅ {dir_name}/ 디렉토리 생성")

def install_requirements():
    """의존성 패키지 설치"""
    print("\n📦 패키지 설치 중...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ 모든 패키지 설치 완료")
        return True
    except subprocess.CalledProcessError:
        print("❌ 패키지 설치 실패")
        return False

def create_env_file():
    """환경 설정 파일 생성"""
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write("""# T world 크롤러 설정
LOG_LEVEL=INFO
HEADLESS=False
MAX_RETRIES=3
""")
        print("✅ .env 파일 생성")
    else:
        print("ℹ️  .env 파일이 이미 존재합니다")

def main():
    print("=== T world 크롤러 설치 ===\n")
    
    # Python 버전 확인
    if not check_python_version():
        return 1
    
    # 디렉토리 생성
    create_directories()
    
    # 패키지 설치
    if not install_requirements():
        return 1
    
    # 환경 설정
    create_env_file()
    
    print("\n✨ 설치 완료!")
    print("\n사용법:")
    print("  python main.py")
    print("\n자세한 사용법은 README.md를 참고하세요.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())