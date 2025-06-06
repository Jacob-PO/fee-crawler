@echo off
chcp 65001 > nul
echo === T world 크롤러 설치 ===
echo.

REM Python 확인
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python이 설치되어 있지 않습니다.
    echo https://www.python.org 에서 Python을 설치하세요.
    pause
    exit /b 1
)

REM 설치 스크립트 실행
python setup.py
pause