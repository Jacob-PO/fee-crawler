#!/bin/bash
echo "=== T world 크롤러 설치 ==="
echo

# Python 확인
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3가 설치되어 있지 않습니다."
    exit 1
fi

# 설치 스크립트 실행
python3 setup.py