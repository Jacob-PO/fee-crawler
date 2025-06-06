#!/usr/bin/env python3
import argparse
import sys
import os
from datetime import datetime
from src.tworld_crawler import TworldCrawler
from src.tworld_complete_crawler import TworldCompleteCrawler
from src.logger import setup_logger
from config import OUTPUT_FORMATS

logger = setup_logger(__name__)

def print_banner():
    """배너 출력"""
    banner = """
╔══════════════════════════════════════════╗
║     T world 공시지원금 크롤러 v2.0      ║
║         Fee Crawler for T world          ║
╚══════════════════════════════════════════╝
    """
    print(banner)

def print_usage_examples():
    """사용 예시 출력"""
    examples = """
📌 사용 예시:
  
  # 기본 크롤링 (단일 페이지)
  python3 main.py
  
  # 특정 URL 크롤링
  python3 main.py --url "https://..."
  
  # 완전 크롤링 (모든 모델/요금제)
  python3 main.py --complete
  
  # CSV만 저장
  python3 main.py --output csv
  
  # 디버그 모드
  python3 main.py --debug
  
  # 브라우저 표시 모드
  python3 main.py --no-headless
    """
    print(examples)

def main():
    print_banner()
    
    parser = argparse.ArgumentParser(
        description='T world 공시지원금 크롤러',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=True
    )
    
    # 크롤링 모드 그룹
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        '--url', 
        type=str, 
        help='크롤링할 특정 URL'
    )
    mode_group.add_argument(
        '--complete',
        action='store_true',
        help='전체 모델/요금제 조합 크롤링 (완전 크롤링 모드)'
    )
    
    # 출력 옵션
    parser.add_argument(
        '--output', 
        choices=OUTPUT_FORMATS + ['all'], 
        default='all',
        help='출력 형식 (기본값: all)'
    )
    
    # 추가 옵션
    parser.add_argument(
        '--screenshot',
        action='store_true',
        help='스크린샷 저장'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='디버그 모드 (상세 로그 출력)'
    )
    
    parser.add_argument(
        '--no-headless',
        action='store_true',
        help='브라우저 화면 표시 (헤드리스 모드 비활성화)'
    )
    
    parser.add_argument(
        '--help-examples',
        action='store_true',
        help='사용 예시 보기'
    )
    
    args = parser.parse_args()
    
    # 사용 예시 출력
    if args.help_examples:
        print_usage_examples()
        return 0
    
    # 디버그 모드 설정
    if args.debug:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("디버그 모드 활성화")
    
    # 헤드리스 모드 설정
    if args.no_headless:
        os.environ['HEADLESS'] = 'False'
        logger.info("브라우저 화면 표시 모드")
    
    try:
        # 완전 크롤링 모드 (모든 모델/요금제 조합)
        if args.complete:
            logger.info("완전 크롤링 모드 시작...")
            logger.info("모든 제조사, 모든 모델, 모든 요금제 조합을 크롤링합니다.")
            logger.info("이 작업은 오래 걸릴 수 있습니다.")
            
            confirm = input("\n계속하시겠습니까? (y/n): ")
            if confirm.lower() != 'y':
                logger.info("크롤링 취소됨")
                return 0
            
            crawler = TworldCompleteCrawler()
            crawler.fetch_complete_data()
            
            if crawler.data:
                saved_files = {}
                
                # 출력 형식에 따라 저장
                if args.output in ['csv', 'all']:
                    csv_file = crawler.save_to_csv(f"tworld_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    if csv_file:
                        saved_files['CSV'] = csv_file
                
                if args.output in ['json', 'all']:
                    json_file = crawler.save_to_json(f"tworld_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                    if json_file:
                        saved_files['JSON'] = json_file
                
                if args.output in ['excel', 'all']:
                    # 시트별 Excel 저장
                    excel_file = crawler.save_to_excel_with_sheets()
                    if excel_file:
                        saved_files['Excel'] = excel_file
                
                # 결과 요약
                print(crawler.get_summary())
                
                # 저장된 파일 목록
                if saved_files:
                    print("\n📁 저장된 파일:")
                    for format_type, filepath in saved_files.items():
                        print(f"  - {format_type}: {filepath}")
                
                # 상위 10개 데이터 샘플 출력
                print("\n📊 데이터 샘플 (공시지원금 상위 10개):")
                sorted_data = sorted(crawler.data, key=lambda x: x.get('public_support_fee', 0), reverse=True)
                for i, item in enumerate(sorted_data[:10], 1):
                    print(f"\n[{i}] {item['device_name']}")
                    if 'plan_name' in item:
                        print(f"    요금제: {item['plan_name']}")
                    print(f"    제조사: {item.get('manufacturer', 'Unknown')}")
                    print(f"    공시지원금: {item['public_support_fee']:,}원")
                    print(f"    추가지원금: {item['additional_support_fee']:,}원")
                    print(f"    총 지원금: {item['total_support_fee']:,}원")
                
                logger.info("\n✅ 완전 크롤링 완료!")
                return 0
            else:
                logger.error("데이터를 수집할 수 없습니다.")
                logger.info("디버그 모드(--debug)로 실행하여 상세 정보를 확인하세요.")
                return 1
        
        # 단일 페이지 크롤링 모드 (기본)
        else:
            logger.info("크롤링 시작...")
            crawler = TworldCrawler()
            
            # URL이 지정되었으면 해당 URL, 아니면 기본 URL
            crawler.fetch_data(url=args.url)
            
            # 스크린샷
            if args.screenshot:
                screenshot_file = crawler.take_screenshot(f"tworld_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                if screenshot_file:
                    logger.info(f"스크린샷 저장: {screenshot_file}")
            
            # 결과 저장
            if crawler.data:
                saved_files = []
                
                if args.output in ['csv', 'all']:
                    filepath = crawler.save_to_csv()
                    if filepath:
                        saved_files.append(filepath)
                
                if args.output in ['excel', 'all']:
                    filepath = crawler.save_to_excel()
                    if filepath:
                        saved_files.append(filepath)
                
                if args.output in ['json', 'all']:
                    filepath = crawler.save_to_json()
                    if filepath:
                        saved_files.append(filepath)
                
                # 결과 요약 출력
                print(crawler.get_summary())
                
                if saved_files:
                    print("\n📁 저장된 파일:")
                    for file in saved_files:
                        print(f"  - {file}")
                
                # 샘플 데이터 출력
                print("\n📊 데이터 샘플 (최대 5개):")
                for i, item in enumerate(crawler.data[:5], 1):
                    print(f"\n[{i}] {item['device_name']}")
                    if 'plan_name' in item:
                        print(f"    요금제: {item.get('plan_name', 'Unknown')}")
                    print(f"    공시지원금: {item['public_support_fee']:,}원")
                    print(f"    추가지원금: {item['additional_support_fee']:,}원")
                    print(f"    총 지원금: {item['total_support_fee']:,}원")
                    if 'release_price' in item and item['release_price'] > 0:
                        print(f"    출시가격: {item['release_price']:,}원")
                    if 'date' in item:
                        print(f"    공시일: {item['date']}")
                
                logger.info("\n✅ 크롤링 완료!")
                return 0
            else:
                logger.error("데이터를 수집할 수 없습니다.")
                logger.info("다음을 확인해보세요:")
                logger.info("  1. 인터넷 연결 상태")
                logger.info("  2. T world 사이트 접속 가능 여부")
                logger.info("  3. --debug 옵션으로 상세 로그 확인")
                return 1
            
    except KeyboardInterrupt:
        logger.info("\n⚠️  사용자에 의해 중단됨")
        return 130
    except ImportError as e:
        logger.error(f"❌ 모듈 임포트 오류: {e}")
        logger.error("필요한 모듈이 설치되지 않았습니다.")
        logger.error("다음 명령어로 설치하세요:")
        logger.error("  pip install -r requirements.txt")
        return 1
    except Exception as e:
        logger.error(f"❌ 예상치 못한 오류: {e}", exc_info=True)
        logger.error("디버그 모드(--debug)로 실행하여 상세 정보를 확인하세요.")
        return 1

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        logger.error(f"프로그램 종료 중 오류: {e}")
        sys.exit(1)
