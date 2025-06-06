#!/usr/bin/env python3
import argparse
import sys
import os
from datetime import datetime
from src.tworld_crawler import TworldCrawler
from src.tworld_full_crawler import TworldFullCrawler
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

def main():
    print_banner()
    
    parser = argparse.ArgumentParser(
        description='T world 공시지원금 크롤러',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  # 기본 크롤링 (단일 페이지)
  python3 main.py
  
  # 특정 URL 크롤링
  python3 main.py --url "https://..."
  
  # 모든 페이지 크롤링
  python3 main.py --all
  
  # 완전 크롤링 (모든 모델/요금제 조합)
  python3 main.py --complete
  
  # CSV만 저장
  python3 main.py --output csv
  
  # 디버그 모드
  python3 main.py --debug
        """
    )
    
    parser.add_argument(
        '--url', 
        type=str, 
        help='크롤링할 URL (기본값: 5GX 프라임 페이지)'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='모든 상품 크롤링 (전체 크롤링 모드)'
    )
    
    parser.add_argument(
        '--complete',
        action='store_true',
        help='전체 모델/요금제 조합 크롤링 (완전 크롤링 모드)'
    )
    
    parser.add_argument(
        '--output', 
        choices=OUTPUT_FORMATS + ['all'], 
        default='all',
        help='출력 형식 (기본값: all)'
    )
    
    parser.add_argument(
        '--screenshot',
        action='store_true',
        help='스크린샷 저장'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='디버그 모드'
    )
    
    parser.add_argument(
        '--no-headless',
        action='store_true',
        help='브라우저 화면 표시 (헤드리스 모드 비활성화)'
    )
    
    args = parser.parse_args()
    
    # 디버그 모드 설정
    if args.debug:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 헤드리스 모드 설정
    if args.no_headless:
        os.environ['HEADLESS'] = 'False'
    
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
                # 기본 저장
                csv_file = crawler.save_to_csv(f"tworld_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                json_file = crawler.save_to_json(f"tworld_complete_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                
                # 시트별 Excel 저장
                excel_file = crawler.save_to_excel_with_sheets()
                
                # 결과 요약
                print(crawler.get_summary())
                
                print("\n📁 저장된 파일:")
                if csv_file:
                    print(f"  - CSV: {csv_file}")
                if excel_file:
                    print(f"  - Excel: {excel_file}")
                if json_file:
                    print(f"  - JSON: {json_file}")
                
                # 상위 10개 데이터 샘플 출력
                print("\n📊 데이터 샘플 (공시지원금 상위 10개):")
                sorted_data = sorted(crawler.data, key=lambda x: x.get('public_support_fee', 0), reverse=True)
                for i, item in enumerate(sorted_data[:10], 1):
                    device_name = item.get('device_name', 'Unknown')
                    plan_name = item.get('plan_name', item.get('rate_plan_name', 'Unknown Plan'))
                    manufacturer = item.get('manufacturer', 'Unknown')
                    network_type = item.get('network_type', 'Unknown')
                    
                    print(f"\n[{i}] {device_name}")
                    if plan_name != 'Unknown Plan':
                        print(f"    요금제: {plan_name}")
                    print(f"    제조사: {manufacturer} / 네트워크: {network_type}")
                    print(f"    공시지원금: {item.get('public_support_fee', 0):,}원")
                    print(f"    추가지원금: {item.get('additional_support_fee', 0):,}원")
                    print(f"    총 지원금: {item.get('total_support_fee', 0):,}원")
                
                logger.info("완전 크롤링 완료!")
                return 0
            else:
                logger.error("데이터를 수집할 수 없습니다.")
                return 1
        
        # 전체 크롤링 모드
        elif args.all:
            logger.info("전체 크롤링 모드 시작...")
            crawler = TworldFullCrawler()
            crawler.fetch_all_data()
            
            if crawler.data:
                # 모든 형식으로 저장
                saved_files = crawler.save_all_formats()
                
                # 결과 요약 출력
                print(crawler.get_summary())
                
                print("\n📁 저장된 파일:")
                for format_type, filepath in saved_files.items():
                    if filepath:
                        print(f"  - {format_type.upper()}: {filepath}")
                
                logger.info("전체 크롤링 완료!")
                return 0
            else:
                logger.error("데이터를 수집할 수 없습니다.")
                return 1
        
        # 단일 페이지 크롤링 모드 (기본)
        else:
            logger.info("크롤링 시작...")
            crawler = TworldCrawler()
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
                    device_name = item.get('device_name', 'Unknown')
                    plan_name = item.get('plan_name', item.get('rate_plan_name', 'Unknown Plan'))
                    
                    print(f"\n[{i}] {device_name} - {plan_name}")
                    print(f"    공시지원금: {item.get('public_support_fee', 0):,}원")
                    print(f"    추가지원금: {item.get('additional_support_fee', 0):,}원")
                    print(f"    총 지원금: {item.get('total_support_fee', 0):,}원")
                    if 'release_price' in item:
                        print(f"    출시가격: {item['release_price']:,}원")
                    if 'date' in item:
                        print(f"    공시일: {item['date']}")
                
                logger.info("크롤링 완료!")
                return 0
            else:
                logger.error("데이터를 수집할 수 없습니다.")
                return 1
            
    except KeyboardInterrupt:
        logger.info("\n사용자에 의해 중단됨")
        return 130
    except ImportError as e:
        logger.error(f"모듈 임포트 오류: {e}")
        logger.error("필요한 모듈이 설치되지 않았습니다.")
        logger.error("다음 명령어로 설치하세요: pip install -r requirements.txt")
        return 1
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())