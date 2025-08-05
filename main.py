"""
Main entry point for the Stock Selector application.
"""
import argparse
from src.data_acquisition.sec_downloader import SECFilingDownloader
from src.data_acquisition.stock_utils import (
    generate_cik_ticker_mapping,
    update_ipo_dates,
    download_daily_stock_prices
)
from src.llm_processing.financial_extractor import FinancialDataExtractor
from src.utils.config import config
from src.utils.logger import setup_logger

logger = setup_logger(__name__, "logs/main.log")

def download_data(args):
    """Download SEC filings and master index files."""
    logger.info("Starting data download")
    downloader = SECFilingDownloader()
    
    # Get years from config
    start_year = config.get("sec_edgar.start_year", 2010)
    end_year = config.get("sec_edgar.end_year", 2025)
    
    # Download master files
    downloader.download_master_files(start_year, end_year)
    
    # Generate filings list
    downloader.generate_filings_list()
    
    # Download filings
    downloader.download_filings()

def process_data(args):
    """Process downloaded filings and extract financial data."""
    logger.info("Starting data processing")
    
    # Generate CIK-ticker mapping
    generate_cik_ticker_mapping()
    
    # Update IPO dates
    update_ipo_dates()
    
    # Download daily stock prices
    download_daily_stock_prices()
    
    # Initialize financial extractor
    extractor = FinancialDataExtractor()
    
    # TODO: Add filing processing logic here
    logger.info("Data processing complete")

def analyze_stocks(args):
    """Analyze stocks based on screening criteria."""
    logger.info("Starting stock analysis")
    
    # TODO: Implement stock analysis logic
    logger.info("Stock analysis complete")

def generate_report(args):
    """Generate reports and visualizations."""
    logger.info("Generating reports")
    
    # TODO: Implement report generation
    logger.info("Report generation complete")

def cleanup_data(args):
    """Clean up and archive old data."""
    logger.info("Cleaning up data")
    
    # TODO: Implement cleanup logic
    logger.info("Data cleanup complete")

def main():
    """Main entry point for the Stock Selector application."""
    parser = argparse.ArgumentParser(
        description="Stock Selector - Identify high-quality stocks based on SEC filings"
    )
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Download data command
    download_parser = subparsers.add_parser(
        "download-data",
        help="Download SEC filings and master index files"
    )
    download_parser.set_defaults(func=download_data)
    
    # Process data command
    process_parser = subparsers.add_parser(
        "process-data",
        help="Process downloaded filings and extract financial data"
    )
    process_parser.set_defaults(func=process_data)
    
    # Analyze stocks command
    analyze_parser = subparsers.add_parser(
        "screen-stocks",
        help="Analyze stocks based on screening criteria"
    )
    analyze_parser.set_defaults(func=analyze_stocks)
    
    # Generate report command
    report_parser = subparsers.add_parser(
        "generate-report",
        help="Generate reports and visualizations"
    )
    report_parser.set_defaults(func=generate_report)
    
    # Cleanup data command
    cleanup_parser = subparsers.add_parser(
        "cleanup-data",
        help="Clean up and archive old data"
    )
    cleanup_parser.set_defaults(func=cleanup_data)
    
    # Parse arguments and execute command
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()