"""
SEC EDGAR data downloader for the Stock Selector project.
"""
import os
import requests
import time
from typing import Dict, List
from tqdm import tqdm
import pandas as pd
from src.utils.config import config
from src.utils.logger import setup_logger

logger = setup_logger(__name__, "logs/sec_downloader.log")


class SECFilingDownloader:
    """Downloads SEC EDGAR filings and master index files."""
    
    def __init__(self):
        """Initialize the SEC downloader with configuration."""
        self.base_url = config.get("sec_edgar.base_url")
        self.user_agent = config.get("sec_edgar.user_agent")
        self.rate_limit_delay = config.get("sec_edgar.rate_limit_delay", 0.1)
        self.headers = {"User-Agent": self.user_agent}
        self.project_folder = config.get("storage.filings_dir", "data/edgar")
        
        # Create project directory
        os.makedirs(self.project_folder, exist_ok=True)
    
    def download_master_files(self, start_year: int, end_year: int) -> None:
        """
        Download SEC EDGAR master index files for specified years.
        
        Args:
            start_year (int): Starting year
            end_year (int): Ending year
        """
        logger.info(f"Downloading master files from {start_year} to {end_year}")
        
        quarters = ["QTR1", "QTR2", "QTR3", "QTR4"]
        
        # Calculate total number of tasks for progress bar
        total_tasks = 0
        for year in range(start_year, end_year + 1):
            if year == start_year or year == end_year:
                total_tasks += 1
            else:
                total_tasks += 4
        if start_year == end_year:
            total_tasks = 1
        
        # Initialize progress bar
        with tqdm(total=total_tasks, desc="Downloading master.idx files") as pbar:
            for year in range(start_year, end_year + 1):
                year_dir = os.path.join(self.project_folder, str(year))
                os.makedirs(year_dir, exist_ok=True)
                
                # Limit quarters based on start and end years
                if year == start_year:
                    qtr_range = quarters
                elif year == end_year:
                    qtr_range = quarters[:1]
                else:
                    qtr_range = quarters
                
                for qtr in qtr_range:
                    qtr_dir = os.path.join(year_dir, qtr)
                    os.makedirs(qtr_dir, exist_ok=True)
                    
                    # Construct URL for master.idx
                    url = f"{self.base_url}edgar/full-index/{year}/{qtr}/master.idx"
                    file_path = os.path.join(qtr_dir, "master.idx")
                    
                    # Download only if file doesn't already exist
                    if not os.path.exists(file_path):
                        try:
                            response = requests.get(url, headers=self.headers)
                            if response.status_code == 200:
                                with open(file_path, 'wb') as f:
                                    f.write(response.content)
                                logger.debug(f"Downloaded {year}/{qtr}/master.idx")
                            else:
                                logger.error(
                                    f"Failed to download {year}/{qtr}/master.idx - "
                                    f"Status: {response.status_code}"
                                )
                        except Exception as e:
                            logger.error(
                                f"Error downloading {year}/{qtr}/master.idx: {e}"
                            )
                        # Respect SEC rate limits
                        time.sleep(self.rate_limit_delay)
                    # Update progress bar after each quarter
                    pbar.update(1)
                    pbar.set_description(f"Processing {year}/{qtr}")
        
        logger.info(f"Download complete. Files stored in {self.project_folder}")
    
    def generate_filings_list(
        self,
        idx_dir: str = None,
        output_file: str = "data/filings_list.csv"
    ) -> None:
        """
        Generate a filings list CSV from master index files.
        
        Args:
            idx_dir (str): Directory containing master.idx files
            output_file (str): Path to save the filings list CSV
        """
        if idx_dir is None:
            idx_dir = self.project_folder
            
        logger.info(f"Generating filings list from {idx_dir}")
        
        # Generate a new filings list from master.idx files
        filings_list = []
        years = range(2010, 2026)
        quarters = ["QTR1", "QTR2", "QTR3", "QTR4"]
        total_idx_files = sum(
            1 for year in years for qtr in quarters
            if os.path.exists(f"{idx_dir}/{year}/{qtr}/master.idx")
        )
        
        with tqdm(total=total_idx_files, desc="Parsing master.idx files") as pbar:
            for year in years:
                for qtr in quarters:
                    idx_file = f"{idx_dir}/{year}/{qtr}/master.idx"
                    if os.path.exists(idx_file):
                        with open(idx_file, 'r', encoding='latin-1') as f:
                            lines = f.readlines()[11:]  # Skip header
                        for line in lines:
                            parts = line.strip().split('|')
                            if len(parts) >= 5 and parts[2] in ['10-K']:
                                filings_list.append({
                                    'CIK': parts[0],
                                    'Company': parts[1],
                                    'Form': parts[2],
                                    'Date': parts[3],
                                    'URL': self.base_url + parts[4]
                                })
                        pbar.update(1)
                        pbar.set_description(f"Parsing {year}/{qtr}")
        
        filings_df = pd.DataFrame(filings_list)
        filings_df.to_csv(output_file, index=False)
        logger.info(f"Generated new {output_file} with {len(filings_df)} entries.")
    
    def download_filings(
        self,
        filings_list_file: str = "data/filings_list.csv",
        download_dir: str = None
    ) -> None:
        """
        Download 10-K filings from a filings list.
        
        Args:
            filings_list_file (str): Path to the filings list CSV
            download_dir (str): Directory to save downloaded filings
        """
        if download_dir is None:
            download_dir = os.path.join(self.project_folder, "filings")
            
        logger.info(f"Downloading filings to {download_dir}")
        
        # Read filings list
        filings_df = pd.read_csv(filings_list_file)
        total_files = len(filings_df)
        downloaded = 0
        skipped = 0
        failed = 0
        
        with tqdm(total=total_files, desc="Downloading Filings") as pbar:
            for index, row in filings_df.iterrows():
                cik, form, date, url = row['CIK'], row['Form'], row['Date'], row['URL']
                year = date[:4]
                save_dir = f"{download_dir}/{cik}/{year}"
                os.makedirs(save_dir, exist_ok=True)
                file_path = f"{save_dir}/{form}_{date}.txt"
                
                # Check if file already exists and is not empty
                if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                    skipped += 1
                    if skipped % 1000 == 0:  # Log every 1000 skips
                        logger.debug(f"Skipped {skipped} files so far (e.g., {file_path})")
                    pbar.update(1)
                    continue
                
                # Download the file
                try:
                    response = requests.get(url, headers=self.headers)
                    if response.status_code == 200:
                        with open(file_path, 'wb') as f:
                            f.write(response.content)
                        downloaded += 1
                        pbar.set_description(
                            f"Downloaded: {downloaded}, Skipped: {skipped}, Failed: {failed}"
                        )
                    else:
                        failed += 1
                        logger.error(f"Failed to download {url} - Status: {response.status_code}")
                except Exception as e:
                    failed += 1
                    logger.error(f"Error downloading {url}: {e}")
                
                time.sleep(self.rate_limit_delay)  # Respect rate limit
                pbar.update(1)
        
        logger.info(
            f"Download complete. Summary: Downloaded: {downloaded}, "
            f"Skipped: {skipped}, Failed: {failed}"
        )