import pandas as pd
from tqdm import tqdm
import os
import requests
import time



def download_sec_master_files(start_year, end_year, project_folder='data/edgar'):
    """
    Downloads SEC EDGAR master.idx files and creates folder structure for specified years and quarters.
    Example usage: download_sec_master_files(1993, 2025, 'data/edgar')

    Args:
        start_year (int): Starting year (e.g., 1993).
        end_year (int): Ending year (e.g., 2025).
        project_folder (str): Directory where files will be stored (default: 'data/edgar').

    Returns:
        None: Downloads files and creates folder structure in the specified directory.
    """
    # Base URL for SEC EDGAR full-index archive
    base_url = "https://www.sec.gov/Archives/edgar/full-index/"

    # Headers to avoid potential blocking (replace with your info)
    headers = {
        # SEC requires a user-agent with contact info
        'User-Agent': 'WilliamFrank william_dieter@hotmail.com'
    }

    # Ensure the project directory exists
    os.makedirs(project_folder, exist_ok=True)

    quarters = ["QTR1", "QTR2", "QTR3", "QTR4"]

    # Calculate total number of tasks for progress bar
    total_tasks = 0
    for year in range(start_year, end_year + 1):
        if year == start_year or year == end_year:
            total_tasks += 1  # QTR1 only for end_year, full year for start_year might adjust
        else:
            total_tasks += 4  # Four quarters per full year
    if start_year == end_year:
        total_tasks = 1  # Special case: only one year, one quarter

    # Initialize progress bar
    with tqdm(total=total_tasks, desc="Downloading master.idx files") as pbar:
        for year in range(start_year, end_year + 1):
            year_dir = os.path.join(project_folder, str(year))
            os.makedirs(year_dir, exist_ok=True)  # Create year folder

            # Limit quarters based on start and end years
            if year == start_year:
                qtr_range = quarters  # Full year for start_year
            elif year == end_year:
                qtr_range = quarters[:1]  # Only QTR1 for end_year
            else:
                qtr_range = quarters

            for qtr in qtr_range:
                qtr_dir = os.path.join(year_dir, qtr)
                os.makedirs(qtr_dir, exist_ok=True)  # Create quarter folder

                # Construct URL for master.idx
                url = f"{base_url}{year}/{qtr}/master.idx"
                file_path = os.path.join(qtr_dir, "master.idx")

                # Download only if file doesn't already exist
                if not os.path.exists(file_path):
                    try:
                        response = requests.get(url, headers=headers)
                        if response.status_code == 200:
                            with open(file_path, 'wb') as f:
                                f.write(response.content)
                        else:
                            print(
                                f"Failed to download {year}/{qtr}/master.idx - Status: {response.status_code}")
                    except Exception as e:
                        print(
                            f"Error downloading {year}/{qtr}/master.idx: {e}")
                    # Respect SEC rate limits (10 requests/second max, so delay 0.1s)
                    time.sleep(0.1)
                # Update progress bar after each quarter
                pbar.update(1)
                pbar.set_description(f"Processing {year}/{qtr}")

    print(f"\nDownload complete. Files stored in {project_folder}")



def generate_and_download_filings(idx_dir='data/edgar', download_dir='data/edgar/filings', 
                                 output_file='data/filings_list.csv', base_url="https://www.sec.gov/Archives/"):
    """
    Generates a new filings_list.csv from master.idx files and downloads missing 10-K filings.
    Example usage: generate_and_download_filings()
    Args:
        idx_dir (str): Directory containing master.idx files (default: 'data/edgar').
        download_dir (str): Directory to save downloaded filings (default: 'data/edgar/filings').
        output_file (str): Path to save the filings_list.csv (default: 'data/filings_list.csv').
        base_url (str): Base URL for SEC EDGAR filings (default: 'https://www.sec.gov/Archives/').
    
    Returns:
        None: Creates filings_list.csv and downloads missing filings.
    """
    # Headers for SEC requests
    headers = {'User-Agent': 'WilliamFrank william_dieter@hotmail.com'}

    # Step 1: Generate a new filings list from master.idx files
    filings_list = []
    years = range(1994, 2026)
    quarters = ["QTR1", "QTR2", "QTR3", "QTR4"]

    # Calculate total tasks for progress bar (parsing phase)
    total_idx_files = sum(1 for year in years for qtr in quarters 
                         if os.path.exists(f"{idx_dir}/{year}/{qtr}/master.idx"))

    with tqdm(total=total_idx_files, desc="Parsing master.idx files") as pbar:
        for year in years:
            for qtr in quarters:
                idx_file = f"{idx_dir}/{year}/{qtr}/master.idx"
                if os.path.exists(idx_file):
                    with open(idx_file, 'r') as f:
                        lines = f.readlines()[11:]  # Skip header
                    for line in lines:
                        parts = line.strip().split('|')
                        if len(parts) >= 5 and parts[2] in ['10-K']:
                            filings_list.append({
                                'CIK': parts[0],
                                'Company': parts[1],
                                'Form': parts[2],
                                'Date': parts[3],
                                'URL': base_url + parts[4]
                            })
                    pbar.update(1)
                    pbar.set_description(f"Parsing {year}/{qtr}")
                # No else clause: silently skip missing idx files

    # Create a new DataFrame and save to CSV
    filings_df = pd.DataFrame(filings_list)
    filings_df.to_csv(output_file, index=False)
    print(f"Generated new {output_file} with {len(filings_df)} entries.")

    # Step 2: Download missing filings
    total_files = len(filings_df)

    with tqdm(filings_df.iterrows(), total=total_files, desc="Downloading Filings") as pbar:
        for index, row in pbar:
            cik, form, date, url = row['CIK'], row['Form'], row['Date'], row['URL']
            year = date[:4]
            save_dir = f"{download_dir}/{cik}/{year}"
            os.makedirs(save_dir, exist_ok=True)
            file_path = f"{save_dir}/{form}_{date}.txt"
            
            # Check if file already exists and is not empty
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                # Log skips sparingly to reduce output
                if index % 100 == 0:
                    pbar.write(f"Skipped {file_path} - already exists")
                pbar.update(1)
                continue
            
            # Download the file
            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    with open(file_path, 'wb') as f:
                        f.write(response.content)
                    pbar.set_description(f"Downloaded {form}_{date}")
                else:
                    pbar.write(f"Failed to download {url} - Status: {response.status_code}")
            except Exception as e:
                pbar.write(f"Error downloading {url}: {e}")
            
            time.sleep(0.1)  # Respect SEC rate limit (10 requests/second)
            pbar.update(1)

    print(f"\nDownload complete. Files stored in {download_dir}")   