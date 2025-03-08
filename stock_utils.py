import pandas as pd
import yfinance as yf
from tqdm import tqdm
import os
import requests
import time


def generate_cik_ticker_mapping(
    filings_dir='data/edgar/filings',
    json_file='data/company_tickers_exchange.json',
    txt_file='data/ticker.txt',
    output_file='data/cik_ticker_mapping.csv'
):
    """
    Generates a CIK-to-ticker mapping using JSON and TXT files, with JSON as primary source and TXT as fallback.
    Example usage: generate_cik_ticker_mapping()
    Args:
        filings_dir (str): Directory containing CIK folders (default: 'data/filings').
        json_file (str): Path to company_tickers_exchange.json (default: 'data/company_tickers_exchange.json').
        txt_file (str): Path to ticker.txt (default: 'data/ticker.txt').
        output_file (str): Path to save the output CSV (default: 'data/cik_ticker_mapping.csv').

    Returns:
        None: Saves the mapping to the specified output_file.
    """
    # Step 1: Load CIK list from filings directory
    cik_folders = [folder for folder in os.listdir(filings_dir)
                   if os.path.isdir(os.path.join(filings_dir, folder))]
    my_ciks_df = pd.DataFrame(
        {"cik": [f"{int(cik):010d}" for cik in cik_folders]})

    # Step 2: Load JSON file
    with open(json_file, 'r') as f:
        json_data = json.load(f)
    tickers_list = json_data.get("data", [])
    if not tickers_list:
        raise ValueError("No 'data' key found in JSON file or data is empty.")

    # Step 3: Create DataFrame from JSON with correct column order
    json_df = pd.DataFrame(tickers_list, columns=[
                           "cik", "ticker", "name", "exchange"])
    json_df["cik"] = json_df["cik"].apply(
        lambda x: f"{int(x):010d}" if isinstance(x, int) else x.zfill(10))

    # Step 4: Load TXT file for fallback
    txt_df = pd.read_csv(txt_file, sep="\t", header=None,
                         names=["ticker", "cik"], dtype=str)
    txt_df["cik"] = txt_df["cik"].apply(lambda x: x.zfill(10))

    # Step 5: Merge and process with progress bar
    total_steps = 3  # Merging with JSON, handling not found, combining results
    with tqdm(total=total_steps, desc="Generating CIK-Ticker Mapping") as pbar:
        # Merge with JSON (primary source)
        merged_json = my_ciks_df.merge(json_df, on="cik", how="left")
        pbar.update(1)
        pbar.set_description("Merged with JSON data")

        # Handle CIKs not found in JSON
        not_found_ciks = merged_json[merged_json["ticker"].isna(
        )]["cik"].unique()
        not_found_df = pd.DataFrame({"cik": not_found_ciks})
        merged_txt = not_found_df.merge(txt_df, on="cik", how="left")
        merged_txt["name"] = None
        merged_txt["exchange"] = None
        pbar.update(1)
        pbar.set_description("Processed TXT fallback")

        # Combine results
        found = merged_json.dropna(subset=["ticker"])
        final_df = pd.concat([found, merged_txt], ignore_index=True)
        final_df["ticker"] = final_df["ticker"].fillna("Not Found")
        final_df = final_df[["cik", "name", "ticker", "exchange"]]
        pbar.update(1)
        pbar.set_description("Combined results")

    # Step 6: Save to CSV
    final_df.to_csv(output_file, index=False)
    print(f"Saved mapping for {len(final_df)} rows to {output_file}")


def update_ipo_dates(file_path='data/consolidated_stock_list.csv'):
    """
    Updates the consolidated stock list CSV with IPO dates for each ticker.
    Example usage: update_ipo_dates('data/consolidated_stock_list.csv')
    Args:
        file_path (str): Path to the consolidated_stock_list.csv file.
                         Default is 'data/consolidated_stock_list.csv'.

    Returns:
        None: Updates the CSV file in place.
    """
    # Step 1: Read the consolidated stock list
    stock_list = pd.read_csv(file_path)

    # Ensure 'ipo_date' column exists; if not, create it with NaN
    if 'ipo_date' not in stock_list.columns:
        stock_list['ipo_date'] = pd.NA

    # Step 2: Filter tickers to process
    # Skip tickers that are "Not Found" or already have a valid IPO date
    tickers_to_process = stock_list[
        (stock_list['ticker'] != "Not Found") &
        (stock_list['ipo_date'].isna() |
         (stock_list['ipo_date'] == "Not Found"))
    ]['ticker'].tolist()

    # Step 3: Function to get IPO date using yfinance
    def get_ipo_date(ticker):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="max")
            if not hist.empty:
                return hist.index[0].strftime('%Y-%m-%d')
            else:
                return "Not Found"
        except Exception:
            return "Not Found"

    # Step 4: Update IPO dates with progress bar
    if tickers_to_process:
        print(f"Processing {len(tickers_to_process)} tickers...")
        for ticker in tqdm(tickers_to_process, desc="Fetching IPO Dates"):
            ipo_date = get_ipo_date(ticker)
            # Update the 'ipo_date' column for this ticker
            stock_list.loc[stock_list['ticker']
                           == ticker, 'ipo_date'] = ipo_date

    # Step 5: Save the updated DataFrame back to the CSV
    stock_list.to_csv(file_path, index=False)
    print(f"Updated {file_path} with IPO dates.")


def download_daily_stock_prices(
    consolidated_file='data/consolidated_stock_list.csv',
    output_dir='data/daily_stock_prices',
    batch_size=100
):
    """
    Downloads and stores daily stock prices since IPO date for each ticker using yfinance.
    Example usage: download_daily_stock_prices()
    Args:
        consolidated_file (str): Path to consolidated_stock_list.csv with ticker and ipo_date.
        output_dir (str): Directory to store daily stock price CSV files (default: 'data/daily_stock_prices').
        batch_size (int): Number of tickers to process before saving progress (default: 100).

    Returns:
        None: Saves daily stock price data as individual CSV files in output_dir.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Load consolidated stock list
    df = pd.read_csv(consolidated_file)
    tickers_with_ipo = df[['ticker', 'ipo_date']].dropna(subset=['ipo_date'])
    tickers_with_ipo = tickers_with_ipo[tickers_with_ipo['ticker']
                                        != 'Not Found']

    # Filter out already processed tickers
    existing_files = {f.split('.')[0] for f in os.listdir(
        output_dir) if f.endswith('.csv')}
    tickers_to_process = tickers_with_ipo[~tickers_with_ipo['ticker'].isin(
        existing_files)]

    if not tickers_to_process.empty:
        print(f"Processing {len(tickers_to_process)} tickers...")
        batch = []

        with tqdm(total=len(tickers_to_process), desc="Downloading Daily Stock Prices") as pbar:
            for index, row in tickers_to_process.iterrows():
                ticker = row['ticker']
                ipo_date = row['ipo_date']

                # Skip if IPO date is invalid or "Not Found"
                if ipo_date == "Not Found" or pd.isna(ipo_date):
                    pbar.update(1)
                    continue

                # Download daily stock prices from IPO date to present
                try:
                    stock = yf.Ticker(ticker)
                    hist = stock.history(start=ipo_date)
                    if not hist.empty:
                        hist.reset_index(inplace=True)
                        hist['Date'] = hist['Date'].dt.strftime(
                            '%Y-%m-%d')  # Ensure consistent date format
                        batch.append((ticker, hist))
                        pbar.set_description(f"Downloaded {ticker}")
                    else:
                        pbar.write(f"No data for {ticker}")
                except Exception as e:
                    pbar.write(f"Error downloading {ticker}: {e}")

                pbar.update(1)

                # Save batch periodically
                if len(batch) >= batch_size or index == len(tickers_to_process) - 1:
                    for tkr, data in batch:
                        data.to_csv(os.path.join(
                            output_dir, f"{tkr}.csv"), index=False)
                    batch = []

        print(f"Daily stock prices saved to {output_dir}")
    else:
        print("No new tickers to process.")
