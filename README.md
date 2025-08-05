# Stock Selector

A comprehensive stock selection system that identifies high-quality stocks and REITs based on fundamental and performance criteria.

## Features

- Data acquisition from SEC EDGAR database (10-K filings)
- Financial data parsing with LLM enhancement
- Stock screening based on quality criteria
- Performance analysis against benchmarks
- Visualization of results
- Efficient storage management

## Installation

1. Install [UV](https://github.com/astral-sh/uv) (recommended Python package installer)
2. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/Stock-Selector.git
   cd Stock-Selector
   ```
3. Create and activate virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   .venv\Scripts\activate     # Windows
   ```
4. Install dependencies using UV:
   ```bash
   uv pip install -r requirements.txt
   ```

## Configuration

Edit `config/config.yaml` to customize:
- Database connection settings
- API keys
- Screening criteria
- Storage management settings

## Usage

```bash
# Download SEC filings
python main.py download-data

# Process filings and extract metrics
python main.py process-data

# Screen stocks based on criteria
python main.py screen-stocks

# Generate reports and visualizations
python main.py generate-report

# Clean up old data
python main.py cleanup-data
```

## Project Structure

```
stock_selector/
├── config/                 # Configuration files
├── data/                   # Data storage
├── src/                    # Source code
│   ├── data_acquisition/   # SEC data download
│   ├── llm_processing/     # LLM-enhanced processing
│   ├── analysis/           # Stock screening
│   ├── visualization/      # Reporting
│   └── utils/              # Shared utilities
├── tests/                  # Unit tests
├── docs/                   # Documentation
├── main.py                 # CLI entry point
└── README.md               # Project overview
```

## Data Flow

1. Download SEC filings
2. Process with LLM/XBRL
3. Store in database
4. Run analysis
5. Generate reports