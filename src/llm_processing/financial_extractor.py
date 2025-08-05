"""
LLM-based financial data extractor for the Stock Selector project.
"""
import os
import json
import re
from typing import Dict, List, Tuple, Optional
from bs4 import BeautifulSoup
import openai
from src.utils.config import config
from src.utils.logger import setup_logger

logger = setup_logger(__name__, "logs/financial_extractor.log")


class FinancialDataExtractor:
    """Extracts financial data from SEC filings using LLMs."""
    
    def __init__(self):
        """Initialize the extractor with configuration."""
        llm_config = config.get_llm_config()
        self.model = llm_config.get("model", "gpt-3.5-turbo")
        self.api_key = llm_config.get("api_key")
        self.cache_responses = llm_config.get("cache_responses", True)
        self.cache_dir = llm_config.get("cache_dir", "data/llm_cache")
        
        # Set up OpenAI API key
        if self.api_key:
            openai.api_key = self.api_key
        
        # Create cache directory
        if self.cache_responses:
            os.makedirs(self.cache_dir, exist_ok=True)
        
        # Precompile regex for us-gaap tags
        self.us_gaap_regex = re.compile(r'us-gaap:.*', re.I)
    
    def _get_cache_path(self, cik: str, year: str) -> str:
        """
        Get cache file path for a CIK and year.
        
        Args:
            cik (str): CIK identifier
            year (str): Fiscal year
            
        Returns:
            str: Cache file path
        """
        return os.path.join(self.cache_dir, f"{cik}_{year}.json")
    
    def _load_from_cache(self, cik: str, year: str) -> Optional[Dict]:
        """
        Load cached response for a CIK and year.
        
        Args:
            cik (str): CIK identifier
            year (str): Fiscal year
            
        Returns:
            Optional[Dict]: Cached response or None if not found
        """
        if not self.cache_responses:
            return None
            
        cache_path = self._get_cache_path(cik, year)
        if os.path.exists(cache_path):
            try:
                with open(cache_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Error loading cache for {cik}_{year}: {e}")
        return None
    
    def _save_to_cache(self, cik: str, year: str, data: Dict) -> None:
        """
        Save response to cache.
        
        Args:
            cik (str): CIK identifier
            year (str): Fiscal year
            data (Dict): Data to cache
        """
        if not self.cache_responses:
            return
            
        cache_path = self._get_cache_path(cik, year)
        try:
            with open(cache_path, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning(f"Error saving cache for {cik}_{year}: {e}")
    
    def _extract_financial_metrics_with_llm(self, filing_text: str) -> Dict[str, float]:
        """
        Extract financial metrics from filing text using LLM.
        
        Args:
            filing_text (str): Text content of the filing
            
        Returns:
            Dict[str, float]: Extracted financial metrics
        """
        if not self.api_key:
            logger.warning("No API key configured. Returning empty metrics.")
            return {}
        
        # Create prompt for LLM
        prompt = f"""
        Extract the following financial metrics from the 10-K filing text below:
        
        Required metrics:
        1. NetIncomeLoss
        2. EarningsPerShareBasic
        3. DebtCurrent
        4. LongTermDebt
        5. CashAndCashEquivalentsAtCarryingValue
        6. OperatingIncomeLoss
        7. StockholdersEquity
        8. Revenues
        9. IncomeTaxExpenseBenefit
        10. IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest
        11. DepreciationDepletionAndAmortization
        12. EarningsBeforeInterestTaxesDepreciationAmortizationEBITDA
        
        Filing text:
        {filing_text[:4000]}  # Limit to first 4000 characters to avoid token limits
        
        Please return the results in JSON format with metric names as keys and numeric values as values.
        If a metric cannot be found, omit it from the response.
        """
        
        try:
            response = openai.ChatCompletion.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a financial analyst expert at extracting financial metrics from SEC filings."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.0,  # Use low temperature for consistent extraction
                max_tokens=1000
            )
            
            # Extract JSON from response
            content = response.choices[0].message.content
            # Try to find JSON in the response
            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            if json_start != -1 and json_end > json_start:
                json_str = content[json_start:json_end]
                return json.loads(json_str)
            else:
                logger.warning("Could not extract JSON from LLM response")
                return {}
                
        except Exception as e:
            logger.error(f"Error extracting financial metrics with LLM: {e}")
            return {}
    
    def extract_from_filing(self, cik: str, year: str, filing_path: str) -> Dict[str, float]:
        """
        Extract financial metrics from a filing.
        
        Args:
            cik (str): CIK identifier
            year (str): Fiscal year
            filing_path (str): Path to the filing file
            
        Returns:
            Dict[str, float]: Extracted financial metrics
        """
        # Check cache first
        cached_data = self._load_from_cache(cik, year)
        if cached_data is not None:
            logger.info(f"Loaded cached data for {cik}_{year}")
            return cached_data
        
        logger.info(f"Extracting financial metrics for {cik}_{year}")
        
        # Read filing content
        try:
            with open(filing_path, 'r', encoding='utf-8') as file:
                content = file.read()
        except Exception as e:
            logger.error(f"Error reading {filing_path}: {e}")
            return {}
        
        # Try to extract with LLM
        metrics = self._extract_financial_metrics_with_llm(content)
        
        # Save to cache
        self._save_to_cache(cik, year, metrics)
        
        return metrics
    
    def extract_from_xbrl(self, cik: str, year: str, filing_path: str) -> Dict[str, float]:
        """
        Extract financial metrics from XBRL data in a filing.
        
        Args:
            cik (str): CIK identifier
            year (str): Fiscal year
            filing_path (str): Path to the filing file
            
        Returns:
            Dict[str, float]: Extracted financial metrics
        """
        logger.info(f"Extracting XBRL data for {cik}_{year}")
        
        # Read filing content
        try:
            with open(filing_path, 'r', encoding='utf-8') as file:
                content = file.read()
        except Exception as e:
            logger.error(f"Error reading {filing_path}: {e}")
            return {}
        
        # Quick check for XBRL content
        if '<context' not in content.lower():
            return {}
        
        soup = BeautifulSoup(content, 'lxml')
        context_elements = soup.find_all('context')
        if not context_elements:
            return {}
        
        # Build context-to-year mapping
        context_to_year = {}
        for context in context_elements:
            context_id = context.get('id')
            period = context.find('period')
            if period:
                # Try to extract year from period
                for tag in ['instant', 'endDate', 'startDate']:
                    date_tag = period.find(tag)
                    if date_tag:
                        date_str = date_tag.text.strip()
                        # Extract year from date string (format: YYYY-MM-DD)
                        if len(date_str) >= 4:
                            try:
                                context_to_year[context_id] = int(date_str[:4])
                            except ValueError:
                                continue
        
        if not context_to_year:
            return {}
        
        # Extract metrics
        metrics = {}
        for element in soup.find_all(self.us_gaap_regex):
            try:
                value = float(element.text.strip())
            except ValueError:
                continue
            context_ref = element.get('contextref')
            if context_ref in context_to_year and context_to_year[context_ref] == int(year):
                tag = element.name.lower()
                metrics[tag] = value
        
        return metrics
    
    def validate_and_combine(
        self, 
        xbrl_metrics: Dict[str, float], 
        llm_metrics: Dict[str, float]
    ) -> Dict[str, float]:
        """
        Validate and combine XBRL and LLM extracted metrics.
        
        Args:
            xbrl_metrics (Dict[str, float]): Metrics extracted from XBRL
            llm_metrics (Dict[str, float]): Metrics extracted with LLM
            
        Returns:
            Dict[str, float]: Combined and validated metrics
        """
        # Prefer XBRL data when available as it's more structured
        combined = xbrl_metrics.copy()
        
        # Add LLM metrics that aren't in XBRL data
        for key, value in llm_metrics.items():
            if key not in combined:
                combined[key] = value
        
        return combined