"""
Configuration management for the Stock Selector project.
"""
import yaml
import os
from typing import Dict, Any


class Config:
    """Configuration manager for the Stock Selector project."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize the configuration manager.
        
        Args:
            config_path (str): Path to the configuration file
        """
        self.config_path = config_path
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Returns:
            Dict[str, Any]: Configuration dictionary
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation.
        
        Args:
            key_path (str): Dot-separated path to the configuration value
            default (Any): Default value if key is not found
            
        Returns:
            Any: Configuration value
        """
        keys = key_path.split('.')
        value = self._config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_database_config(self) -> Dict[str, str]:
        """
        Get database configuration.
        
        Returns:
            Dict[str, str]: Database configuration
        """
        return self._config.get('database', {})
    
    def get_sec_edgar_config(self) -> Dict[str, Any]:
        """
        Get SEC EDGAR configuration.
        
        Returns:
            Dict[str, Any]: SEC EDGAR configuration
        """
        return self._config.get('sec_edgar', {})
    
    def get_llm_config(self) -> Dict[str, Any]:
        """
        Get LLM configuration.
        
        Returns:
            Dict[str, Any]: LLM configuration
        """
        return self._config.get('llm', {})
    
    def get_storage_config(self) -> Dict[str, Any]:
        """
        Get storage configuration.
        
        Returns:
            Dict[str, Any]: Storage configuration
        """
        return self._config.get('storage', {})
    
    def get_screening_config(self) -> Dict[str, Dict[str, float]]:
        """
        Get stock screening configuration.
        
        Returns:
            Dict[str, Dict[str, float]]: Screening configuration for stocks and REITs
        """
        return self._config.get('screening', {})
    
    def get_performance_config(self) -> Dict[str, Any]:
        """
        Get performance analysis configuration.
        
        Returns:
            Dict[str, Any]: Performance analysis configuration
        """
        return self._config.get('performance', {})


# Global configuration instance
config = Config()