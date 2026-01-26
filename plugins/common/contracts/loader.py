"""
Contract loader to read and parse YAML data contracts.
"""
import os
import logging
from typing import Dict, Any
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)


class ContractLoader:
    """Load and cache data contracts from YAML files."""
    
    def __init__(self, contracts_dir: str = None):
        """
        Initialize contract loader.
        
        Args:
            contracts_dir: Directory containing contract YAML files
        """
        if contracts_dir is None:
            current_dir = Path(__file__).parent
            contracts_dir = current_dir
        
        self.contracts_dir = Path(contracts_dir)
        self._cache = {}
    
    def load(self, source: str, resource: str) -> Dict[str, Any]:
        """
        Load contract for a specific source and resource.
        
        Args:
            source: Source name (e.g., 'coingecko', 'massive')
            resource: Resource name (e.g., 'btc_usd_daily', 'stocks_daily')
            
        Returns:
            Parsed contract as dict
            
        Raises:
            FileNotFoundError: If contract file doesn't exist
            yaml.YAMLError: If contract YAML is invalid
        """
        cache_key = f"{source}/{resource}"
        
        if cache_key in self._cache:
            logger.debug(f"Loading contract from cache: {cache_key}")
            return self._cache[cache_key]
        
        contract_path = self.contracts_dir / source / f"{resource}.yaml"
        
        if not contract_path.exists():
            raise FileNotFoundError(f"Contract not found: {contract_path}")
        
        logger.info(f"Loading contract from {contract_path}")
        
        with open(contract_path, 'r') as f:
            contract = yaml.safe_load(f)
        
        self._validate_contract(contract, cache_key)
        
        self._cache[cache_key] = contract
        return contract
    
    def _validate_contract(self, contract: Dict[str, Any], cache_key: str) -> None:
        """
        Basic validation of contract structure.
        
        Args:
            contract: Parsed contract
            cache_key: Contract identifier for error messages
            
        Raises:
            ValueError: If contract is invalid
        """
        required_keys = ['source', 'resource']
        for key in required_keys:
            if key not in contract:
                raise ValueError(f"Contract {cache_key} missing required key: {key}")
        
        source = contract['source']
        if 'name' not in source:
            raise ValueError(f"Contract {cache_key} missing source.name")
        
        resource = contract['resource']
        if 'name' not in resource:
            raise ValueError(f"Contract {cache_key} missing resource.name")
        
        if 'extract' not in resource:
            raise ValueError(f"Contract {cache_key} missing resource.extract")
        
        logger.debug(f"Contract {cache_key} validated successfully")
