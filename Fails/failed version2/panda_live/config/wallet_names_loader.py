"""
PANDA LIVE Wallet Names Loader

Loads wallet address to name mappings from JSON file.
"""

import json
from pathlib import Path
from typing import Dict, Optional


class WalletNamesLoader:
    """
    Loads and manages wallet address to name mappings.
    
    Expected JSON format:
    {
        "FULL_WALLET_ADDRESS": "WalletName",
        "FULL_MINT_ADDRESS": "TokenName"
    }
    """
    
    def __init__(self, json_path: Optional[str] = None):
        self.names: Dict[str, str] = {}
        
        if json_path:
            self.load_from_file(json_path)
    
    def load_from_file(self, json_path: str) -> int:
        """
        Load wallet names from JSON file.
        
        Args:
            json_path: Path to JSON file
        
        Returns:
            Number of names loaded
        """
        path = Path(json_path)
        
        if not path.exists():
            print(f"Warning: Wallet names file not found: {json_path}")
            return 0
        
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            
            if not isinstance(data, dict):
                print(f"Warning: Invalid wallet names format in {json_path}")
                return 0
            
            self.names = data
            print(f"Loaded {len(self.names)} wallet names from {json_path}")
            return len(self.names)
        
        except json.JSONDecodeError as e:
            print(f"Error parsing wallet names JSON: {e}")
            return 0
        except Exception as e:
            print(f"Error loading wallet names: {e}")
            return 0
    
    def get_name(self, address: str) -> Optional[str]:
        """
        Get name for an address.
        
        Args:
            address: Full wallet or mint address
        
        Returns:
            Name if found, None otherwise
        """
        return self.names.get(address)
    
    def format_wallet_display(self, address: str) -> str:
        """
        Format wallet address for display with optional name.
        
        Args:
            address: Full wallet address
        
        Returns:
            Formatted string: "abc...xyz (Name)" or "abc...xyz"
        """
        short = f"{address[:4]}...{address[-4:]}"
        name = self.get_name(address)
        
        if name:
            return f"{short} ({name})"
        else:
            return short
    
    def add_name(self, address: str, name: str):
        """
        Add a name mapping programmatically.
        
        Args:
            address: Full address
            name: Name to associate
        """
        self.names[address] = name
    
    def merge_from_dict(self, names: Dict[str, str]):
        """
        Merge names from another dictionary.
        
        Args:
            names: Dictionary of address -> name mappings
        """
        self.names.update(names)
    
    def save_to_file(self, json_path: str):
        """
        Save current names to JSON file.
        
        Args:
            json_path: Path to output JSON file
        """
        path = Path(json_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'w') as f:
            json.dump(self.names, f, indent=2, sort_keys=True)
        
        print(f"Saved {len(self.names)} wallet names to {json_path}")
