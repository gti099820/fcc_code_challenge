# src/data_loader.py
import pandas as pd
from src.config import RAW_DATA_DIR, SOURCE_FILES

class DataLoader:
    """Handles loading of all raw source data files."""

    def __init__(self):
        self.raw_data_path = RAW_DATA_DIR
        self.source_files = SOURCE_FILES
        print("DataLoader initialized.")

    def load_source_file(self, source_name: str):
        """Loads a single CSV file into a DataFrame."""
        try:
            file_name = self.source_files[source_name]
            file_path = self.raw_data_path / file_name
            df = pd.read_csv(file_path)
            print(f"  Successfully loaded {file_name}")
            return df
        except FileNotFoundError:
            print(f"  ERROR: File not found at {file_path}")
            return pd.DataFrame()
        except Exception as e:
            print(f"  ERROR: Could not load {file_name}. Reason: {e}")
            return pd.DataFrame()

    def load_all_sources(self) -> dict:
        """
        Loads all source files defined in config into a dictionary of DataFrames.
        
        Returns:
            dict: A dictionary where keys are source names (e.g., 'user') 
                  and values are their DataFrames.
        """
        print("Loading all raw data sources...")
        raw_data = {}
        for source_name in self.source_files.keys():
            raw_data[source_name] = self.load_source_file(source_name)
        
        print("All raw data loaded.")
        return raw_data