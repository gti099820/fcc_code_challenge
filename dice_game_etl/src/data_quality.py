# src/data_quality.py
import pandas as pd

class DataQualityValidator:
    """Performs DQ checks on DataFrames."""

    def __init__(self):
        self.results = {"passed": [], "failed": []}
        print("DataQualityValidator initialized.")

    def check_uniqueness(self, df: pd.DataFrame, columns: list, table_name: str) -> bool:
        """Checks if specified columns are a unique key."""
        if df.empty:
            return True # Skip check on empty df
        is_unique = not df.duplicated(subset=columns).any()
        test_name = f"DQ_UNIQUE: {table_name} on {columns}"
        
        if is_unique:
            self.results["passed"].append(test_name)
            return True
        else:
            self.results["failed"].append(test_name)
            print(f"  FAILED: {test_name} - Duplicate values found.")
            return False

    def check_nulls(self, df: pd.DataFrame, columns: list, table_name: str) -> bool:
        """Checks for any NULL values in specified columns."""
        if df.empty:
            return True # Skip check on empty df
        has_nulls = df[columns].isnull().any().any()
        test_name = f"DQ_NULL: {table_name} on {columns}"

        if not has_nulls:
            self.results["passed"].append(test_name)
            return True
        else:
            self.results["failed"].append(test_name)
            print(f"  FAILED: {test_name} - NULL values found.")
            return False

    def check_referential_integrity(self, parent_df: pd.DataFrame, child_df: pd.DataFrame, 
                                      parent_key: str, child_key: str, relationship_name: str) -> bool:
        """Checks if all child keys exist in the parent table."""
        if child_df.empty or parent_df.empty:
            return True # Skip check
            
        parent_keys = set(parent_df[parent_key].unique())
        child_keys = set(child_df[child_key].unique())
        
        orphan_keys = child_keys - parent_keys
        test_name = f"DQ_REF_INTEGRITY: {relationship_name}"

        if not orphan_keys:
            self.results["passed"].append(test_name)
            return True
        else:
            self.results["failed"].append(test_name)
            print(f"  FAILED: {test_name} - Orphan keys found: {orphan_keys}")
            return False

    def print_summary(self):
        print("\n--- Data Quality Check Summary ---")
        print(f"Total Passed: {len(self.results['passed'])}")
        print(f"Total Failed: {len(self.results['failed'])}")
        for failure in self.results["failed"]:
            print(f"  - {failure}")
        print("----------------------------------\n")