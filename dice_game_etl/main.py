# main.py
import sys
from src.data_loader import DataLoader
from src.data_quality import DataQualityValidator
from src.transformations import StarSchemaBuilder
from src.insights import InsightGenerator
from src.config import BASE_DIR

def run_pipeline():
    """Main function to orchestrate the ETL and analysis pipeline."""
    
    print("Starting Dice Game ETL Pipeline...")
    
    # 1. Load Data
    loader = DataLoader()
    raw_data = loader.load_all_sources()
    
    # 2. Data Quality Checks (on raw data)
    dq = DataQualityValidator()
    dq.check_uniqueness(raw_data["user"], ["user_id"], "user")
    dq.check_uniqueness(raw_data["registration"], ["user_registration_id"], "registration")
    dq.check_uniqueness(raw_data["plan"], ["plan_id"], "plan")
    dq.check_referential_integrity(raw_data["user"], raw_data["registration"], "user_id", "user_id", "user->registration")
    dq.check_referential_integrity(raw_data["user"], raw_data["play_session"], "user_id", "user_id", "user->play_session")
    dq.check_referential_integrity(raw_data["plan"], raw_data["user_plan"], "plan_id", "plan_id", "plan->user_plan")
    
    dq.print_summary()
    if len(dq.results["failed"]) > 0:
        print("Critical data quality checks failed. Aborting pipeline.")
        sys.exit(1)
        
    print("Data quality checks passed.")

    # 3. Transformations (Build Star Schema)
    builder = StarSchemaBuilder(raw_data)
    builder.create_dimensions()
    builder.create_facts()
    
    print("ETL transformation complete. Data warehouse built.")

    # 4. Generate Insights
    analyzer = InsightGenerator()
    analyzer.generate_all_insights()
    
    print("Dice Game ETL Pipeline finished successfully.")

if __name__ == "__main__":
    # Ensure project root is in path for imports
    sys.path.append(str(BASE_DIR))
    run_pipeline()