# src/transformations.py
import pandas as pd
from src.config import (
    DIM_DIR, 
    FACT_DIR, 
    OUTPUT_FORMAT, 
    DATE_DIM_START, 
    DATE_DIM_END
)

class StarSchemaBuilder:
    """
    Transforms raw DataFrames into a star schema (Dimensions and Facts).
    """
    def __init__(self, raw_data: dict):
        self.raw_data = raw_data
        self.dimensions = {}
        self.facts = {}
        print("StarSchemaBuilder initialized.")

    def _save_output(self, df: pd.DataFrame, dir: Path, name: str):
        """Helper to save DataFrame to the specified format (Parquet or CSV)."""
        dir.mkdir(parents=True, exist_ok=True)
        file_path = dir / f"{name}.{OUTPUT_FORMAT}"
        
        try:
            if OUTPUT_FORMAT == "parquet":
                df.to_parquet(file_path, index=False)
            else:
                df.to_csv(file_path, index=False)
            print(f"  Saved {name}.{OUTPUT_FORMAT} to {dir}")
        except Exception as e:
            print(f"  ERROR saving {name}: {e}")

    def create_dimensions(self):
        """Orchestrator method to create all dimensions."""
        print("Creating dimensions...")
        self._create_dim_date()
        self._create_dim_channel()
        self._create_dim_status()
        self._create_dim_plan()
        self._create_dim_payment_method()
        self._create_dim_user()
        print("All dimensions created.")
        return self.dimensions

    def _create_dim_date(self):
        df = pd.DataFrame(
            {"date": pd.date_range(start=DATE_DIM_START, end=DATE_DIM_END)}
        )
        df["date_key"] = df["date"].dt.strftime("%Y%m%d").astype(int)
        df["full_date"] = df["date"].dt.date
        df["year"] = df["date"].dt.year
        df["quarter"] = df["date"].dt.quarter
        df["month"] = df["date"].dt.month
        df["month_name"] = df["date"].dt.strftime("%B")
        df["day"] = df["date"].dt.day
        df["day_of_week"] = df["date"].dt.day_name()
        
        self.dimensions["dim_date"] = df
        self._save_output(df, DIM_DIR, "dim_date")

    def _create_dim_channel(self):
        df = self.raw_data["channel"].copy()
        df["channel_key"] = range(1, len(df) + 1)
        self.dimensions["dim_channel"] = df
        self._save_output(df, DIM_DIR, "dim_channel")

    def _create_dim_status(self):
        df = self.raw_data["status"].copy()
        df["status_key"] = range(1, len(df) + 1)
        self.dimensions["dim_status"] = df
        self._save_output(df, DIM_DIR, "dim_status")
        
    def _create_dim_payment_method(self):
        df = self.raw_data["payment_detail"].copy()
        df["payment_detail_key"] = range(1, len(df) + 1)
        self.dimensions["dim_payment_method"] = df
        self._save_output(df, DIM_DIR, "dim_payment_method")

    def _create_dim_plan(self):
        # Join plan with its frequency description
        df_plan = self.raw_data["plan"].copy()
        df_freq = self.raw_data["payment_frequency"].copy()
        
        df = pd.merge(
            df_plan,
            df_freq,
            on="payment_frequency_code",
            how="left"
        )
        df["plan_key"] = range(1, len(df) + 1)
        self.dimensions["dim_plan"] = df
        self._save_output(df, DIM_DIR, "dim_plan")

    def _create_dim_user(self):
        # Join user account info with user registration profile
        df_user = self.raw_data["user"].copy()
        df_reg = self.raw_data["registration"].copy()
        
        df = pd.merge(
            df_user,
            df_reg,
            on="user_id",
            how="left",
            suffixes=("_account", "_profile")
        )
        df["user_key"] = range(1, len(df) + 1)
        self.dimensions["dim_user"] = df
        self._save_output(df, DIM_DIR, "dim_user")

    def create_facts(self):
        """Orchestrator method to create all fact tables."""
        if not self.dimensions:
            print("ERROR: Dimensions must be created before facts.")
            return

        print("Creating fact tables...")
        self._create_fact_play_session()
        self._create_fact_subscription()
        print("All fact tables created.")
        return self.facts

    def _create_fact_play_session(self):
        df = self.raw_data["play_session"].copy()
        
        # Add surrogate keys from dimensions
        df = pd.merge(df, self.dimensions["dim_user"][["user_id", "user_key"]], on="user_id", how="left")
        df = pd.merge(df, self.dimensions["dim_channel"][["channel_code", "channel_key"]], on="channel_code", how="left")
        df = pd.merge(df, self.dimensions["dim_status"][["play_session_status_code", "status_key"]], 
                      left_on="status_code", right_on="play_session_status_code", how="left")
        
        # Date/Time transformations
        df["start_datetime"] = pd.to_datetime(df["start_datetime"])
        df["end_datetime"] = pd.to_datetime(df["end_datetime"])
        
        # Create Date Keys for joining to DimDate
        df["start_date_key"] = df["start_datetime"].dt.strftime("%Y%m%d").astype(int)
        df["end_date_key"] = df["end_datetime"].dt.strftime("%Y%m%d").astype(int)
        
        # Calculate new measure: duration
        df["duration_minutes"] = (df["end_datetime"] - df["start_datetime"]).dt.total_seconds() / 60
        
        # Select and rename columns for the final fact table
        fact_df = df[[
            "play_session_id", "user_key", "channel_key", "status_key",
            "start_date_key", "end_date_key", "total_score", "duration_minutes"
        ]]
        
        self.facts["fact_play_session"] = fact_df
        self._save_output(fact_df, FACT_DIR, "fact_play_session")

    def _create_fact_subscription(self):
        df = self.raw_data["user_plan"].copy()
        
        # Get user_key
        df = pd.merge(df, self.dimensions["dim_user"][["user_registration_id", "user_key"]], 
                      on="user_registration_id", how="left")
        
        # Get plan_key and cost_amount
        df = pd.merge(df, self.dimensions["dim_plan"][["plan_id", "plan_key", "cost_amount"]], 
                      on="plan_id", how="left")
                      
        # Get payment_detail_key
        df = pd.merge(df, self.dimensions["dim_payment_method"][["payment_detail_id", "payment_detail_key"]],
                      on="payment_detail_id", how="left")

        # Date/Time transformations
        df["start_date"] = pd.to_datetime(df["start_date"], utc=True)
        df["end_date"] = pd.to_datetime(df["end_date"], utc=True)

        # Create Date Keys
        df["start_date_key"] = df["start_date"].dt.strftime("%Y%m%d").astype(int)
        df["end_date_key"] = df["end_date"].dt.strftime("%Y%m%d").astype(int)
        
        # Create new measure: is_active
        df["is_active"] = df["end_date"] > pd.Timestamp.now(tz='utc')
        
        # Select and rename
        fact_df = df[[
            "user_key", "plan_key", "payment_detail_key", "start_date_key",
            "end_date_key", "cost_amount", "is_active"
        ]]
        
        self.facts["fact_subscription"] = fact_df
        self._save_output(fact_df, FACT_DIR, "fact_subscription")