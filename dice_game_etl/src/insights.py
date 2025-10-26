# src/insights.py
import pandas as pd
from src.config import DIM_DIR, FACT_DIR, OUTPUT_FORMAT

class InsightGenerator:
    """
    Generates key insights from the processed data warehouse.
    """
    def __init__(self):
        self.dim_path = DIM_DIR
        self.fact_path = FACT_DIR
        self.report_content = []
        print("InsightGenerator initialized.")

    def _load_data(self, name: str, is_fact: bool = False):
        """Helper to load processed data."""
        dir = self.fact_path if is_fact else self.dim_path
        file_path = dir / f"{name}.{OUTPUT_FORMAT}"
        try:
            if OUTPUT_FORMAT == "parquet":
                return pd.read_parquet(file_path)
            else:
                return pd.read_csv(file_path)
        except Exception as e:
            print(f"  ERROR loading processed file {name}: {e}")
            return pd.DataFrame()

    def generate_all_insights(self):
        """Orchestrates all insight generation."""
        print("Generating insights...")
        self.report_content.append("# 2024 Dice Game Analysis Report\n")
        
        self._get_insight_1()
        self._get_insight_2()
        self._get_insight_3()
        self._get_insight_4_session_outcomes()
        self._get_insight_5_payment_methods()
        self._get_insight_6_top_users()
        self._get_insight_7_monthly_revenue()
        self._get_insight_8_avg_duration()
        
        print("Insights generated.")
        return self._save_report()

    def _get_insight_1(self):
        """[cite: 14] How many play sessions took place Online vs on the Mobile App?"""
        fact_play = self._load_data("fact_play_session", is_fact=True)
        dim_channel = self._load_data("dim_channel")
        
        if fact_play.empty or dim_channel.empty:
            return

        merged = pd.merge(fact_play, dim_channel, on="channel_key")
        result = merged.groupby("english_description")["play_session_id"].count().reset_index()
        result = result.rename(columns={"english_description": "Channel", "play_session_id": "Total Sessions"})
        
        self.report_content.append("## Insight 1: Play Sessions by Channel\n")
        self.report_content.append(result.to_markdown(index=False))
        self.report_content.append("\n")

    def _get_insight_2(self):
        """[cite: 15] How many registered users opted for a onetime payment vs a subscription?"""
        fact_sub = self._load_data("fact_subscription", is_fact=True)
        dim_plan = self._load_data("dim_plan")

        if fact_sub.empty or dim_plan.empty:
            return

        merged = pd.merge(fact_sub, dim_plan, on="plan_key")
        
        # We count *distinct users* per plan type
        result = merged.groupby("english_description")["user_key"].nunique().reset_index()
        result = result.rename(columns={"english_description": "Plan Type", "user_key": "Unique Users"})

        self.report_content.append("## Insight 2: Unique Users by Plan Type\n")
        self.report_content.append(result.to_markdown(index=False))
        self.report_content.append("\n")

    def _get_insight_3(self):
        """[cite: 16] How much gross revenue was generated from the app?"""
        fact_sub = self._load_data("fact_subscription", is_fact=True)
        dim_plan = self._load_data("dim_plan")
        
        if fact_sub.empty:
            return

        total_revenue = fact_sub["cost_amount"].sum()
        
        merged = pd.merge(fact_sub, dim_plan, on="plan_key")
        revenue_by_plan = merged.groupby("english_description")["cost_amount"].sum().reset_index()
        revenue_by_plan = revenue_by_plan.rename(columns={"english_description": "Plan Type", "cost_amount": "Total Revenue"})
        
        self.report_content.append("## Insight 3: Gross Revenue\n")
        self.report_content.append(f"**Total Gross Revenue (2024): ${total_revenue:,.2f}**\n")
        self.report_content.append("\n### Revenue Breakdown by Plan Type\n")
        self.report_content.append(revenue_by_plan.to_markdown(index=False))
        self.report_content.append("\n")

    def _get_insight_4_session_outcomes(self):
        """Insight 4: What are the outcomes of all play sessions?"""
        fact_play = self._load_data("fact_play_session", is_fact=True)
        dim_status = self._load_data("dim_status")

        if fact_play.empty or dim_status.empty:
            return

        merged = pd.merge(fact_play, dim_status, on="status_key")
        result = merged.groupby("english_description")["play_session_id"].count().reset_index()
        result = result.rename(columns={"english_description": "Session Outcome", "play_session_id": "Total Sessions"})
        result = result.sort_values(by="Total Sessions", ascending=False)
        
        self.report_content.append("## Insight 4: Play Session Outcomes\n")
        self.report_content.append("This shows the final status of all games played, indicating user engagement or potential issues (like timeouts or aborts).\n")
        self.report_content.append(result.to_markdown(index=False))
        self.report_content.append("\n")

    def _get_insight_5_payment_methods(self):
        """Insight 5: What are the most popular payment methods?"""
        fact_sub = self._load_data("fact_subscription", is_fact=True)
        dim_payment = self._load_data("dim_payment_method")
        
        if fact_sub.empty or dim_payment.empty:
            return
            
        merged = pd.merge(fact_sub, dim_payment, on="payment_detail_key")
        
        # We'll count distinct users per payment *type* (e.g., CREDIT_CARD vs MOBILE_PHONE_PLATFORM)
        result = merged.groupby("payment_method_code")["user_key"].nunique().reset_index()
        result = result.rename(columns={"payment_method_code": "Payment Type", "user_key": "Unique Users"})
        result = result.sort_values(by="Unique Users", ascending=False)

        self.report_content.append("## Insight 5: Popularity of Payment Method Types\n")
        self.report_content.append("This helps understand what payment platforms are most trusted by users.\n")
        self.report_content.append(result.to_markdown(index=False))
        self.report_content.append("\n")

    def _get_insight_6_top_users(self):
        """Insight 6: Who are the most engaged users (Top 10 by Score)?"""
        fact_play = self._load_data("fact_play_session", is_fact=True)
        dim_user = self._load_data("dim_user")

        if fact_play.empty or dim_user.empty:
            return

        # Sum total score per user
        user_scores = fact_play.groupby("user_key")["total_score"].sum().reset_index()
        
        # Join with user info to get names
        merged = pd.merge(user_scores, dim_user, on="user_key")
        
        # Select and rename
        result = merged[["username", "first_name", "last_name", "total_score"]]
        result = result.sort_values(by="total_score", ascending=False).head(10)

        self.report_content.append("## Insight 6: Top 10 Users by Total Score\n")
        self.report_content.append("Identifying top players is key for marketing, rewards, and community building.\n")
        self.report_content.append(result.to_markdown(index=False))
        self.report_content.append("\n")

    def _get_insight_7_monthly_revenue(self):
        """Insight 7: What is the monthly revenue trend for 2024?"""
        fact_sub = self._load_data("fact_subscription", is_fact=True)
        dim_date = self._load_data("dim_date")

        if fact_sub.empty or dim_date.empty:
            return

        # Join with DimDate on the subscription's start date
        merged = pd.merge(fact_sub, dim_date, left_on="start_date_key", right_on="date_key")
        
        # Group by month and sum the revenue
        # We only care about 2024 data as per the prompt
        revenue_2024 = merged[merged["year"] == 2024]
        result = revenue_2024.groupby(["year", "month", "month_name"])["cost_amount"].sum().reset_index()
        result = result.sort_values(by="month")
        result = result[["month_name", "cost_amount"]].rename(columns={"cost_amount": "Total Revenue"})

        self.report_content.append("## Insight 7: Monthly Revenue Trend (2024)\n")
        self.report_content.append("Understanding monthly revenue is critical for forecasting and identifying seasonal trends.\n")
        self.report_content.append(result.to_markdown(index=False))
        self.report_content.append("\n")

    def _get_insight_8_avg_duration(self):
        """Insight 8: What is the average play session duration by channel?"""
        fact_play = self._load_data("fact_play_session", is_fact=True)
        dim_channel = self._load_data("dim_channel")

        if fact_play.empty or dim_channel.empty:
            return

        merged = pd.merge(fact_play, dim_channel, on="channel_key")
        result = merged.groupby("english_description")["duration_minutes"].mean().reset_index()
        result = result.rename(columns={"english_description": "Channel", "duration_minutes": "Avg. Duration (Minutes)"})
        
        # Format the duration to 2 decimal places
        result["Avg. Duration (Minutes)"] = result["Avg. Duration (Minutes)"].round(2)

        self.report_content.append("## Insight 8: Average Session Duration by Channel\n")
        self.report_content.append("This shows how engaged users are on each platform. Longer sessions might indicate a better user experience.\n")
        self.report_content.append(result.to_markdown(index=False))
        self.report_content.append("\n")

    def _save_report(self):
        """Saves the generated insights to analysis_report.md."""
        report_path = BASE_DIR / "analysis_report.md"
        final_report = "\n".join(self.report_content)
        
        with open(report_path, "w") as f:
            f.write(final_report)
            
        print(f"\nAnalysis report saved to: {report_path}")
        print("\n--- REPORT PREVIEW ---")
        print(final_report)
        print("----------------------\n")