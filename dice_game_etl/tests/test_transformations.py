# tests/test_transformations.py
import pandas as pd
import pytest
from src.transformations import StarSchemaBuilder

@pytest.fixture
def sample_raw_data():
    """Creates a mock raw_data dictionary for testing."""
    return {
        "user": pd.DataFrame({"user_id": [1, 2], "ip_address": ["1.1.1.1", "2.2.2.2"]}),
        "registration": pd.DataFrame({"user_registration_id": [101, 102], "user_id": [1, 2], "username": ["user1", "user2"]}),
        "play_session": pd.DataFrame({
            "play_session_id": [1001],
            "user_id": [1],
            "start_datetime": ["2024-01-01T10:00:00.000-06:00"],
            "end_datetime": ["2024-01-01T10:30:00.000-06:00"],
            "channel_code": ["MOBILE"],
            "status_code": ["COMPLETED"],
            "total_score": [150]
        }),
        "channel": pd.DataFrame({"play_session_channel_code": ["MOBILE"], "english_description": ["Mobile"]}),
        "status": pd.DataFrame({"play_session_status_code": ["COMPLETED"], "english_description": ["Completed"]}),
        "plan": pd.DataFrame(),
        "payment_frequency": pd.DataFrame(),
        "payment_detail": pd.DataFrame(),
        "user_plan": pd.DataFrame(),
    }

@pytest.fixture
def builder(sample_raw_data):
    """Initializes the builder with mock data."""
    return StarSchemaBuilder(sample_raw_data)

def test_create_dim_user(builder):
    builder._create_dim_user()
    dim_user = builder.dimensions["dim_user"]
    
    assert "user_key" in dim_user.columns
    assert "username" in dim_user.columns
    assert "ip_address" in dim_user.columns
    assert len(dim_user) == 2
    assert dim_user.loc[dim_user["user_id"] == 1, "username"].values[0] == "user1"

def test_create_fact_play_session_duration(builder):
    # Need to create dimensions first
    builder.create_dimensions()
    builder._create_fact_play_session()
    fact_play = builder.facts["fact_play_session"]
    
    assert "duration_minutes" in fact_play.columns
    assert fact_play["duration_minutes"].values[0] == 30.0
    assert "user_key" in fact_play.columns
    assert "channel_key" in fact_play.columns
    assert "status_key" in fact_play.columns
    assert fact_play["start_date_key"].values[0] == 20240101