# Dice Game Data Processing Pipeline

## 1. Project Description

This project is for a fictional game app development company that launched a "Dice Game" app in 2024. The company needs to process and analyze the first year of data collected from user registrations, play sessions, and payments.

The goal of this project is to:
1.  Build a robust ETL (Extract, Transform, Load) pipeline in Python.
2.  Process raw `.csv` data into a clean, query-ready Star Schema.
3.  Implement data quality checks and unit tests.
4.  Generate a report with key insights to understand 2024 performance and forecast needs for 2025.

This pipeline uses `pandas` for data transformation and `pytest` for testing.

---

## 2. Features

* **ETL Pipeline:** A complete, runnable pipeline to process 9 raw data sources.
* **Star Schema:** Transforms the raw data into a dimensional model (Dimensions and Facts) ideal for analysis.
* **Data Quality:** Includes a `DataQualityValidator` class to check for nulls, uniqueness, and referential integrity.
* **Unit Testing:** Contains a `tests/` directory with `pytest` unit tests for core transformation logic.
* **Insight Generation:** Automatically runs 8 analyses and generates a summary report (`analysis_report.md`).

---

## 3. Project Structure
```bash
dice_game_etl/
|
|-- data/
|   |-- raw/
|   |   |-- channel_code.csv
|   |   |-- plan.csv
|   |   |-- plan_payment_frequency.csv
|   |   |-- status_code.csv
|   |   |-- user.csv
|   |   |-- user_payment_detail.csv
|   |   |-- user_plan.csv
|   |   |-- user_play_session.csv
|   |   |-- user_registration.csv
|   |
|   |-- processed/
|       |-- dimensions/
|       |-- facts/
|
|-- src/
|   |-- __init__.py
|   |-- config.py
|   |-- data_loader.py
|   |-- transformations.py
|   |-- data_quality.py
|   |-- insights.py
|
|-- tests/
|   |-- __init__.py
|   |-- test_transformations.py
|   |-- test_data_quality.py
|
|-- main.py
|-- analysis_report.md
|-- requirements.txt
|-- README.md
```
---

## 4. Setup and Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/gti099820/fcc_code_challenge.git
    cd dice_game_etl
    ```

2.  **Create and activate a virtual environment (recommended):**
    ```bash
    python -m venv venv
    .\venv\Scripts\activate  # On Windows
    # source venv/bin/activate  # On macOS/Linux
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Add Data:**
    Place all 9 raw `.csv` files (`user.csv`, `plan.csv`, etc.) into the `data/raw/` directory.

---

## 5. How to Run

### Run the Main ETL Pipeline

With your virtual environment active and from the root `dice_game_etl/` directory, run:

```bash
python main.py
```
This command will:
- Load all raw data from data/raw/.
- Run data quality checks.
- Build all dimensions and facts and save them to data/processed/.
- Generate all 8 insights and save them to analysis_report.md.

Run Tests:
- To verify the transformation logic, run pytest from the root directory:
```bash
Bash pytest
```
