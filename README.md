# Reederei Nord: Data Engineering & TCE Analytics Pipeline

This repository contains an automated ETL pipeline designed to ingest, clean, and analyze shipping operations data for **Reederei Nord**. The pipeline bridges the gap between raw ERP exports and manual commercial trackers to provide a "Single Source of Truth" for voyage profitability.

## Project Overview
In the maritime industry, financial data is often fragmented across multiple systems and messy Excel trackers. This project automates the transition from raw data to a structured **Star Schema** in **DuckDB**, enabling precise calculation of **Time Charter Equivalent (TCE)** and identification of revenue leakage.

---

## Data Architecture

### ERD
 **[View Interactive ERD](https://dbdiagram.io/d/69d819d50f7c9ef2c0bf6026)**

### Data Flow
1.  **Raw Ingestion**: Loading CSVs (Voyages, Port Costs, Laytime, Invoices).
2.  **Cleaning (Python)**:
    * **Vessel Normalization**: Mapping messy strings (e.g., "N.Apex") to canonical names ("Nord Apex") and matching **IMO numbers**.
    * **FX Calibration**: Correcting systematic currency errors by applying a 3-year mean Static FX (USD: 1.0, EUR: 1.07, SGD: 0.74, AED: 0.27).
    * **Logic Enrichment**: Automatically flags missing discharge ports as Ship-to-Ship (STS) transfers. validates cross-table references (e.g., matching Laytime to Voyage IDs
3.  **Analytics Mart (DuckDB & SQL)**: Transforming cleaned data into fact and dimension tables.

---

## Getting Started

### Prerequisites
* Python 3.9+
* Install dependencies: `pip install -r requirements.txt`

### **Environment Setup**
The pipeline uses an environment variable to locate your raw data, as the original datasets are not included in the repository for confidentiality.

**To define your data path:**
Run the following command in your terminal (replace `/your/local/path` with the actual folder containing your CSVs):

```bash
export REEDERI_DATA_DIR="/your/local/path/to/data"
```

### **One-Command Reproduce**
Once the path is set, run the ETL:
```bash
make
```

*The system will automatically read from your defined path and generate results in `./output`.*

---

## Project Structure
```text
├── data/               # Contains raw input: voyages.csv, port_costs.csv, etc.
│   ├── data_audit.py       # Post-ETL auditor
│   ├── Makefile
│   ├── run_etl.sh          # initialize environment and execute the pipeline
│   ├── reederei_etl/         # Contains all ETL business logic
│   │   ├── __init__.py     
│   │   ├── __main__.py     
│   │   ├── config.py       # Global path configurations
│   │   ├── fx.py         # Implements Static FX normalization
│   │   ├── pipeline.py         # Manages ingestion, cleaning, and Mart loading
│   │   ├── requirements.txt    # Project dependencies (duckdb, openpyxl, etc.)
│   │   ├── vessel_normalize.py    # String matching logic to map vessel names
│   │   ├── cleaning.py        # Data cleaning
│   │   ├── assertion.py       # Data Quality unit tests (verifies P&L totals match raw inputs)
│   │   └── sql/
│   │       ├── duck_schema.sql     # Analytical Star Schema (Fact/Dimension tables)
│   │       └── 01_schema.sql      # Staging DDLs
│   └── output/
│   │   ├── cleaned/        # Cleaned datasets
│   │   └── reederei_mart.duckdb     # Final DuckDB analytical database

```

---

## Tech Stack
* **Core**: Python (Native CSV/Dict processing for memory efficiency)
* **Database**: DuckDB (In-process OLAP)
* **Transformation**: SQL (CTEs and Star Schema modeling)
* **Environment**: Makefile for reproducible workflow

