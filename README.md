#  Base Labs Project - Architecture of the DataFlow

## Overview
This project aims to build a local, automated, and scalable data architecture that simulates a local Data Lake and allows to extract, transform, and load data from files published by PwC. The final result is the generation of business tables that are consumed in visual dashboards in PowerBI.

## Requirements
- Python 3 and Visual Studio Code
- Docker installed and running
- Dependencies listed in requirements.txt
- Spark container configured in docker-compose.yml

## Stack of technologies
- **Python** for general development and automation
- **Docker** to run PySpark + Delta Lake without local dependencies
- **Apache Spark + Delta Lake** for data processing
- **PostgreSQL (Railway)** as an intermediate database for consumption
- **PowerBI** as a data visualization tool

## Architecture
The project simulates a local Data Lake, divided into layers:

### 1. Prelanding
- Script: `01_prelanding/Template_Ingestion.py`
- Downloads and organizes `.zip` from PwC’s official site
- Extracts CSV files and stores them in their corresponding folders

### 2. Bronze
- Stores raw data in Delta Lake format
- Scripts in `02_bronze/`
- No transformations applied, only conversion to Delta
  
### 3. Silver
- Cleansing and normalization of data
- Unnecessary tables, nulls, and irrelevant columns are discarded
- Scripts in `03_silver/`

### 4. Gold
- Generates business tables: `margins_profits`, `margins_profits_brand`, `rotation`
- Scripts in `04_gold/`

### 5. PostgreSQL
- PostgreSQL database hosted on Railway to expose data to PowerBI
- Scripts in `05_postgres/`

### 6. PowerBI
- Data is consumed from PostgreSQL to visualize insights

### Pipeline
- Script: `pipelines/ppl.py`
- Automates the execution of the entire flow from Prelanding to PostgreSQL
