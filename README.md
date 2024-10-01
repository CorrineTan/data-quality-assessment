# Technical Test - Data Quality Assessment and SQL Query

## Project Summary

This repository contains solutions to a technical test involving:

1. **Data Quality Assessment**: A Python script (`tech_test_qa.py`) that connects to a PostgreSQL database to identify intentionally introduced quality control issues in the data.

2. **SQL Query Generation**: A SQL script (`tech_test_query.sql`) that generates a result set based on specific requirements, incorporating fixes for the quality control issues identified in the first task.

### Repository Structure
- tech_test_qa.py: Python script for data quality assessment.
- tech_test_query.sql: SQL script to generate the required result set.
- requirements.txt: List of Python packages required.
- set_env_vars.sh: Shell script to set environment variables (update with your credentials).
- README.md: Project documentation (this file).

---

## How to Run the Program

1. Setting Up Environment Variables

Before running the Python script, you need to set up environment variables for your database credentials.

#### Using the Provided Shell Script (`set_env_vars.sh`)

   Open `set_env_vars.sh` and replace the placeholder values with your actual database credentials.

   Use the following command in your terminal:

   ```bash
   chmod +x set_env_vars.sh
   source ./set_env_vars.sh
   ```

2. Installing Requirements

Create a python virtual envrionment, and install the required Python packages using `pip`:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
This will install the following packages:

- psycopg2
- pandas
- scipy

3. Run the Program

To perform the data quality checks:

```bash
python tech_test_qa.py
```

The script will connect to the PostgreSQL database using the environment variables, perform the data quality checks, and output the findings and conclusions to the console.

## Data Quality Checks and Conclusions

The Python script performs various data quality checks on the `trades`, `users`, and `symbols` tables from the PostgreSQL database. Below are the checks performed and the conclusions drawn:

### 1. Unexpected Strings in Numeric Fields

**Check**: Verified that fields expected to be numeric (`digits`, `cmd`, `volume`, `open_price`, `contractsize`) do not contain unexpected string values.

**Conclusion**: Some numeric fields contain string values, indicating data type inconsistencies.

### 2. Unexpected Numerical Values

**a) Negative Volumes**

**Check**: Identified trades where the `volume` is negative.

**Conclusion**: Negative volume values are invalid and may indicate data entry errors.

**b) Negative Prices**

**Check**: Identified trades where the `open_price` is negative.

**Conclusion**: Negative prices are invalid for trade prices.

**c) Outliers in Volume**

**Check**: Detected outliers in the `volume` field using Z-score statistical analysis.

**Conclusion**: Trades with volumes significantly different from the mean may need investigation for potential anomalies.

### 3. Unexpected Dates

**a) Trades Before 2020**

**Check**: Found trades with `open_time` before the year 2020.

**Conclusion**: Trades occurring before 2020 are outside the expected date range and may be invalid.

**b) `close_time` Before `open_time`**

**Check**: Identified trades where the `close_time` is before the `open_time`.

**Conclusion**: Trades where `close_time` precedes `open_time` are invalid.

### 4. Invalid Joins Between Tables

**a) Trades with Missing Users**

**Check**: Merged `trades` with `users` to find trades with `login_hash` not present in the `users` table.

**Conclusion**: Some trades reference `login_hash` values not present in the `users` table, indicating orphan records.

**b) Trades with Missing Symbols**

**Check**: Merged `trades` with `symbols` to find trades with `symbol` not present in the `symbols` table.

**Conclusion**: Some trades reference symbols not present in the `symbols` table, which may indicate invalid or outdated symbols.

### 5. Trades by Disabled Accounts

**Check**: Identified trades associated with accounts where `enable` is set to `False`.

**Conclusion**: Trades are associated with disabled accounts, which may be invalid and require review.


### 6. Zero Volume Trades

**Check**: Found trades where the `volume` is zero.

**Conclusion**: Trades with zero volume may indicate placeholders or errors and should be investigated.


### 7. Invalid `cmd` Values

**Check**: Checked the `cmd` field for values other than 0 (buy) or 1 (sell).

**Conclusion**: The `cmd` field contains values other than 0 or 1, which are invalid and indicate data errors.

---

### Overall Conclusion

The data contains several quality issues that need to be addressed to ensure accurate analysis and reporting. It is crucial to:

- Exclude or correct invalid data entries, such as negative volumes, negative prices, and invalid `cmd` values.
- Investigate trades linked to non-existent users or symbols.
- Review trades associated with disabled accounts or zero volume.
- Ensure data consistency and integrity before performing further computations or analyses.

---

