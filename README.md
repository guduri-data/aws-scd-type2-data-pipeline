
# AWS SCD Type 2 Data Pipeline (Project 3)

This project implements **Slowly Changing Dimension (SCD) Type 2** on customer data using **AWS S3 + AWS Glue (PySpark) + Athena**.  
It preserves full history of changes instead of overwriting customer attributes.

---

## What is SCD Type 2?

When customer attributes (like city/state) change, SCD Type 2:
- **expires** the old record (`is_current=false`, sets `end_date`)
- **inserts** a new record (`is_current=true`, `end_date=NULL`)
- keeps historical versions for audit and analytics

---

## Architecture

S3 Raw (customers_day1.csv, customers_day2.csv)  
→ AWS Glue PySpark (SCD Type 2 logic)  
→ S3 Silver (Parquet dimension table)  
→ Athena validation queries

---

## S3 Structure

Raw:
- `s3://surya-project3-scd/raw/customers/customers_day1.csv`
- `s3://surya-project3-scd/raw/customers/customers_day2.csv`

Silver:
- `s3://surya-project3-scd/silver/customers_scd2/`

---

## Output Schema (Silver)

- customer_id  
- customer_unique_id  
- customer_city  
- customer_state  
- start_date  
- end_date  
- is_current  

---

## Proof (Athena Validation)

Result after running SCD:
- `is_current = true` → 99441 rows  
- `is_current = false` → 10 rows  

This confirms 10 customers had attribute changes and history was preserved.

---

## Tech Stack

- AWS S3
- AWS Glue
- PySpark
- Athena
- Parquet

---

## Learning Outcome

Built a real-world **historical dimension** pipeline using SCD Type 2 logic with AWS Glue, and validated results using Athena queries.
