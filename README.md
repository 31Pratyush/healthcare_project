# 🏥 HealthCare Data Pipeline Project with PySpark

This project implements a **production-grade ETL pipeline** for a Healthcare system using **PySpark on Databricks**. It processes both static and daily datasets, applies **schema enforcement**, ensures **data quality**, and models data into a **star schema** for analytics.

---

## 🎯 Project Objective

The goal is to simulate a **daily batch pipeline** for a healthcare provider that ingests, transforms, and stores clinical and claims data for analytical consumption.

### Key Objectives:

- 📅 **Daily ingestion** of claim and patient files with `batch_date` in filenames  
- 🛡️ **Data validation** for null checks and schema enforcement  
- 🔁 **SCD Type 2 handling** for patient dimension  
- 🧹 **Quarantining** of bad records  
- 🧼 **Deduplication** of all entities  
- 🗃️ Generate **star schema** outputs: fact and dimension tables  
- ☁️ Simulated **S3 output using DBFS**  

---

## 📁 Project Structure

```
├── HealthCare_Project.py   # Main PySpark ETL pipeline
├── README.md               # Project documentation
├── HLD/
└── sample_data/
```

---

## 🏗️ Architecture Overview

**Tech Stack:**
- ⚙️ PySpark (Databricks)
- 🗂️ DBFS (Simulated S3 storage)
- 🧬 Star Schema modeling
- 📊 BI-ready fact and dimension outputs

**Pipeline Flow:**

```
CSV Files (DBFS) 
    ↓
PySpark (Read → Validate → SCD2 Transform)
    ↓
Dim & Fact tables (Saved to DBFS path as CSVs)
```

---

## 📚 Datasets Processed

- `claims_<batch_date>.csv` – daily healthcare claims  
- `patient_<batch_date>.csv` – daily updated patient info  
- `provider/version=<date>/provider.csv` – static provider info (versioned)  
- `diagnosis/version=<date>/diagnosis.csv` – static diagnosis codes (versioned)  
- `procedure/version=<date>/procedure.csv` – static procedure codes (versioned)  

---

## 🛠️ Features Implemented

✅ Schema enforcement using PySpark `StructType`  
✅ Null validation for critical fields  
✅ Quarantine logic for invalid records (logged)  
✅ Deduplication of all datasets  
✅ Dynamic loading of latest versioned static files  
✅ Full **SCD Type 2 handling** for patient dimension  
✅ Star schema with fact and dimension outputs  
✅ CSV writes to partitioned output folder: `/FileStore/tables/healthcare/processed/<batch_date>/`

---

## 🗃️ Star Schema Tables

### Dimension Tables
- `dim_patients` (SCD2 enabled)
- `dim_provider`
- `dim_diagnosis`
- `dim_procedure`

### Fact Tables
- `fact_claim`

---

## 🧪 Sample BI Queries (Post-Load Use Cases)

```sql
-- Total claims per provider
SELECT provider_id, SUM(claim_amount)
FROM fact_claim
GROUP BY provider_id;

-- Identify frequently claimed procedures
SELECT procedure_code, COUNT(*) AS claim_count
FROM fact_claim
GROUP BY procedure_code
ORDER BY claim_count DESC;

-- Current active patient profiles
SELECT * FROM dim_patients
WHERE is_current = true;
```

---

## 🚀 Future Enhancements

- Integrate with **Airflow for scheduling**
- Implement **Delta Lake** and data versioning
- Add **unit tests** for schema and quality logic
- Implement full **audit logging and lineage**

---

## 📂 How to Run

1. Place input datasets under `/FileStore/tables/healthcare/`
2. Set the desired `batch_date` in the script (default: `2025_06_18`)
3. Run `HealthCare_Project.py` in Databricks
4. Output files will be saved to `/FileStore/tables/healthcare/processed/<batch_date>/`
5. Use Spark or external BI tools to query final outputs

---

## 👨‍💻 Author

**Pratyush Sahay**  
pratyush(dot)310899@gmail(dot)com  
Data Engineer | 3 Years Experience  
LinkedIn: [www.linkedin.com/in/pratyush-sahay](https://www.linkedin.com/in/pratyush-sahay)
