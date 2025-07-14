# Databricks notebook source
dbutils.fs.ls("/FileStore/tables/healthcare/")

# COMMAND ----------

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MyLogger")
logger.info("logger enabled")

# COMMAND ----------

# import argparse
# parser = argparse.ArgumentParser()
# parser.add_argument('--batch_date',required=True)
# args = parser.parse_args()
# batch_date = args.batch_date
# logger.info("Running job for batch date",batch_date) 
# "used to pass batch_date from airflow "
from datetime import datetime
batch_date ="2025_06_18" 
base_path = "/FileStore/tables/healthcare"
# output_path = "s3a://etl_healthcare/output/" #we will write processed data to s3 then copy from there to redshift staging tables
output_path=f"{base_path}/processed"
quarantine_path = f"{output_path}/quarantine/{batch_date}"

# COMMAND ----------

#SCHEMA ENFORCEMENT
#diagnosis procedure provider claims patients
from pyspark.sql.types import StructField, StructType,IntegerType,StringType,DoubleType,DateType,LongType,BooleanType
claims_schema = StructType([
    StructField("claim_id",StringType(),False),
    StructField("patient_id",StringType(),False),
    StructField("provider_id",StringType(),False),
    StructField("diagnosis_code",StringType(),False),
    StructField("procedure_code",StringType(),False),
    StructField("claim_amount",DoubleType(),False),
    StructField("claim_date",DateType(),False)
])
patients_schema = StructType([
    StructField("patient_id",StringType(),False),
    StructField("name",StringType(),False),
    StructField("gender",StringType(),False),
    StructField("dob",StringType(),False),
    StructField("effective_date",DateType(),False)
])
providers_schema = StructType([
    StructField("provider_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("specialty", StringType(), True)
])
diagnosis_schema = StructType([
    StructField("diagnosis_code", StringType(), False),
    StructField("description", StringType(), True)
])
procedure_schema = StructType([
    StructField("procedure_code", StringType(), False),
    StructField("description", StringType(), True)
])
date_schema = StructType([
    StructField("date_id", IntegerType(), False),
    StructField("date", DateType(), False),
    StructField("year", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("day", IntegerType(), False)
])

# COMMAND ----------

#static files update occassionally not daily, and they are versioned when updated.So following code is to get latest version of static files.
import re
from datetime import datetime
def get_latest_path(file_name):
    try:
        latest_date = None
        latest_path = None
        folders = dbutils.fs.ls(f"{base_path}/{file_name}")
        for f in folders:
            match = re.search(r"version=(\d{4}-\d{2}-\d{2})",f.path)
            if match:
                date_str = match.group(1)
                date_obj = datetime.strptime(date_str,"%Y-%m-%d")
                if latest_date is None or date_obj>latest_date:
                    latest_date = date_obj
                    latest_path = f.path
        if latest_path:
            return f"{latest_path}{file_name}.csv"
        return None
    except Exception as e:
        logger.error(f"Error while reading latest path: {e}")


# COMMAND ----------

#Reading files using above schema
def read_files(path,schema,description:str):
    try:
        df = spark.read.format('csv').schema(schema).option("header","true").load(path)
        logger.info(f"Read {description}: {df.count()} records ")
        return df
    except Exception as e:
        logger.error(f"Error reading {description}: error:{str(e)}")

# COMMAND ----------

claims_path = f"{base_path}/claims_{batch_date}.csv"
patients_path = f"{base_path}/patient_{batch_date}.csv"
provider_path = get_latest_path("provider")
diagnosis_path = get_latest_path("diagnosis")
procedure_path = get_latest_path("procedure")

# COMMAND ----------

claims_df = read_files(claims_path,claims_schema,"claims")
patients_df = read_files(patients_path,patients_schema,"patients")
provider_df = read_files(provider_path,providers_schema,"provider")
diagnosis_df = read_files(diagnosis_path,diagnosis_schema,"diagnosis")
procedure_df = read_files(procedure_path,procedure_schema,"procedure")

# COMMAND ----------

#Dataframe validation and quarantine null records.
from pyspark.sql.functions import *
def validate_not_null(df,critical_cols,quarantine_name:str):
    df_valid = df
    for c in critical_cols:
        df_valid = df_valid.filter(col(c).isNotNull())
    condition = col(critical_cols[0]).isNull()
    for c in critical_cols[1:]:
        condition = condition | col(c).isNull()
    df_invalid = df.filter(condition)
    if df_invalid.count()>0:
        logger.warning(f"Quality check found {df_invalid.count()} null records in {quarantine_name}")
        # df_invalid.write.mode("overwrite").csv(f"{quarantine_path}/{quarantine_name}
        # ")
    logger.info(f"Found {df_valid.count()} valid records")
    return df_valid
    

# COMMAND ----------

def deduplicate(df,name:str):
    logger.info(f"Reading {df.count()} records for {name}_df")
    df_deduped = df.dropDuplicates()
    df_deduped_count = df_deduped.count()
    dropped_count = df.count() - df_deduped_count
    logger.info(f"Dropped {dropped_count} records for {name}_df")
    logger.info(f"{df_deduped_count} deduplicated records read for {name}_df")
    return df_deduped


# COMMAND ----------

claims_df = validate_not_null(claims_df,["claim_id","patient_id","provider_id","claim_amount","claim_date"],"claims")
patients_df = validate_not_null(patients_df,["patient_id"],"patients")
provider_df = validate_not_null(provider_df,["provider_id","name","specialty"],"provider")
diagnosis_df = validate_not_null(diagnosis_df,["diagnosis_code"],"diagnosis")
procedure_df = validate_not_null(procedure_df,["procedure_code"],"procedure")

# COMMAND ----------

claims_df = deduplicate(claims_df,"claims")
patients_df = deduplicate(patients_df,"patients")
provider_df = deduplicate(provider_df,"provider")
diagnosis_df = deduplicate(diagnosis_df,"diagnosis")
procedure_df = deduplicate(procedure_df,"procedure")

# COMMAND ----------

#SCD2 HANDLING FOR patients
#Read latest file from processed folder (s3 is simulated here by dbfs storage)
#/FileStore/tables/healthcare/processed/patients_2025-07-10
scd2_files = dbutils.fs.ls(output_path)
l_path = None
l_date = None
scd2_patients_path = None
for f in scd2_files:
    match = re.search(r"patients_(\d{4}-\d{2}-\d{2})\.csv",f.path)
    if match:
        date_str = match.group(1)
        date_obj = datetime.strptime(date_str,"%Y-%m-%d")
        if l_date is None or date_obj>l_date:
            l_date = date_obj
            l_path = f.path
if l_path:
    scd2_patients_path = f"{l_path}"
latest_patients_file = scd2_patients_path
print(latest_patients_file)

# COMMAND ----------

#source -> patients_df
#target/existing -> latest_patient_file / dim_patients_df
if latest_patients_file:
    dim_patients_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(latest_patients_file)
    dim_patients_df = dim_patients_df.withColumn("start_date",to_date("start_date"))\
        .withColumn("end_date",to_date("end_date"))\
            .withColumn("is_current",col("is_current").cast("boolean"))
else:
    logger.info("No previous file found,initializing empty dimension table")
    schema = StructType([
        StructField("patient_id", StringType()),
        StructField("name", StringType()),
        StructField("gender", StringType()),
        StructField("dob", StringType()),
        StructField("effective_date", DateType()),
        StructField("start_date", DateType()),
        StructField("end_date", DateType()),
        StructField("is_current", BooleanType())
    ])
    dim_patients_df = spark.createDataFrame([], schema) 

# COMMAND ----------

df_current = dim_patients_df.filter(col("is_current") == True)
df_join = patients_df.alias("src").join(
    df_current.alias("dim"), on="patient_id", how="left"
)
df_changed = df_join.filter(
    (col("src.name")    != col("dim.name")) |
    col("dim.patient_id").isNull()
)
df_expired = df_changed.filter(col("dim.patient_id").isNotNull()) \
    .select("dim.*") \
    .withColumn("end_date", lit(batch_date)) \
    .withColumn("is_current", lit(False))

df_new = df_changed.select("src.*") \
    .withColumn("start_date", lit(batch_date)) \
    .withColumn("end_date", lit(None).cast("date")) \
    .withColumn("is_current", lit(True))

df_unchanged = df_current.join(
    df_changed.select("patient_id"), on="patient_id", how="left_anti"
)
df_final = df_expired.select(dim_patients_df.columns) \
    .unionByName(df_new.select(dim_patients_df.columns)) \
    .unionByName(df_unchanged.select(dim_patients_df.columns))


# COMMAND ----------

#claims/daily provider/static diagnosis/static procedure/static patients/scd2
#DIMENSION TABLES
dim_provider = provider_df
dim_diagnosis = diagnosis_df
dim_procedure = procedure_df
dim_patients = df_final

# COMMAND ----------

#FACT TABLE
fact_claim = claims_df.alias("c") \
    .join(dim_patients.filter(col("is_current") == True).alias("p"), "patient_id", "left") \
    .join(dim_provider.alias("pr"), "provider_id", "left") \
    .join(dim_diagnosis.alias("d"), "diagnosis_code", "left") \
    .join(dim_procedure.alias("pc"), "procedure_code", "left") \
    .select(
        "claim_id", "c.patient_id", "c.provider_id", "diagnosis_code", "procedure_code",
        "claim_amount"
    )

# COMMAND ----------

#Facts and Dim tables with scd2 handling in patients completed, now loading these files daily to s3/processed with daily timestamp.
#s3 storage is simulated with DBFS.
def write_to_s3_processed(df,file_name:str):
    try:
        df.write.format('csv').mode("overwrite").option("header","true").save(f"{output_path}/{batch_date}/{file_name}")
        logger.info(f"successfully wrote {file_name} to {output_path}/{batch_date}/{file_name} ")
    except Exception as e:
         logger.error(f"Failed to write {file_name} to {output_path}/{batch_date}/{file_name}. Error: {str(e)}")

# COMMAND ----------

write_to_s3_processed(dim_provider,"dim_provider")
write_to_s3_processed(dim_diagnosis,"dim_diagnosis")
write_to_s3_processed(dim_procedure,"dim_procedure")
write_to_s3_processed(dim_patients,"dim_patients")
write_to_s3_processed(fact_claim,"fact_claim")

# COMMAND ----------

df_check = spark.read.format("csv").option("header","true").option("header","true").load(f"{output_path}/{batch_date}/fact_claim")
df_check.show(5)

# COMMAND ----------

logger.info(f"Pipeline run for date : {batch_date} sucessfull")