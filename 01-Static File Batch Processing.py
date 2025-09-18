# Databricks notebook source
#set path
base_path = "/Volumes/medisure_llh"
bronze_path = f"{base_path}/bronze"
file_dir = f"{bronze_path}/source_files"
silver_path = f"{base_path}/silver"
gold_path = f"{base_path}/gold"
chk_path = f"{base_path}/checkpoint"

print(file_dir)
print(bronze_path)

# COMMAND ----------

#set path
base_path = "/Volumes/medisure_llh"
bronze_path = f"{base_path}/bronze"

#load data to bronze tables:

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, DateType, TimestampType, FloatType, DoubleType

#define schema
diagnosis_ref_schema = StructType([
  StructField("Code", StringType(), True),
  StructField("Description", StringType(), True)
])

members_schema = StructType([
  StructField("MemberID", StringType(), True),
  StructField("Name", StringType(), True),
  StructField("DOB", DateType(), True),
  StructField("Gender", StringType(), True),
  StructField("Region", StringType(), True),
  StructField("PlanType", StringType(), True),
  StructField("EffectiveDate", DateType(), True),
  StructField("Email", StringType(), True),
  StructField("IsActive", DoubleType(), True),
  StructField("LastUpdated", DateType(), True),
])

claims_schema = StructType([
  StructField("ClaimID", StringType(), True),
  StructField("MemberID", StringType(), True),
  StructField("ProviderID", StringType(), True),
  StructField("ClaimDate", DateType(), True),
  StructField("ServiceDate", DateType(), True),
  StructField("Amount", DoubleType(), True),
  StructField("Status", StringType(), True),
  StructField("ICD10Codes", StringType(), True),
  StructField("CPTCodes", StringType(), True),
  StructField("ClaimType", StringType(), True),
  StructField("SubmissionChannel", StringType(), True),
  StructField("Notes", StringType(), True),
  StructField("IngestTimestamp", TimestampType(), True)
])

providers_schema = StructType([
  StructField("ProviderID", StringType(), True),
  StructField("Name", StringType(), True),
  StructField("Specialties", ArrayType(StringType()), True),
  StructField("Location", ArrayType(
    #StringType()
    StructType([
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True)])
                ), True),
  StructField("IsActive", BooleanType(), True),
  StructField("TIN", StringType(), True),
  StructField("LastVerified", StringType(), True)
])

diagnosis_df = (spark.read
              .format("csv")
              .schema(diagnosis_ref_schema)
              .option("header","true")
              .option("timestampFormat","yyyy-mm-dd[hh:mm:ss]")
              .load(f"{file_dir}/diagnosis_ref.csv")
              .write
              .format("delta")
              .mode("overwrite")
              .saveAsTable("medisure_llh.bronze.diagnosis")
)

members_df = (spark.read
              .format("csv")
              .schema(members_schema)
              .option("header","true")
              .option("timestampFormat","yyyy-mm-dd[hh:mm:ss]")
              .load(f"{file_dir}/members.csv")
              .write
              .format("delta")
              .mode("overwrite")
              .saveAsTable("medisure_llh.bronze.members")
            )
 
             
claims_df = (spark.read
              .format("csv")
              .schema(claims_schema)
              .option("header","true")
              .option("timestampFormat","yyyy-MM-dd HH:mm:ss")
              .load(f"{file_dir}/claims_batch.csv")
              .write
              .format("delta")
              .mode("overwrite")
              .saveAsTable("medisure_llh.bronze.claims")
            )



# COMMAND ----------

#workaround for mismatch schema
spark.sql("DROP TABLE IF EXISTS medisure_llh.bronze.providers")

providers_df = (spark.read
              .format("json")
              .option("mode","PERMISSIVE")
              .option("timestampFormat","yyyy-mm-dd")
              #.schema(providers_schema)
              .option("inferSchema","true")
              .load(f"{file_dir}/providers.json")
              )
providers_df = providers_df.withColumn("LastVerified", providers_df["LastVerified"].cast(DateType()))

providers_df.write.format("delta").mode("overwrite").saveAsTable("medisure_llh.bronze.providers")
