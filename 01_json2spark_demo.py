# Databricks notebook source
# MAGIC %md 
# MAGIC ## Download FHIR Json Schemas 
# MAGIC
# MAGIC In Healthcare, Fast Healthcare Interoperability Resources (FHIR) is used to exchange data across different organizations in a standardized format. There are primarily 157 different resources represented in complex JSON schemas

# COMMAND ----------

# MAGIC %sh
# MAGIC wget -O /dbfs/user/hive/warehouse/json2spark-schema/fhir.schema.json.zip https://build.fhir.org/fhir.schema.json.zip

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Build Spark Schemas from FHIR Json Schema

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ## Read & Write Data Using Schemas
