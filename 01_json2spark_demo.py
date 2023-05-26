# Databricks notebook source
# MAGIC %md 
# MAGIC ## Download FHIR Json Schemas 
# MAGIC
# MAGIC In Healthcare, Fast Healthcare Interoperability Resources (FHIR) is used to exchange data across different organizations in a standardized format. There are primarily 157 different resources represented in complex JSON schemas

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /dbfs/user/hive/warehouse/json2spark-schema /tmp/json_schemas  /tmp/spark_schemas 
# MAGIC mkdir -p  /dbfs/user/hive/warehouse/json2spark-schema/spark_schemas /tmp/json_schemas /tmp/spark_schemas 
# MAGIC wget -O /tmp/fhir.schema.json.zip https://build.fhir.org/fhir.schema.json.zip
# MAGIC unzip -d /tmp/json_schemas/ /tmp/fhir.schema.json.zip

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Build Spark Schemas from FHIR Json Schema

# COMMAND ----------

# DBTITLE 1,Detecting Circular Dependencies in FHIR 
# MAGIC %scala
# MAGIC import com.databricks.industry.solutions.json2spark._  
# MAGIC import org.apache.spark.sql.types._
# MAGIC
# MAGIC val jsonFile = "/tmp/json_schemas/fhir.schema.json"
# MAGIC //Load a schema to get circular references, all definitions of FHIR resources
# MAGIC val circularDependencies = new Json2Spark(Json2Spark.file2String(jsonFile), 
# MAGIC   defsLocation="definitions", 
# MAGIC   enforceRequiredField=false)  
# MAGIC   
# MAGIC //All definitions in the FHIR schema
# MAGIC val keys = circularDependencies.json.hcursor.downField("definitions").keys.getOrElse(Seq.empty)
# MAGIC //All FHIR bundle resource types
# MAGIC val v = circularDependencies.json.hcursor.downField("oneOf").values.getOrElse(Seq.empty)  
# MAGIC //Define circular refs in the FHIR schema
# MAGIC val circularRefs = { 
# MAGIC   keys.map(y => circularDependencies.isSelfReference("#/definitions/" + y))
# MAGIC     .filter(!_.isEmpty).flatMap(y => y)
# MAGIC     .toSeq ++ Seq("#/definitions/ResourceList", "#/definitions/CodeableConcept", "#/definitions/Reference", "#/definitions/EvidenceVariable_Characteristic", "#/definitions/ExampleScenario_Step")
# MAGIC     }

# COMMAND ----------

# DBTITLE 1,Creating a Spark Schema for all 158 FHIR resource types
# MAGIC %scala
# MAGIC //Reload the FHIR resource with circularRefs
# MAGIC val fhir = new Json2Spark(Json2Spark.file2String(jsonFile), 
# MAGIC  defsLocation="definitions", 
# MAGIC  enforceRequiredField=false,
# MAGIC  circularReferences=Some(circularRefs))
# MAGIC  
# MAGIC //Create a Map[ResoureType -> SparkSchema] of all FHIR ResourceTypes  
# MAGIC val fhirSchemas = v.toSeq
# MAGIC   .map(x => x.hcursor)
# MAGIC   .map(c => c.downField("$ref").as[String].getOrElse(""))
# MAGIC   .map(str => Map[String, StructType](str.split("/").last -> new StructType(fhir.defs(str.split("/").last).toArray)))
# MAGIC   .reduce(_ ++ _)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Saving schemas in Databricks for future references
# MAGIC After this step, we can choose whatever language we desire to work with our Spark Schemas

# COMMAND ----------

# MAGIC %scala
# MAGIC //Write to local storage first
# MAGIC import java.io._
# MAGIC
# MAGIC fhirSchemas.foreach(pair => {
# MAGIC   val file = new FileWriter(new File("/tmp/spark_schemas/" + pair._1 + ".json"))
# MAGIC   file.write(pair._2.prettyJson)
# MAGIC   file.close
# MAGIC })

# COMMAND ----------

# DBTITLE 1,Move into DBFS 
# MAGIC %sh
# MAGIC ls -ltr /tmp/spark_schemas 
# MAGIC mv /tmp/spark_schemas/*json /dbfs/user/hive/warehouse/json2spark-schema/spark_schemas/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -ltr /dbfs/user/hive/warehouse/json2spark-schema/spark_schemas/

# COMMAND ----------

# MAGIC %md # Read data in pyspark using schemas

# COMMAND ----------

# MAGIC %python
# MAGIC import json
# MAGIC from pyspark.sql.types import *
# MAGIC dbutils.fs.cp("dbfs:/user/hive/warehouse/json2spark-schema/spark_schemas/Patient.json", "file:///tmp/Patient.json")
# MAGIC patient_schema = None
# MAGIC with open("/tmp/Patient.json") as f:
# MAGIC   patient_schema = StructType.fromJson(json.load(f))
# MAGIC   dbutils.fs.rm("./Patient.json")
# MAGIC
# MAGIC print(patient_schema)

# COMMAND ----------

data = spark.sparkContext.parallelize(["""
{
  "id": "first",
  "name": 
  [{
      "use": "official",
      "given": ["Maya"],
      "family": "XYZ"
  }]
}""",
"""
{
  "id": "second",
  "name": 
  [{
      "use": "official",
      "given": ["Emma"],
      "family": "XYZ"
  }]
}"""])

df = spark.read.option("multiline", True).schema(patient_schema).json(data)


# COMMAND ----------

df.select('id', 'name.given', 'name.family').distinct().show()
