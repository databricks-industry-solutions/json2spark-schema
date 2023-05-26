# Databricks notebook source
# MAGIC %md 
# MAGIC # Json Schema Representation in Spark
# MAGIC
# MAGIC ## Use Case 
# MAGIC
# MAGIC
# MAGIC Json schema objects serve as a way to represent possible implementations of an object. Spark natively has the capability to read a json object and infer a schema from the object. However, not all objects, or permutations of objects, with potential errors in object representation, accurately and fully represent a json schema. 
# MAGIC
# MAGIC Therefore, this repo was built with the intent of helping to represent large and complex Json schemas in Spark in a simple manner.
# MAGIC
# MAGIC
# MAGIC ## What Schemas Represented Here? 
# MAGIC
# MAGIC The use case here is for Healthcare FHIR data (https://build.fhir.org/resourcelist.html). There exists 157 unique schemas and over 157 definitions of Json objects. This makes working with FHIR data extremely cumbersome and complex. We'll show how to represent these schemas dynamically and apply it to some Synthetic FHIR data from SyntheticMass.
# MAGIC
# MAGIC **SyntheticMass**: https://synthea.mitre.org/: Jason Walonoski, Mark Kramer, Joseph Nichols, Andre Quina, Chris Moesel, Dylan Hall, Carlton Duffett, Kudakwashe Dube, Thomas Gallagher, Scott McLachlan, Synthea: An approach, method, and software mechanism for generating synthetic patients and the synthetic electronic health care record, Journal of the American Medical Informatics Association, Volume 25, Issue 3, March 2018, Pages 230–238, https://doi.org/10.1093/jamia/ocx079 

# COMMAND ----------


