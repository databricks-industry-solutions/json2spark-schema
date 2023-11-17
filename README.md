# json2spark-schema
Given a json schema file, translate to an equivalent spark schema. 

## Implementation
The goal of this repo is not to represent every permutation of a json schema -> spark schema mapping, but provide a foundational layer to achieve similar representation. E.g. keywords like oneOf, allOf, enums, constants, etc are not supported for Spark definitions and therefore are not in scope. 


## Running

Built for **Spark 3.3.0 & Scala 2.12** Please make sure your cluster matches these versions

```scala
import com.databricks.industry.solutions.json2spark._

val x = new Json2Spark(Json2Spark.file2String("src/test/scala/resources/address.schema.json"))
x.convert2Spark
/*
org.apache.spark.sql.types.StructType = StructType
StructType(
  StructField(post-office-box,StringType,false), 
  StructField(extended-address,StringType,false), 
  StructField(street-address,StringType,false), 
  StructField(locality,StringType,true), 
  StructField(region,StringType,true), 
  StructField(postal-code,StringType,false), 
  StructField(country-name,StringType,true)
)
*/

```

## Running with Spark
```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions.col

val spark = ( SparkSession.builder()
      .master("local[2]")
      .config("spark.driver.bindAddress","127.0.0.1") 
      .getOrCreate() )
      
val rdd = spark.sparkContext.parallelize(Seq( Seq("apple"), Seq("orange", "blueberry"), Seq("starfruit"), Seq("mango", "strawberry", "apple"))).map(row => Row(row))
val schema = new Json2Spark(Json2Spark.file2String("src/test/scala/resources/veggies.json")).convert2Spark
val df = spark.createDataFrame(rdd, schema)

df.printSchema
/*
root
 |-- fruits: array (nullable = true)
 |    |-- element: string (containsNull = true)
 */

```

## More Complex Representations 

### Do not enforce required fields
Optionally allow flexibility in schema definition by not requiring fields to be populated

```scala
val x = new Json2Spark(Json2Spark.file2String("src/test/scala/resources/address.schema.json"), enforceRequiredField=false)
x.convert2Spark
/*
org.apache.spark.sql.types.StructType = StructType
StructType(
  StructField(post-office-box,StringType,true), 
  StructField(extended-address,StringType,true), 
  StructField(street-address,StringType,true), 
  StructField(locality,StringType,true), 
  StructField(region,StringType,true), 
  StructField(postal-code,StringType,true), 
  StructField(country-name,StringType,true)
)
*/
```

### Definition as a spark schema
Json schemas can represent objects in a definitions path. Using the ***defs*** method converts a json definition to a Spark Schema

```scala
val x = new Json2Spark(Json2Spark.file2String("src/test/scala/resources/in-network-rates.json"), 
	defsLocation="definitions")

StructType(x.defs("in_network")).prettyJson
/*
String =
{
  "type" : "struct",
  "fields" : [ {
    "name" : "negotiation_arrangement",
    "type" : "string",
    "nullable" : true,
    "metadata" : {
      "description" : "",
      "path" : "#/definitions/in_network/properties/negotiation_arrangement"
    }
  }, {
    "name" : "name",
    "type" : "string",
    "nullable" : true,
    "metadata" : {
      "description" : "",
      "path" : "#/definitions/in_network/properties/name"
    }
  },
...
*/
```

### Reference resolution 
Automatically resolving and translating references like below into a unified spark schema

```json
    "vegetables": {
      "type": "array",
      "items": { "$ref": "#/$defs/veggie" }
    }
```

```scala
val x = new Json2Spark(Json2Spark.file2String("src/test/scala/resources/veggies.schema.json"))
x.convert2Spark.prettyJson
/*
String =
{
  "type" : "struct",
  "fields" : [ {
    "name" : "fruits",
    "type" : {
      "type" : "array",
      "elementType" : "string",
      "containsNull" : true
    },
    "nullable" : true,
    "metadata" : {
      "description" : "",
      "path" : "#/properties/fruits"
    }
  }, {
    "name" : "vegetables",
    "type" : {
      "type" : "array",
      "elementType" : {
        "type" : "struct",
        "fields" : [ {
          "name" : "veggieName",
          "type" : "string",
          "nullable" : true,
          "metadata" : {
            "description" : "The name of the vegetable.",
            "path" : "#/properties/vegetables/items/properties/veggieName"
          }
        }, {
          "name" : "veggieLike",
          "type" : "b...
*/	  
```

### Circular Depdencies 
Some schemas have self references and extensions that result in an unlimited schema definition (stack overflow errors). These can be filtered out by specifying their path like below

```scala
val x = new Json2Spark(Json2Spark.file2String("src/test/scala/resources/fhir.schema.json"), 
	circularReferences=Some(Seq("#/definitions/Extension", "#/definitions/Element", "#/definitions/Identifier", "#/definitions/Period","#/definitions/Reference")))
	
new StructType(x.defs("Patient").toArray).prettyJson
/*
String =
{
  "type" : "struct",
  "fields" : [ {
    "name" : "id",
    "type" : "string",
    "nullable" : true,
    "metadata" : {
      "description" : "Any combination of letters, numerals, \"-\" and \".\", with a length limit of 64 characters.  (This might be an integer, an unprefixed OID, UUID or any other identifier pattern that meets these constraints.)  Ids are case-insensitive.",
      "path" : "#/definitions/Patient/properties/id/$ref//#/definitions/id"
    }
  }, {
    "name" : "meta",
    "type" : {
      "type" : "struct",
      "fields" : [ {
        "name" : "id",
        "type" : "string",
        "nullable" : true,
        "metadata" : {
          "description" : "A sequence of Unicode characters",
          "path" : "#/definitions/Pati...
*/
```

### Saving schema to a file for future use 
```scala
import java.io._

val fileName = ??? //add your location here 
val file = new FileWriter(new File(fileName))
file.write(x.convert2Spark.prettyJson)
file.close()
```

## Databricks Notebooks Examples

Further examples found in *01_healthcare_FHIR_demo.py* and include samples of using Healthcare FHIR (Fast Healthcare Interoperability Resources).
