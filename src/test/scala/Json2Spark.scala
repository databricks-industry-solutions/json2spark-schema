package com.databricks.labs.json2spark

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions.col


class Json2SparkTest extends AnyFunSuite{

  test("test basic properties and simple datatypes"){
    val result = new Json2Spark(Json2Spark.file2String("src/test/scala/resources/address.schema.json")).convert2Spark
    assert(result.getClass == new StructType().getClass)
    assert(result.size == 7)
    assert(result("post-office-box").dataType == StringType)
    assert(result("post-office-box").nullable == false)
  }

  test("test arrays, objects"){
    val result = new Json2Spark(Json2Spark.file2String("src/test/scala/resources/veggies.json")).convert2Spark
    assert(result.size == 1)
    assertCompiles(""" result("fruits").dataType.asInstanceOf[ArrayType] """) 
  }

  test("test creating dataframes and rows"){
    val spark = SparkSession.builder()
      .master("local[2]")
      .config("spark.driver.bindAddress","127.0.0.1") 
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Seq( Seq("apple"), Seq("orange", "blueberry"), Seq("starfruit"), Seq("mango", "strawberry", "apple"))).map(row => Row(row))
    val schema = new Json2Spark(Json2Spark.file2String("src/test/scala/resources/veggies.json")).convert2Spark
    val df = spark.createDataFrame(rdd, schema)
    assert(df.count() == 4)
    assert(df.select(col("fruits")).first.getSeq[String](0)(0) == "apple")
  }


  test("test FHIR resources"){
    val x = new Json2Spark(Json2Spark.file2String("src/test/scala/resources/fhir.schema.json"),
      defsLocation="definitions",
      enforceRequiredField=false,
      circularReferences=Some(Seq("#/definitions/Extension", "#/definitions/Element", "#/definitions/Identifier", "#/definitions/Period","#/definitions/Reference")))

    assert(new StructType(x.defs("Patient_Link").toArray).size == 6)
    val s = new StructType(x.defs("Patient").toArray)
    assert(s.fields.size == 35)
    assert(s("id").metadata.getString("path") == "#/definitions/Patient/properties/id/$ref//#/definitions/id")


    //Patient Name
    assert(s("name").metadata.toString == """{"description":"A name associated with the individual.","path":"#/definitions/Patient/properties/name"}""")
    //First 
    assert(s("name").dataType.asInstanceOf[ArrayType].productElement(0).asInstanceOf[StructType]("given").metadata.toString == """{"description":"Given name.","path":"#/definitions/Patient/properties/name/items/$ref//#/definitions/HumanName/properties/given"}""")
    //Last
    assert(s("name").dataType.asInstanceOf[ArrayType].productElement(0).asInstanceOf[StructType]("family").metadata.toString == """{"description":"A sequence of Unicode characters","path":"#/definitions/Patient/properties/name/items/$ref//#/definitions/HumanName/properties/family/$ref//#/definitions/string"}""")

    //All fields under name
    val a = s("name").dataType.asInstanceOf[ArrayType].productElement(0).asInstanceOf[StructType].fields.map(x => x.name)
    val b = Array("id", "extension", "use", "_use", "text", "_text", "family", "_family", "given", "_given", "prefix", "_prefix", "suffix", "_suffix", "period")

    assert(a.toSet == b.toSet)
  }
}


/*
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import com.databricks.labs.json2spark._

//val spark = SparkSession.builder().master("local[2]").config("spark.driver.bindAddress","127.0.0.1").getOrCreate()


import spark.implicits._


val x = new Json2Spark(Json2Spark.file2String("src/test/scala/resources/fhir.schema.json"), 
	defsLocation="definitions", 
	enforceRequiredField=false,
	circularReferences=Some(Seq("#/definitions/Extension", "#/definitions/Element", "#/definitions/Identifier", "#/definitions/Period","#/definitions/Reference")))

val s = new StructType(x.defs("Patient").toArray)

val s = new StructType(x.defs("HumanName").toArray) 
 */