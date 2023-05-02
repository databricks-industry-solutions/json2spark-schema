package com.databricks.labs.json2spark

import org.apache.spark.sql.types._
import io.circe._, io.circe.parser._
import scala.reflect.runtime.universe._

/*
 * Main entry point
 */
object Json2Spark{

  def file2String(fqPath: String): String = {
    scala.io.Source.fromFile(fqPath).mkString
  }

  /*
   * A list of required fields, None otherwise
   */
  def requiredFields(c: ACursor): Option[Seq[String]] = {
    c.downField("required").as[Seq[String]] match {
      case Left(e) => None
      case Right(v) => Some(v)
    }
  }

  /*
   * Returns current path of cursor in Json schema
   */
  def cursorPath(c: ACursor): String = {
    c.key match {
      case None => "#" //base case
      case Some(entry) => cursorPath(c.up) + "/" + entry
      case _ => ??? 
    }
  }

  /*
   * Metadata is the full path of the cursor(lineage).
   *  In Spark's ArrayType, this cannot be populated and therefore must be maintained from the parent
   *    e.g. why we need to pass in additional param (a) to maintain lineage
   */
  def metadata(path: String, description: String=""): Metadata = {
    Metadata.fromJson("""
       {
         "path": """" + path + """",
         "description": """ + Literal(Constant(description)).toString.replace("\\'", "") + """
       }
      """)
  }

  val TypeMapping = Map(
    "string" -> StringType,
    "decimal" -> DecimalType,
    "number" -> DoubleType,
    "float" -> FloatType,
    "integer" -> LongType,
    "boolean" -> BooleanType,
    "timestamp" -> DataTypes.TimestampType
  )
}


class Json2Spark(rawJson: String,
  enforceRequiredField: Boolean = true,
  defaultType: String = "string",
  defsLocation: String = "$def",
  circularReferences: Option[Seq[String]] = None ){


  /*
   * Schema as a json object
   */
  val json = parse(rawJson) match {
    case Left(e) => throw new Exception("Invalid JSON string: " + e)
    case Right(v) => v
  }


  /*
   * Find circular references given a resource
   */
  def circularReferences(resourcePath: String, visitedNodes: Seq[String]) = Seq[String]{
    val c = cursorAt(resourcePath)

    ???
  }

  /*
   * See if the specified field name is required
   */
  def nullable(fieldName: String, rf: Option[Seq[String]]): Boolean = {
    rf match {
      case Some(x) =>
        x.contains(fieldName) || !enforceRequiredField
      case None => true
     }
  }



  /*
   * Return relevant struct for object referenced 
   */
  def convert2Spark: StructType = {
    json.hcursor.downField("properties").keys match {
      case Some(x) => StructType(
        x.map(fieldName =>
          property2Struct(json.hcursor.downField("properties").downField(fieldName),
            fieldName,
            Json2Spark.cursorPath(json.hcursor.downField("properties").downField(fieldName)),
            Json2Spark.requiredFields(json.hcursor))
        )
          .reduce( (a, b) => a ++ b )
      )
      case None => throw new Exception("No properties found in json schema")
    }
  }

  def property2Struct(c: ACursor, fieldName: String, path: String, requiredFields: Option[Seq[String]] = None): Seq[StructField] = {
    c.keys match {
      case Some(x) if isCircularReference(c) =>  Nil 
      case Some(x) if x.toSeq.contains("const") => Nil //const not supported in spark schema
      case Some(x) if x.toSeq.contains("$ref") => fieldName match {
        case "" => refs( c.downField("$ref").as[String].getOrElse(""), path, fieldName)
        case _ =>  refs( c.downField("$ref").as[String].getOrElse(""), path, fieldName) match {
          case x if x.size == 1 => x
          case x =>  Seq(StructField(fieldName, StructType(x)))
        }
      }

      case Some(x) if x.toSeq.contains("enum")  =>
        new StructType()
          .add(fieldName,
            StringType,
            nullable(fieldName, requiredFields),
            Json2Spark.metadata(path, c.downField("description").as[String].getOrElse("")))
      case Some(x) if x.toSeq.contains("type") =>
        c.downField("type").as[String].getOrElse(defaultType) match {
          case "string" | "number" | "float" | "integer" | "boolean" =>
            Seq(
              new StructField(
                fieldName,
                Json2Spark.TypeMapping.get(c.downField("type").as[String].getOrElse(defaultType)).getOrElse(StringType).asInstanceOf[DataType],
                nullable(fieldName, requiredFields),
                Json2Spark.metadata(path,c.downField("description").as[String].getOrElse("") )
              ))
          case "array" =>
            property2Struct(c.downField("items"), "", path + "/items", Json2Spark.requiredFields(c.downField("items"))) match {
              case Nil =>
                Seq(
                  StructField(
                    fieldName,
                    ArrayType(Json2Spark.TypeMapping.get(defaultType).getOrElse(StringType).asInstanceOf[DataType]),
                    nullable(fieldName, requiredFields),
                    Json2Spark.metadata(path,c.downField("description").as[String].getOrElse("") ))
                )
              case x if x.size == 1 =>
                Seq(
                  StructField(
                    fieldName,
                    ArrayType(x(0).dataType),
                    nullable(fieldName, requiredFields),
                    Json2Spark.metadata(path,c.downField("description").as[String].getOrElse("") ))
                )
              case x if x.size > 1 =>
                Seq(
                  StructField(
                    fieldName,
                    ArrayType(new StructType(x.toArray)),
                    nullable(fieldName, requiredFields),
                    Json2Spark.metadata(path,c.downField("description").as[String].getOrElse("") ))
                )
            }
          case "object" =>
            new StructType({
              c.downField("properties").keys match {
                case Some(x) =>
                  x.map(fn =>
                    property2Struct(c.downField("properties").downField(fn),
                      fn,
                      path + "/properties/" + fn,
                      Json2Spark.requiredFields(c)))
                    .reduce( (a,b) => a ++ b ).toArray
                    case None => throw new Exception("No properties found in json schema nested object")
              }
            }).fields
        }
      case x =>
        Seq(
          StructField(
            fieldName,
            Json2Spark.TypeMapping.get(defaultType).getOrElse(StringType).asInstanceOf[DataType],
            nullable(fieldName, requiredFields),
            Json2Spark.metadata(path,c.downField("description").as[String].getOrElse("")))
        )
    }
  }

  def isCircularReference(c: ACursor): Boolean = {
    circularReferences match {
      case Some(x) if x.contains(Json2Spark.cursorPath(c)) => true
      case _ => false
    }
  }

  def cursorAt(path: String): ACursor = {
    var c = json.hcursor.asInstanceOf[ACursor]
    for ( y <- path.split('/').drop(1) ) c = c.downField(y)
    c
  }

  /*
   * Returns a struct from a "$refs" mapping
   *  (only supporting local refs now, e.g. begins with #
   */
  def refs(resourcePath: String, basePath: String, fieldName: String): Seq[StructField] = {
    val c = cursorAt(resourcePath)
    property2Struct(c, fieldName, basePath + "/" + "$ref//" + resourcePath ,Json2Spark.requiredFields(c))
  }

  /*
   * Returns a struct from a "$defs" mapping
   */
  def defs(resourceDefinition: String): Seq[StructField] = {
    json.hcursor.downField(defsLocation).downField(resourceDefinition) match{
      case x if x.succeeded => property2Struct(x, resourceDefinition, "#/" + defsLocation + "/" + resourceDefinition, Json2Spark.requiredFields(x))
      case _ => new StructType //Do not fail on definition not found
//      case _ => throw new Exception("resource definition not found " + resourceDefinition + "\nAt location /" + defsLocation + "/" + resourceDefinition)
    }
  }
}
