lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"
lazy val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "3.3.0")
lazy val publishVersion = "0.1.0"

ThisBuild / scalaVersion := sys.env.getOrElse("SCALA_VERSION", scala212)
ThisBuild / organization := "com.databricks"
ThisBuild / organizationName := "Databricks, Inc."

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
lazy val providedSparkDependencies = sparkDependencies.map(_ % "provided")

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.0" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests"
)

lazy val coreDependencies = (providedSparkDependencies ++ testDependencies )
val circeVersion = "0.14.1"
lazy val projectDependencies = Seq(
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
)

lazy val core = (project in file("."))
  .settings(
    name := "json2spark-schema",
    version := "0.0.1",
    scalacOptions += "-target:jvm-1.8",
    libraryDependencies ++= coreDependencies ++ projectDependencies ,
    artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      s"${name.value}-${version.value}." + artifact.extension
    }
  )

