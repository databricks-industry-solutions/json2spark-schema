lazy val scala212 = "2.12.8"
lazy val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "3.3.0")

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

//Spark+cats compatibility issues https://github.com/typelevel/cats/issues/3628
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll,
  ShadeRule.rename("cats.kernel.**" -> s"new_cats.kernel.@1").inAll
)

lazy val core = (project in file("."))
  .settings(
    name := "json2spark-schema",
    version := "0.0.3",
    scalacOptions += "-target:jvm-1.8",
    libraryDependencies ++= coreDependencies ++ projectDependencies ,
    assemblyJarName := s"${name.value}-${version.value}_assembly.jar",
    artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      s"${name.value}-${version.value}." + artifact.extension
    }
  )

