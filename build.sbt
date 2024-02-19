ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "spark-scala-project",
    idePackagePrefix := Some("tomasz.spark_project")
  )


val SparkVersion = "3.5.0"

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion,
  "org.apache.spark" %% "spark-sql" % SparkVersion
)
