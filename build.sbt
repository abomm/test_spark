name := "test_spark"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Versions.spark,
  "org.apache.spark" %% "spark-mllib" % Versions.spark

)