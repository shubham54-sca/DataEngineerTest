ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "DataEngineerTest"
  )
mainClass in  Compile := Some("com.test.dataengineer.TestApp")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
val sparkVersion = "3.2.2"
val scalatestVersion ="3.2.15"
val awsHadoopVersion = "3.2.1"
val googleGuava = "30.0-jre"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
  "org.apache.hadoop" % "hadoop-aws" % awsHadoopVersion,
  "com.google.guava" % "guava" %  googleGuava
)

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

enablePlugins(JavaAppPackaging)
