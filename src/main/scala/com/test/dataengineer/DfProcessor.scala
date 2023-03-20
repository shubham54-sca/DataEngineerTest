package com.test.dataengineer

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DfProcessor {

  def createSparkSession(accessKey: String, secretKey: String) = {
    SparkSession.builder()
      .appName("DataEngineerTest")
      .config("spark.master", "local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.fast.upload", "true") // multiple file transfer in parallel
      .config("spark.hadoop.fs.s3a.access.key", accessKey)
      .config("spark.hadoop.fs.s3a.secret.key", secretKey)
      .getOrCreate()
  }

  /**
   * Algorithm work as following
   * 1. fill = 0 if there is any null value in the dataset
   * 2. group by is used to collect the identical data into groups on dataframe and performs aggregation function on the group data
   * 3. count => return the count of rows for each group
   * 4. filter => filter out even number so will get odd count for the key
   * 5. select => select the column required to stored in the output path
   * @param df
   * @return
   */
  def processDF(df: DataFrame) = {
    df.na.fill(0)
      .groupBy("key", "value")
      .agg(count("*").as("count"))
      .filter(col("count") % 2 =!= 0)
      .select("key", "count")
  }

  /**
   *
   * @param inputDirectoryPath
   * @param spark
   * @return
   */
  def readInputFiles(inputDirectoryPath: String)(implicit spark: SparkSession) = {
    import FileExtension._

    val customSchema = StructType(Array(
      StructField("key", IntegerType),
      StructField("value", IntegerType)
    ))

    getFileTypes.map { ext =>
      spark.read.format("csv").options(Map("sep" -> getSeparator(ext), "header" -> "true"))
        .schema(customSchema)
        .csv(s"$inputDirectoryPath/*.$ext")
    }.reduceLeft(_ union _)
  }

  /**
   *
   * @param outputPath
   * @param df
   */
  def writeFile(outputPath: String, df: DataFrame) = {
    df.write.format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("sep", "\t")
      .save(outputPath)
  }

  /**
   * It Handle file separator based on file type. It is very useful in case of scaling the code. If in future other file types will come into the input folder
   * then just need to add file type and separator in this singleton object
   */
  object FileExtension {
    val CSV = "csv"
    val TSV = "tsv"

    def getFileTypes = List(CSV, TSV)

    def getSeparator(ext: String) = {
      ext match {
        case CSV => ","
        case TSV => "\t"
        case _ => throw new RuntimeException("Unknown file Extension")
      }
    }
  }
}
