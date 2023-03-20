package com.test.dataengineer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite

class DfProcessorSuite extends AnyFunSuite {

  val testSparkSession = SparkSession.builder()
    .appName("DataEngineerTest")
    .config("spark.master", "local")
    .getOrCreate()

  import testSparkSession.implicits._

  val df = Seq((1, 2), (1, 3), (1, 3), (2, 4), (2, 4)).toDF("key", "value")

  test("Filter Odd Number of Values") {
    val processDf = DfProcessor.processDF(df)
    assert(processDf.filter(col("count") % 2 === 0).count() === 0)
  }

}
