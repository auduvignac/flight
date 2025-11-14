// com/emiasd/flight/io/Readers.scala
package com.emiasd.flight.io

import org.apache.spark.sql.{DataFrame, SparkSession}
import _root_.io.delta.tables.DeltaTable

object Readers {
  def exists(spark: SparkSession, path: String): Boolean = {
    DeltaTable.isDeltaTable(spark, path)
  }

  def readCsv(
    spark: SparkSession,
    paths: Seq[String],
    header: Boolean = true,
    infer: Boolean = true,
    sep: String = ","
  ): DataFrame =
    spark.read
      .option("header", header)
      .option("inferSchema", infer)
      .option("sep", sep)
      .csv(paths: _*)

  def readTxt(
    spark: SparkSession,
    paths: Seq[String],
    sep: String = ",",
    header: Boolean = true,
    infer: Boolean = true
  ): DataFrame =
    spark.read
      .option("sep", sep)
      .option("header", header)
      .option("inferSchema", infer)
      .csv(paths: _*)

  def readDelta(spark: SparkSession, path: String): DataFrame =
    spark.read.format("delta").load(path)
}
