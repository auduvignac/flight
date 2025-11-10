// com/emiasd/flight/silver/MappingWBAN.scala
package com.emiasd.flight.silver

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MappingWBAN {
  // mapping CSV : WBAN,airport_id,timezone (IANA, ex: America/New_York)
  def readMapping(spark: SparkSession, path: String): DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
      .withColumn("WBAN", upper(trim(col("WBAN"))))
      .withColumn("airport_id", col("airport_id").cast("int"))
      .withColumn("timezone", trim(col("timezone")))
      .dropDuplicates("WBAN")

  def airportTimezone(spark: SparkSession, path: String): DataFrame =
    // si le mapping contient aussi airport_id→timezone, on le normalise ici
    readMapping(spark, path)
      .select("airport_id", "timezone")
      .dropDuplicates("airport_id")
}
