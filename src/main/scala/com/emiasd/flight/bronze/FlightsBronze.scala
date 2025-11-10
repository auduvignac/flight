// com/emiasd/flight/bronze/FlightsBronze.scala
package com.emiasd.flight.bronze

import com.emiasd.flight.io.Readers
import com.emiasd.flight.util.DFUtils._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object FlightsBronze {
  def readAndEnrich(
    spark: SparkSession,
    flightInputs: Seq[String],
    wbanTzPath: String
  ): DataFrame = {

    val logger = Logger.getLogger(getClass.getName)

    logger.info(s"Lecture du fichier : $flightInputs")

    val raw = Readers.readCsv(spark, flightInputs)

    val tz = Readers
      .readCsv(spark, Seq(wbanTzPath))
      .select(
        col("AirportID").cast("int").as("airport_id"),
        col("TimeZone").cast("int").as("tz_hour")
      )
      .dropDuplicates("airport_id")

    val f1 = raw
      .withColumn("OP_CARRIER_AIRLINE_ID", col("OP_CARRIER_AIRLINE_ID").cast("int"))
      .withColumn("OP_CARRIER_FL_NUM", col("OP_CARRIER_FL_NUM").cast("int"))
      .withColumn("ORIGIN_AIRPORT_ID", col("ORIGIN_AIRPORT_ID").cast("int"))
      .withColumn("DEST_AIRPORT_ID", col("DEST_AIRPORT_ID").cast("int"))
      .withColumn("CRS_DEP_TIME", col("CRS_DEP_TIME").cast("int"))
      .withColumn("CRS_ELAPSED_TIME", col("CRS_ELAPSED_TIME").cast("double"))
      .withColumn("ARR_DELAY_NEW", col("ARR_DELAY_NEW").cast("double"))
      .withColumn("CANCELLED", col("CANCELLED").cast("int"))
      .withColumn("DIVERTED", col("DIVERTED").cast("int"))
      .withColumn("WEATHER_DELAY", col("WEATHER_DELAY").cast("double"))
      .withColumn("NAS_DELAY", col("NAS_DELAY").cast("double"))

    // tz origine + destination
    val f2 = f1
      .join(tz.as("tzO"), col("ORIGIN_AIRPORT_ID") === col("tzO.airport_id"), "left")
      .withColumnRenamed("tz_hour", "origin_tz")
      .drop(col("tzO.airport_id")) // << important: col(...) et pas string
      .join(tz.as("tzD"), col("DEST_AIRPORT_ID") === col("tzD.airport_id"), "left")
      .withColumnRenamed("tz_hour", "dest_tz")
      .drop(col("tzD.airport_id")) // << idem

    // départ local (origine)
    val f3 = f2.withColumn("CRS_DEP_TS_LOCAL", parseLocal(col("FL_DATE"), col("CRS_DEP_TIME")))

    // UTC : dep_utc = dep_local - origin_tz (heures), arr_utc = dep_utc + elapsed
    val f4 = f3
      .withColumn("elapsed_min", coalesce(col("CRS_ELAPSED_TIME").cast("int"), lit(0)))
      .withColumn("dep_ts_utc", addMinutes(col("CRS_DEP_TS_LOCAL"), -col("origin_tz") * 60))
      .withColumn("arr_ts_utc", addMinutes(col("dep_ts_utc"), col("elapsed_min")))
      .withColumn("year", year(col("FL_DATE")))
      .withColumn("month", date_format(col("FL_DATE"), "MM"))
      .withColumnRenamed("OP_CARRIER_FL_NUM", "FL_NUM")
      .withColumnRenamed("ORIGIN_AIRPORT_ID", "origin_airport_id")
      .withColumnRenamed("DEST_AIRPORT_ID", "dest_airport_id")
      // Sécurité : s'il restait un airport_id résiduel pour une raison X, on l’élimine.
      .drop("airport_id")

    f4

  }
}
