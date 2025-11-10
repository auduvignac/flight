// com/emiasd/flight/silver/CleaningPlans.scala
package com.emiasd.flight.silver

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object CleaningPlans {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def deriveFlightsPlan(df: DataFrame): DataFrame = df // hook pour logique future


  def cleanFlights(df: DataFrame, plan: DataFrame): DataFrame = {

    logger.info("=== [CLEAN FLIGHTS] Préparation et nettoyage des données de vols ===")

    val alphaByYearCol = typedLit(Map(
      2009 -> 0.657, 2010 -> 0.636, 2011 -> 0.633,
      2012 -> 0.588, 2013 -> 0.583
    ))

    val base = df
      .filter(coalesce(col("CANCELLED"), lit(0)) === 0 && coalesce(col("DIVERTED"), lit(0)) === 0)
      .withColumn("WEATHER_DELAY", coalesce(col("WEATHER_DELAY"), lit(0.0)))
      .withColumn("NAS_DELAY",     coalesce(col("NAS_DELAY"),     lit(0.0)))
      .withColumn("alpha_nas", coalesce(element_at(alphaByYearCol, col("year").cast("int")), lit(0.58)))
      .withColumn("total_weather_delay", col("WEATHER_DELAY") + col("alpha_nas") * col("NAS_DELAY"))
      // ONE HOT ENCODING de la colonne total_weather_delay
      .withColumn("label_wx_15", (col("total_weather_delay") >= 15).cast("int"))
      .withColumn("label_wx_30", (col("total_weather_delay") >= 30).cast("int"))
      .withColumn("label_wx_45", (col("total_weather_delay") >= 45).cast("int"))
      .withColumn("label_wx_60", (col("total_weather_delay") >= 60).cast("int"))
      .withColumn("label_wx_90", (col("total_weather_delay") >= 90).cast("int"))

    base.select(
      // colonnes réellement présentes en bronze
      col("OP_CARRIER_AIRLINE_ID"),
      col("CRS_DEP_TIME"),
      col("ARR_DELAY_NEW"),
      col("origin_airport_id"), col("dest_airport_id"),
      // time features issues du bronze
      col("FL_DATE"), col("FL_NUM"),
      col("dep_ts_utc"), col("arr_ts_utc"),
      col("year"), col("month"),
      //
      col("WEATHER_DELAY"), col("NAS_DELAY"), col("alpha_nas"), col("total_weather_delay"),
      col("label_wx_15"), col("label_wx_30"), col("label_wx_45"), col("label_wx_60"), col("label_wx_90")
    )
  }



  def deriveWeatherPlan(df: DataFrame, missingnessThreshold: Double): DataFrame = df // placeholder


  def airportsOfInterest(f: DataFrame): DataFrame = {
    f.select(col("origin_airport_id").as("airport_id")).union(
      f.select(col("dest_airport_id").as("airport_id"))
    ).distinct()
  }
}