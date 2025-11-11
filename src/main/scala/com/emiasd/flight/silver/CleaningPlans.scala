// com/emiasd/flight/silver/CleaningPlans.scala
package com.emiasd.flight.silver

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object CleaningPlans {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def deriveFlightsPlan(df: DataFrame): DataFrame =
    df // hook pour logique future

  def cleanFlights(df: DataFrame): DataFrame = {

    logger.info(
      "=== [CLEAN FLIGHTS] Préparation et nettoyage des données de vols ==="
    )

    // -----------------------------
    // Définition du coefficient alphaByYearCol
    // -----------------------------
    logger.info("Définition du coefficient alphaByYearCol")
    val alphaByYearCol = typedLit(
      Map(
        2009 -> 0.657,
        2010 -> 0.636,
        2011 -> 0.633,
        2012 -> 0.588,
        2013 -> 0.583
      )
    )
    logger.debug(s"alphaByYearCol défini avec ${alphaByYearCol.expr}")

    // -----------------------------
    // Filtrage des vols annulés ou déviés
    // -----------------------------
    logger.info("Filtrage des vols annulés ou déviés")
    val base = df
      .filter(
        coalesce(col("CANCELLED"), lit(0)) === 0 && coalesce(
          col("DIVERTED"),
          lit(0)
        ) === 0
      )
      .withColumn("WEATHER_DELAY", coalesce(col("WEATHER_DELAY"), lit(0.0)))
      .withColumn("NAS_DELAY", coalesce(col("NAS_DELAY"), lit(0.0)))
      .withColumn(
        "alpha_nas",
        coalesce(element_at(alphaByYearCol, col("year").cast("int")), lit(0.58))
      )
      .withColumn("NAS_WEATHER_DELAY", col("alpha_nas") * col("NAS_DELAY"))
      .withColumn(
        "total_weather_delay",
        col("WEATHER_DELAY") + col("NAS_WEATHER_DELAY")
      )
      .withColumn(
        "ARR_DELAY_NASW",
        col("ARR_DELAY_NEW") - (lit(1.0) - col("alpha_nas")) * col("NAS_DELAY")
      )

    base.select(
      // colonnes réellement présentes en bronze
      col("OP_CARRIER_AIRLINE_ID"),
      col("CRS_DEP_TIME"),
      col("ARR_DELAY_NEW"),
      col("origin_airport_id"),
      col("dest_airport_id"),
      // time features issues du bronze
      col("FL_DATE"),
      col("FL_NUM"),
      col("dep_ts_utc"),
      col("arr_ts_utc"),
      col("year"),
      col("month"),
      //
      col("WEATHER_DELAY"),
      col("NAS_DELAY"),
      col("NAS_WEATHER_DELAY"),
      col("alpha_nas"),
      col("total_weather_delay"),
      col("ARR_DELAY_NASW")
    )
  }

  def deriveWeatherPlan(df: DataFrame): DataFrame = df // placeholder

  def airportsOfInterest(f: DataFrame): DataFrame =
    f.select(col("origin_airport_id").as("airport_id"))
      .union(
        f.select(col("dest_airport_id").as("airport_id"))
      )
      .distinct()
}
