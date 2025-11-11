// com/emiasd/flight/silver/CleaningPlans.scala
package com.emiasd.flight.silver

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object CleaningPlans {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def deriveFlightsPlan(df: DataFrame): DataFrame = df // hook pour logique future

  def cleanFlights(df: DataFrame): DataFrame = {

    logger.info("=== [CLEAN FLIGHTS] Préparation et nettoyage des données de vols ===")

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
    val filtered = df.filter(
      coalesce(col("CANCELLED"), lit(0)) === 0 && coalesce(col("DIVERTED"), lit(0)) === 0
    )

    // -----------------------------
    // Gestion des valeurs manquantes
    // -----------------------------
    logger.info("Gestion des valeurs manquantes pour WEATHER_DELAY et NAS_DELAY")
    val cleaned = filtered
      .withColumn("WEATHER_DELAY", coalesce(col("WEATHER_DELAY"), lit(0.0)))
      .withColumn("NAS_DELAY", coalesce(col("NAS_DELAY"), lit(0.0)))

    // -----------------------------
    // Pondération NAS et calcul du total weather delay
    // -----------------------------
    logger.info("Étape de pondération des retards dus au NAS selon l'année du vol")
    val enriched = cleaned
      .withColumn(
        "alpha_nas",
        coalesce(element_at(alphaByYearCol, col("year").cast("int")), lit(0.58))
      )
      .withColumn("total_weather_delay", col("WEATHER_DELAY") + col("alpha_nas") * col("NAS_DELAY"))

    // -----------------------------
    // Création des étiquettes de retard météo
    // -----------------------------
    logger.info("Création des labels binaires de retard météo (15, 30, 45, 60, 90 min)")
    val labeled = enriched
      .withColumn("label_wx_15", (col("total_weather_delay") >= 15).cast("int"))
      .withColumn("label_wx_30", (col("total_weather_delay") >= 30).cast("int"))
      .withColumn("label_wx_45", (col("total_weather_delay") >= 45).cast("int"))
      .withColumn("label_wx_60", (col("total_weather_delay") >= 60).cast("int"))
      .withColumn("label_wx_90", (col("total_weather_delay") >= 90).cast("int"))

    // -----------------------------
    // Sélection finale des colonnes
    // -----------------------------
    logger.info("Sélection finale des colonnes nettoyées et enrichies")

    val result = labeled.select(
      col("OP_CARRIER_AIRLINE_ID"),
      col("CRS_DEP_TIME"),
      col("ARR_DELAY_NEW"),
      col("origin_airport_id"),
      col("dest_airport_id"),
      col("FL_DATE"),
      col("FL_NUM"),
      col("dep_ts_utc"),
      col("arr_ts_utc"),
      col("year"),
      col("month"),
      col("WEATHER_DELAY"),
      col("NAS_DELAY"),
      col("alpha_nas"),
      col("total_weather_delay"),
      col("label_wx_15"),
      col("label_wx_30"),
      col("label_wx_45"),
      col("label_wx_60"),
      col("label_wx_90")
    )

    logger.info(s"Nettoyage terminé — ${result.count()} lignes prêtes pour la phase suivante.")
    result
  }

  def deriveWeatherPlan(df: DataFrame): DataFrame = df // placeholder

  def airportsOfInterest(f: DataFrame): DataFrame =
    f.select(col("origin_airport_id").as("airport_id"))
      .union(
        f.select(col("dest_airport_id").as("airport_id"))
      )
      .distinct()
}
