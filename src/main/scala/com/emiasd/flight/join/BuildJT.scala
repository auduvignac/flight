// com/emiasd/flight/join/BuildJT.scala
package com.emiasd.flight.join

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp

object BuildJT {

  private def hourlyArrayClosest(
    fBase: DataFrame,
    wxPrefixed: DataFrame,
    airportColInF: String,
    tsColInF: String,
    prefix: String,
    toleranceMin: Int
  ): DataFrame = {

    // 1) Grille horaire: 13 timestamps cibles [ts-12h .. ts] pas 1h
    val targetsColName = s"${prefix}_targets"
    val targetColName  = s"${prefix}_target"

    val fWithGrid = fBase
      .withColumn(
        targetsColName,
        sequence(
          col(tsColInF) - expr("INTERVAL 12 HOURS"),
          col(tsColInF),
          expr("INTERVAL 1 HOURS")
        )
      )
      .withColumn(targetColName, explode(col(targetsColName)))
      .drop(col(targetsColName))

    // 2) Candidats météo dans la tolérance ±toleranceMin
    val joined = fWithGrid.join(
      wxPrefixed,
      col(airportColInF) === col(s"${prefix}_airport_id") &&
        col("obs_utc").between(
          col(s"${prefix}_target") - expr(s"INTERVAL ${toleranceMin} MINUTES"),
          col(s"${prefix}_target") + expr(s"INTERVAL ${toleranceMin} MINUTES")
        ),
      "left"
    )

    // 3) Garder l'observation la plus proche pour chaque (vol, heure cible)
    val partCols: Seq[Column] =
      fBase.columns.map(col) :+ col(s"${prefix}_target")
    val w = Window
      .partitionBy(partCols: _*)
      .orderBy(
        abs(col("obs_utc").cast("long") - col(s"${prefix}_target").cast("long"))
      )

    val pointCol = s"${prefix}_point"
    val best = joined
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") === 1)
      .select(
        (fBase.columns.map(col) :+
          struct(
            col(s"${prefix}_target").as(s"${prefix}_ts"),
            col("SkyCondition").as(s"${prefix}_sky"),
            col("WeatherType").as(s"${prefix}_wxType"),
            col("Visibility").cast("double").as(s"${prefix}_vis"),
            col("TempC").cast("double").as(s"${prefix}_tempC"),
            col("DewPointC").cast("double").as(s"${prefix}_dewC"),
            col("RelativeHumidity").cast("double").as(s"${prefix}_rh"),
            col("WindSpeedKt").cast("double").as(s"${prefix}_windKt"),
            col("WindDirection").cast("double").as(s"${prefix}_windDir"),
            col("Altimeter").cast("double").as(s"${prefix}_altim"),
            col("SeaLevelPressure").cast("double").as(s"${prefix}_slp"),
            col("StationPressure").cast("double").as(s"${prefix}_stnp"),
            col("HourlyPrecip").cast("double").as(s"${prefix}_precip")
          ).as(pointCol)): _*
      )

    // 4) Recomposer l'array trié (ts, ts-1h, ..., ts-12h)
    val outCol = if (prefix == "o") "Wo" else "Wd"
    best
      .groupBy(fBase.columns.map(col): _*)
      .agg(sort_array(collect_list(col(pointCol)), asc = false).as(outCol))
  }

  def buildJT(
    flightsEnriched: DataFrame,
    weatherSlim: DataFrame,
    thMinutes: Int,
    toleranceMin: Int = 20
  ): DataFrame = {
    val base = buildJTBase(flightsEnriched, weatherSlim, toleranceMin)
    attachLabel(base, thMinutes)
  }

  /**
   * Pipeline commun vols/météo sans application du seuil. Optimisations pour un
   * run unique : filtre temporel météo et pruning de colonnes.
   */
  def buildJTBase(
    flightsEnriched: DataFrame,
    weatherSlim: DataFrame,
    toleranceMin: Int = 20
  ): DataFrame = {
    val fWithBounds = flightsEnriched
      .filter(col("dep_ts_utc").isNotNull && col("arr_ts_utc").isNotNull)
      .withColumn(
        "dep_minus_12h",
        col("dep_ts_utc") - expr("INTERVAL 12 HOURS")
      )
      .withColumn(
        "arr_plus_12h",
        col("arr_ts_utc") + expr("INTERVAL 12 HOURS")
      )

    val boundsOpt = fWithBounds
      .agg(
        min(col("dep_minus_12h")).as("min_ts"),
        max(col("arr_plus_12h")).as("max_ts")
      )
      .collect()
      .headOption
      .flatMap { row =>
        val minTs = Option(row.getAs[Timestamp]("min_ts"))
        val maxTs = Option(row.getAs[Timestamp]("max_ts"))
        for {
          min <- minTs
          max <- maxTs
        } yield (min, max)
      }

    val f = fWithBounds
      .drop("dep_minus_12h", "arr_plus_12h")
      .select(
        col("origin_airport_id"),
        col("dest_airport_id"),
        col("dep_ts_utc"),
        col("arr_ts_utc"),
        col("ARR_DELAY_NEW"),
        col("WEATHER_DELAY"),
        col("NAS_DELAY"),
        col("NAS_WEATHER_DELAY"),
        col("OP_CARRIER_AIRLINE_ID"),
        col("FL_NUM"),
        col("FL_DATE"),
        col("CRS_DEP_TIME"),
        col("flight_key"),
        col("year"),
        col("month")
      )

    val weatherPruned = boundsOpt.map { case (minTs, maxTs) =>
      weatherSlim.filter(col("obs_utc").between(lit(minTs), lit(maxTs)))
    }
      .getOrElse(weatherSlim)

    // ========= OPTIMS MÉTÉO =========
    val wxBase = weatherPruned
      .repartition(col("airport_id"))
      .sortWithinPartitions(col("airport_id"), col("obs_utc"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val wxOrigin = wxBase.select(
      col("airport_id").as("o_airport_id"),
      col("obs_utc"),
      col("SkyCondition"),
      col("WeatherType"),
      col("Visibility"),
      col("TempC"),
      col("DewPointC"),
      col("RelativeHumidity"),
      col("WindSpeedKt"),
      col("WindDirection"),
      col("Altimeter"),
      col("SeaLevelPressure"),
      col("StationPressure"),
      col("HourlyPrecip")
    )

    val wxDest = wxBase.select(
      col("airport_id").as("d_airport_id"),
      col("obs_utc"),
      col("SkyCondition"),
      col("WeatherType"),
      col("Visibility"),
      col("TempC"),
      col("DewPointC"),
      col("RelativeHumidity"),
      col("WindSpeedKt"),
      col("WindDirection"),
      col("Altimeter"),
      col("SeaLevelPressure"),
      col("StationPressure"),
      col("HourlyPrecip")
    )

    // Répartition pour limiter le shuffle
    val fForO = f.repartition(col("origin_airport_id"))

    // ========= ORIGINE : Wo (closest hourly) =========
    val withWo = hourlyArrayClosest(
      fForO,
      wxOrigin.withColumnRenamed("o_airport_id", "o_airport_id"),
      airportColInF = "origin_airport_id",
      tsColInF = "dep_ts_utc",
      prefix = "o",
      toleranceMin = toleranceMin
    )

    val fForD = withWo.repartition(col("dest_airport_id"))

    // ========= DESTINATION : Wd (closest hourly) =========
    val withWd = hourlyArrayClosest(
      fForD,
      wxDest.withColumnRenamed("d_airport_id", "d_airport_id"),
      airportColInF = "dest_airport_id",
      tsColInF = "arr_ts_utc",
      prefix = "d",
      toleranceMin = toleranceMin
    )

    wxBase.unpersist()

    withWd
  }

  /**
   * Applique le seuil de retard et recompose la sortie finale (F/Wo/Wd/C).
   * Séparé pour pouvoir réutiliser le résultat de buildJTBase sur plusieurs th.
   */
  def attachLabel(jtBase: DataFrame, thMinutes: Int): DataFrame = {
    val withC = jtBase.withColumn(
      "C",
      when(col("ARR_DELAY_NEW") >= lit(thMinutes), lit(1)).otherwise(lit(0))
    )

    withC.select(
      struct(
        col("OP_CARRIER_AIRLINE_ID").as("carrier"),
        col("FL_NUM").as("flnum"),
        col("FL_DATE").as("date"),
        col("origin_airport_id"),
        col("dest_airport_id"),
        col("CRS_DEP_TIME").as("crs_dep_scheduled_hhmm"),
        col("dep_ts_utc"),
        col("arr_ts_utc"),
        col("ARR_DELAY_NEW").as("arr_delay_new"),
        col("WEATHER_DELAY").as("weather_delay"),
        col("NAS_DELAY").as("nas_delay"),
        col("NAS_WEATHER_DELAY").as("nas_weather_delay"),
        col("flight_key")
      ).as("F"),
      col("Wo"),
      col("Wd"),
      col("C"),
      col("year"),
      col("month")
    )
  }
}
