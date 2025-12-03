// com/emiasd/flight/join/BuildJT.scala
package com.emiasd.flight.join

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

object BuildJT {

  // ============================================================
  // 1) Pré-agrégation météo : 1 observation par (airport_id, heure)
  // ============================================================
  private def buildWeatherPerHour(
                                   weatherSlim: DataFrame,
                                   toleranceMin: Int
                                 ): DataFrame = {

    val tolSeconds = toleranceMin * 60

    // On bucketise les observations à l'heure la plus proche
    val wxWithHour = weatherSlim
      .withColumn("obs_hour", date_trunc("HOUR", col("obs_utc")))
      .withColumn(
        "abs_diff_sec",
        abs(col("obs_utc").cast("long") - col("obs_hour").cast("long"))
      )

    // Pour chaque (airport_id, obs_hour) on garde l'observation la plus proche
    val w = Window
      .partitionBy(col("airport_id"), col("obs_hour"))
      .orderBy(col("abs_diff_sec").asc)

    val wxHourly = wxWithHour
      .withColumn("rn", row_number().over(w))
      .filter(col("rn") === 1 && col("abs_diff_sec") <= lit(tolSeconds))
      .drop("rn", "abs_diff_sec")
      // Layout optimisé pour les joins suivants
      .repartition(col("airport_id"))
      .sortWithinPartitions(col("airport_id"), col("obs_hour"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    wxHourly
  }

  // ============================================================
  // 2) Attache une timeline météo horaire (13 points) à un DataFrame de vols
  //    -> produit Wo ou Wd sous forme d'array<struct>
  // ============================================================
  private def attachWeatherTimeline(
                                     fBase: DataFrame,
                                     wxHourly: DataFrame,
                                     airportColInF: String,
                                     tsColInF: String,
                                     prefix: String
                                   ): DataFrame = {

    val targetsColName = s"${prefix}_target_hours"
    val targetColName  = s"${prefix}_target_hour"

    // Grille de 13 heures : [ts_hour - 12h, ..., ts_hour]
    val fWithGrid = fBase
      .withColumn(
        targetsColName,
        sequence(
          date_trunc("HOUR", col(tsColInF)) - expr("INTERVAL 12 HOURS"),
          date_trunc("HOUR", col(tsColInF)),
          expr("INTERVAL 1 HOURS")
        )
      )
      .withColumn(targetColName, explode(col(targetsColName)))
      .drop(targetsColName)

    // On alias les deux côtés pour éviter toute ambiguïté (year, month, etc.)
    val fAliased  = fWithGrid.alias("f")
    val wxAliased = wxHourly.alias("w")

    // Join en égalité (beaucoup plus léger que BETWEEN)
    val joined = fAliased.join(
      wxAliased,
      col(s"f.$airportColInF") === col("w.airport_id") &&
        col(s"f.$targetColName") === col("w.obs_hour"),
      "left"
    )

    val pointCol = s"${prefix}_point"

    // Colonnes de base du vol (sans suffixe, on ne garde que celles de fBase)
    val baseCols: Seq[Column] =
      fBase.columns.map(c => col(s"f.$c").as(c))

    // On construit un "point" météo pour chaque (vol, heure cible)
    val withPoint = joined.select(
      (baseCols :+
        struct(
          col(s"f.$targetColName").as(s"${prefix}_ts"),
          col("w.SkyCondition").as(s"${prefix}_sky"),
          col("w.WeatherType").as(s"${prefix}_wxType"),
          col("w.Visibility").cast("double").as(s"${prefix}_vis"),
          col("w.TempC").cast("double").as(s"${prefix}_tempC"),
          col("w.DewPointC").cast("double").as(s"${prefix}_dewC"),
          col("w.RelativeHumidity").cast("double").as(s"${prefix}_rh"),
          col("w.WindSpeedKt").cast("double").as(s"${prefix}_windKt"),
          col("w.WindDirection").cast("double").as(s"${prefix}_windDir"),
          col("w.Altimeter").cast("double").as(s"${prefix}_altim"),
          col("w.SeaLevelPressure").cast("double").as(s"${prefix}_slp"),
          col("w.StationPressure").cast("double").as(s"${prefix}_stnp"),
          col("w.HourlyPrecip").cast("double").as(s"${prefix}_precip")
        ).as(pointCol)
        ): _*
    )

    // Recomposition de l'array : (ts, ts-1h, ..., ts-12h)
    val outCol = if (prefix == "o") "Wo" else "Wd"

    withPoint
      .groupBy(fBase.columns.map(col): _*)
      .agg(
        sort_array(
          collect_list(col(pointCol)),
          asc = false // ts, ts-1h, ..., ts-12h
        ).as(outCol)
      )
  }

  // ============================================================
  // 3) BuildJT : orchestration globale (signature et schéma JT identiques)
  // ============================================================
  def buildJT(
               flightsEnriched: DataFrame,
               weatherSlim: DataFrame,
               thMinutes: Int,
               toleranceMin: Int = 20
             ): DataFrame = {

    // 1) Pré-agrégation météo compacte
    val wxHourly = buildWeatherPerHour(weatherSlim, toleranceMin)

    // 2) Préparation vols (comme avant, mais plus simple)
    val f0 = flightsEnriched
      .filter(col("dep_ts_utc").isNotNull && col("arr_ts_utc").isNotNull)

    // 3) Origine : Wo
    val fForO = f0.repartition(col("origin_airport_id"))

    val withWo = attachWeatherTimeline(
      fForO,
      wxHourly,
      airportColInF = "origin_airport_id",
      tsColInF = "dep_ts_utc",
      prefix = "o"
    )

    // 4) Destination : Wd
    val fForD = withWo.repartition(col("dest_airport_id"))

    val withWd = attachWeatherTimeline(
      fForD,
      wxHourly,
      airportColInF = "dest_airport_id",
      tsColInF = "arr_ts_utc",
      prefix = "d"
    )

    // On peut libérer la météo agrégée à ce stade
    wxHourly.unpersist()

    // 5) Étiquette C
    val withC = withWd.withColumn(
      "C",
      when(col("ARR_DELAY_NEW") >= lit(thMinutes), lit(1)).otherwise(lit(0))
    )

    // 6) Struct F + colonnes finales (schéma identique à ta version actuelle)
    val withF = withC.select(
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

    withF
  }
}
