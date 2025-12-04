// com/emiasd/flight/join/BuildJT.scala
package com.emiasd.flight.join

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

import java.sql.Timestamp

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
  // 2) Attache une timeline météo horaire (13 points) à un DF de vols
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

    // Join en égalité (beaucoup plus léger que BETWEEN + Window)
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
        ).as(pointCol)): _*
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
  // 3) buildJT : façade simple (utilisée par JM)
  //    -> conserve la signature existante
  // ============================================================
  def buildJT(
    flightsEnriched: DataFrame,
    weatherSlim: DataFrame,
    thMinutes: Int,
    toleranceMin: Int = 20
  ): DataFrame = {
    val base = buildJTBase(flightsEnriched, weatherSlim, toleranceMin)
    attachLabel(base, thMinutes)
  }

  // ============================================================
  // 4) buildJTBase : pipeline commun vols/météo SANS seuil
  //    Version JM + pruning temporel ADR
  // ============================================================
  def buildJTBase(
    flightsEnriched: DataFrame,
    weatherSlim: DataFrame,
    toleranceMin: Int = 20
  ): DataFrame = {

    // 4.1 Filtre des vols avec timestamps complets + bornes temporelles
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

    // On enlève les colonnes auxiliaires et on réduit aux colonnes nécessaires
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

    // 4.2 Pruning temporel de la météo (optim ADR conservée)
    val weatherPruned = boundsOpt.map { case (minTs, maxTs) =>
      weatherSlim.filter(col("obs_utc").between(lit(minTs), lit(maxTs)))
    }
      .getOrElse(weatherSlim)

    // 4.3 Pré-agrégation météo compacte (algo JM)
    val wxHourly = buildWeatherPerHour(weatherPruned, toleranceMin)

    // 4.4 Origine : Wo
    val fForO = f.repartition(col("origin_airport_id"))

    val withWo = attachWeatherTimeline(
      fForO,
      wxHourly,
      airportColInF = "origin_airport_id",
      tsColInF = "dep_ts_utc",
      prefix = "o"
    )

    // 4.5 Destination : Wd
    val fForD = withWo.repartition(col("dest_airport_id"))

    val withWd = attachWeatherTimeline(
      fForD,
      wxHourly,
      airportColInF = "dest_airport_id",
      tsColInF = "arr_ts_utc",
      prefix = "d"
    )

    // 4.6 On peut libérer la météo agrégée à ce stade
    wxHourly.unpersist()

    withWd
  }

  // ============================================================
  // 5) attachLabel : applique le seuil de retard + struct F
  //    Signature et schéma identiques à ta version ADR actuelle
  // ============================================================
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
