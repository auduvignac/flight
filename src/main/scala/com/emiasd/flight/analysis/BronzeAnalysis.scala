package com.emiasd.flight.analysis

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BronzeAnalysis {

  val logger = Logger.getLogger(getClass.getName)

  /** Expression “valeur manquante” adaptée au type */
  private def missingExpr(colName: String, dt: DataType) = {
    val c = col(colName)
    dt match {
      case DoubleType | FloatType =>
        c.isNull || isnan(c)
      case StringType =>
        c.isNull || trim(c) === "" || lower(c) === "na" || lower(c) === "null"
      case _ =>
        c.isNull
    }
  }

  /** Comptage des valeurs manquantes par colonne (NaN/NULL/vides selon le type) */
  def nullsReport(df: DataFrame): DataFrame = {
    val exprs = df.schema.fields.map { f =>
      sum(when(missingExpr(f.name, f.dataType), 1).otherwise(0)).as(f.name)
    } :+ count(lit(1)).as("_rows")
    df.agg(exprs.head, exprs.tail: _*)
  }

  /** Comptage d’uniques sur un sous-ensemble de colonnes */
  def uniquesReport(df: DataFrame, cols: Seq[String], approx: Boolean = true): DataFrame = {
    val exprs = cols.filter(df.columns.contains).map { c =>
      if (approx) approx_count_distinct(col(c)).as(c) else countDistinct(col(c)).as(c)
    } :+ count(lit(1)).as("_rows")
    df.agg(exprs.head, exprs.tail: _*)
  }

  /** Analyse FLIGHTS Bronze + export CSV */
  def analyzeFlights(df: DataFrame, outDir: String): Unit = {
    logger.info("=== ANALYSE FLIGHTS BRONZE ===")
    logger.info(s"Rows = ${df.count}")

    val nulls = nullsReport(df)
    val uniq = uniquesReport(
      df,
      Seq(
        "OP_CARRIER_AIRLINE_ID",
        "FL_NUM",
        "origin_airport_id",
        "dest_airport_id",
        "year",
        "month"
      )
    )

    nulls.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$outDir/flights_nulls")
    uniq
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outDir/flights_uniques")
  }

  /** Analyse WEATHER Bronze + export CSV (sur colonnes utiles) */
  def analyzeWeather(df: DataFrame, outDir: String): Unit = {
    logger.info("=== ANALYSE WEATHER BRONZE ===")
    logger.info(s"Rows = ${df.count}")

    val cols = Seq(
      "airport_id",
      "WBAN",
      "obs_utc",
      "SkyCondition",
      "WeatherType",
      "Visibility",
      "TempC",
      "DewPointC",
      "RelativeHumidity",
      "WindSpeedKt",
      "WindDirection",
      "Altimeter",
      "SeaLevelPressure",
      "StationPressure",
      "HourlyPrecip",
      "year",
      "month"
    ).filter(df.columns.contains)

    val wx = if (cols.nonEmpty) df.select(cols.map(col): _*) else df

    val nulls = nullsReport(wx)
    val uniq  = uniquesReport(wx, Seq("airport_id", "WBAN", "year", "month"))

    nulls.show(false); uniq.show(false)

    nulls.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$outDir/weather_nulls")
    uniq
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outDir/weather_uniques")
  }
}
