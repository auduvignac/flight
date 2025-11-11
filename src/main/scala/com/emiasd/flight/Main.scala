package com.emiasd.flight

// =======================
// Imports
// =======================
import com.emiasd.flight.analysis.{BronzeAnalysis, SilverAnalysis}
import com.emiasd.flight.bronze.{FlightsBronze, WeatherBronze}
import com.emiasd.flight.config.AppConfig
import com.emiasd.flight.io.{Readers, Writers}
import com.emiasd.flight.join.{BuildJT, FlightsEnriched}
import com.emiasd.flight.silver.{CleaningPlans, WeatherSlim}
import com.emiasd.flight.spark.{PathResolver, SparkBuilder}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

/**
 * Point d'entrée principal pour exécuter l'ensemble du pipeline
 */
object Main {
  // =======================
  // Logger
  // =======================
  val logger: Logger = Logger.getLogger(getClass.getName)

  // =======================
  // Point d'entrée principal
  // =======================
  def main(args: Array[String]): Unit =
    try {
      val logger = Logger.getLogger(getClass.getName)
      logger.info("Lancement de l'application...")

      val cfg   = AppConfig.load()
      val spark = SparkBuilder.build(cfg)

      val paths = PathResolver.resolve(cfg)
      logger.info(s"IO paths resolved: $paths")

      // === BRONZE ===
      val flightsBronze =
        FlightsBronze.readAndEnrich(spark, paths.flightsInputs, paths.mapping)

      // Vérification de l'unicité des colonnes
      logger.info("Vérification de l'unicité des colonnes")
      val dupCols = flightsBronze.columns.groupBy(_.toLowerCase).collect {
        case (n, arr) if arr.length > 1 => n
      }
      require(dupCols.isEmpty, s"Duplicate columns: ${dupCols.mkString(", ")}")

      // Lecture et enrichissement
      logger.info("Lecture et enrichissement")
      val weatherBronze =
        WeatherBronze.readAndEnrich(spark, paths.weatherInputs)

      Writers.writeDelta(
        flightsBronze.coalesce(2),
        paths.bronzeFlights,
        Seq("year", "month"),
        overwriteSchema = true
      )
      Writers.writeDelta(
        weatherBronze.coalesce(2),
        paths.bronzeWeather,
        Seq("year", "month"),
        overwriteSchema = true
      )

      // === ANALYSE DES DONNÉES BRONZE → CSV ===
      // Dossier local "analysis" au chemin absolu, créé s'il n'existe pas.
      val qaOutDirFile = new java.io.File("analysis")
      val qaOutDir = Try {
        if (!qaOutDirFile.exists()) {
          if (qaOutDirFile.mkdirs())
            logger.info(s"Répertoire créé : ${qaOutDirFile.getAbsolutePath}")
          else
            logger.warn(
              s"Impossible de créer le répertoire : ${qaOutDirFile.getAbsolutePath}"
            )
        }
        qaOutDirFile.getAbsolutePath
      } match {
        case Success(path) => path
        case Failure(e) =>
          logger.error(
            s"Erreur lors de la création du répertoire ${qaOutDirFile.getAbsolutePath}",
            e
          )
          throw e
      }

      BronzeAnalysis.analyzeFlights(flightsBronze, qaOutDir)
      BronzeAnalysis.analyzeWeather(weatherBronze, qaOutDir)

      // === SILVER ===
      // /!\ flightsPlan n'est pas utilisé => utilisation ?
      // val flightsPlan   = CleaningPlans.deriveFlightsPlan(flightsBronze)
      val flightsSilver = CleaningPlans.cleanFlights(flightsBronze)
      Writers.writeDelta(
        flightsSilver.coalesce(2),
        paths.silverFlights,
        Seq("year", "month"),
        overwriteSchema = true
      )

      // === ANALYSE SILVER ===
      val silverQaDirFile = new java.io.File("analysis/silver")
      val silverQaDir = Try {
        if (!silverQaDirFile.exists()) {
          if (silverQaDirFile.mkdirs())
            logger.info(s"Répertoire créé : ${silverQaDirFile.getAbsolutePath}")
          else
            logger.warn(
              s"Impossible de créer le répertoire : ${silverQaDirFile.getAbsolutePath}"
            )
        }
        silverQaDirFile.getAbsolutePath
      } match {
        case Success(path) => path
        case Failure(e) =>
          logger.error(
            s"Erreur lors de la création du répertoire ${silverQaDirFile.getAbsolutePath}",
            e
          )
          throw e
      }

      // Recharge ou réutilise flightsCleaned
      val flightsSilverCheck = Readers.readDelta(spark, paths.silverFlights)
      SilverAnalysis.analyzeFlights(flightsSilverCheck, silverQaDir)

      // Météo → UTC par offset fixe d'heures (pas de DST)
      val weatherSlim =
        WeatherSlim.enrichWithUTC(spark, weatherBronze, paths.mapping)
      Writers.writeDelta(
        weatherSlim.coalesce(2),
        paths.silverWeatherFiltered,
        Seq("year", "month"),
        overwriteSchema = true
      )

      // === JOIN → JT ===
      val flightsPrepared = Readers.readDelta(spark, paths.silverFlights)
      val weatherSlimDF = Readers.readDelta(spark, paths.silverWeatherFiltered)
      val flightsEnriched = FlightsEnriched.build(flightsPrepared)
      val jtOut = BuildJT.buildJT(flightsEnriched, weatherSlimDF, cfg.thMinutes)

      Writers.writeDelta(
        jtOut,
        paths.goldJT,
        Seq("year", "month"),
        overwriteSchema = true
      )

      logger.info(s"JT écrit → ${paths.goldJT}")
      logger.info(
        "Lignes JT: " + Readers.readDelta(spark, paths.goldJT).count()
      )

      // === Sanity Checks ===

      import spark.implicits._

      val jtCheck = Readers.readDelta(spark, paths.goldJT)

      // Nombre de lignes
      logger.info("JT rows = " + jtCheck.count)

      // Unicité de la clef vol
      logger.info(
        "JT distinct flight_key = " + jtCheck
          .select($"F.flight_key")
          .distinct
          .count
      )

      // Présence des timestamps (alternative robuste sur champs imbriqués)
      jtCheck
        .agg(
          sum(when(col("F.dep_ts_utc").isNull, 1).otherwise(0)).as("null_dep"),
          sum(when(col("F.arr_ts_utc").isNull, 1).otherwise(0)).as("null_arr"),
          count(lit(1)).as("total")
        )
        .show(false)

      jtCheck.printSchema()

      // Part des vols avec observations météo Wo / Wd
      val withFlags =
        jtCheck
          .withColumn("hasWo", size($"Wo") > 0)
          .withColumn("hasWd", size($"Wd") > 0)
      withFlags
        .agg(
          avg(when($"hasWo", lit(1)).otherwise(lit(0))).as("pct_with_Wo"),
          avg(when($"hasWd", lit(1)).otherwise(lit(0))).as("pct_with_Wd")
        )
        .show(false)

      // Aperçu visuel (top 10)
      jtCheck
        .select(
          $"F.carrier",
          $"F.flnum",
          $"F.date",
          $"F.origin_airport_id",
          $"F.dest_airport_id",
          $"C",
          size($"Wo").as("nWo"),
          size($"Wd").as("nWd")
        )
        .orderBy(desc("nWo"))
        .show(10, false)

      logger.info("Application terminée avec succès.")
      spark.stop()
    } catch {
      case e: Exception =>
        logger.error("Application failed", e)
        throw e
    }
}
