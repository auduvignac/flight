package com.emiasd.flight

// =======================
// Imports
// =======================
import com.emiasd.flight.analysis.BronzeAnalysis
import com.emiasd.flight.bronze.{FlightsBronze, WeatherBronze}
import com.emiasd.flight.config.AppConfig
import com.emiasd.flight.io.Writers
import com.emiasd.flight.silver.CleaningPlans
import com.emiasd.flight.spark.{PathResolver, SparkBuilder}
import org.apache.log4j.Logger

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
      val flightsBronze = FlightsBronze.readAndEnrich(spark, paths.flightsInputs, paths.mapping)

      // Vérification de l'unicité des colonnes
      logger.info("Vérification de l'unicité des colonnes")
      val dupCols = flightsBronze.columns.groupBy(_.toLowerCase).collect {
        case (n, arr) if arr.length > 1 => n
      }
      require(dupCols.isEmpty, s"Duplicate columns: ${dupCols.mkString(", ")}")

      // Lecture et enrichissement
      logger.info("Lecture et enrichissement")
      val weatherBronze = WeatherBronze.readAndEnrich(spark, paths.weatherInputs)

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
            logger.warn(s"Impossible de créer le répertoire : ${qaOutDirFile.getAbsolutePath}")
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
      val flightsPlan   = CleaningPlans.deriveFlightsPlan(flightsBronze)
      val flightsSilver = CleaningPlans.cleanFlights(flightsBronze, flightsPlan)
      Writers.writeDelta(
        flightsSilver.coalesce(2),
        paths.silverFlights,
        Seq("year", "month"),
        overwriteSchema = true
      )

      logger.info("Application terminée avec succès.")
      spark.stop()
    } catch {
      case e: Exception =>
        logger.error("Application failed", e)
        throw e
    }
}
