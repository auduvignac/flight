package com.emiasd.flight.spark

import com.emiasd.flight.config.{AppConfig, Environment}

/** Chemins résolus dynamiquement selon l'environnement et la base Delta. */
final case class IOPaths(
  flightsInputs: Seq[String],
  weatherInputs: Seq[String],
  mapping: String,
  analysisDir: String,
  bronzeFlights: String,
  bronzeWeather: String,
  silverFlights: String,
  silverWeatherFiltered: String,
  goldJT: String
)

object PathResolver {

  // --- Utils --------------------------------------------------------------

  /**
   * Supprime proprement un suffixe dans un chemin, avec ou sans slash final.
   */
  private def stripSuffix(path: String, suffix: String): String =
    path.stripSuffix(suffix).stripSuffix(suffix + "/")

  /**
   * Retourne la base Delta "effective" : - si deltaBase est définie →
   * utilisable directement - sinon → dérivée de la base gold (env local ou
   * Hadoop)
   */
  private def computeEffectiveDeltaBase(cfg: AppConfig): String =
    cfg.deltaBase.getOrElse {
      val rawBase =
        cfg.env match {
          case Environment.Hadoop => cfg.hDeltaGoldBase
          case _                  => cfg.deltaGoldBase
        }

      // supprime uniquement le suffixe /gold
      stripSuffix(rawBase, "/gold")
    }

  /** Retourne les chemins d'entrée selon l'environnement. */
  private def inputPaths(cfg: AppConfig): (String, String, String) =
    cfg.env match {
      case Environment.Local =>
        (cfg.inFlightsDir, cfg.inWeatherDir, cfg.inMapping)
      case Environment.Hadoop =>
        (cfg.hInFlightsDir, cfg.hInWeatherDir, cfg.hInMapping)
    }

  /** Retourne le chemin d'analyse QA selon l'environnement. */
  private def analysisPath(cfg: AppConfig): String =
    cfg.env match {
      case Environment.Hadoop => cfg.hanalysisDir // Chemin HDFS/Cluster
      case _                  => cfg.analysisDir  // Chemin local/Docker
    }

  // --- Main resolve -------------------------------------------------------

  def resolve(cfg: AppConfig): IOPaths = {

    // 1) Base Delta effective (robuste)
    val effectiveDeltaBase = computeEffectiveDeltaBase(cfg)

    // 2) Entrées
    val (flightsDir, weatherDir, mapping) = inputPaths(cfg)

    val flightsInputs =
      cfg.monthsF.map(m => s"$flightsDir/$m.csv")

    val weatherInputs =
      cfg.monthsW.map(m => s"$weatherDir/${m}hourly.txt")

    // 3) Arborescences Delta
    val bronzeBase = s"$effectiveDeltaBase/bronze"
    val silverBase = s"$effectiveDeltaBase/silver"
    val goldBase   = s"$effectiveDeltaBase/gold"

    IOPaths(
      flightsInputs = flightsInputs,
      weatherInputs = weatherInputs,
      mapping = mapping,
      analysisDir = analysisPath(cfg),
      bronzeFlights = s"$bronzeBase/flights",
      bronzeWeather = s"$bronzeBase/weather",
      silverFlights = s"$silverBase/flights",
      silverWeatherFiltered = s"$silverBase/weather_filtered",
      goldJT = s"$goldBase/JT_th${cfg.thMinutes}"
    )
  }
}
