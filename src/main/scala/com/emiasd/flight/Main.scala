package com.emiasd.flight

// =======================
// Imports
// =======================
import com.emiasd.flight.analysis.{BronzeAnalysis, SilverAnalysis, TargetRatioAnalysis, TargetsInspection}
import com.emiasd.flight.bronze.{FlightsBronze, WeatherBronze}
import com.emiasd.flight.config.AppConfig
import com.emiasd.flight.io.{Readers, Writers}
import com.emiasd.flight.join.{BuildJT, FlightsEnriched}
import com.emiasd.flight.ml.FeatureBuilder.FeatureConfig
import com.emiasd.flight.ml.{ExperimentConfig, FeatureBuilder, ModelingPipeline}
import com.emiasd.flight.silver.{CleaningPlans, WeatherSlim}
import com.emiasd.flight.spark.{IOPaths, PathResolver, SparkBuilder}
import com.emiasd.flight.targets.TargetBatch
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

/**
 * Point d'entrée principal pour exécuter le pipeline ETL (Bronze → Silver →
 * Gold)
 */
object Main {

  // =======================
  // Logger
  // =======================
  val logger: Logger = Logger.getLogger(getClass.getName)

  // =======================
  // Étape 1 : BRONZE
  // =======================
  def runBronze(spark: SparkSession, paths: IOPaths): Unit = {
    logger.info("=== Étape BRONZE ===")

    // Lecture et enrichissement des vols
    logger.info("Lecture et enrichissement des vols (FlightsBronze)")
    val flightsBronze =
      FlightsBronze.readAndEnrich(spark, paths.flightsInputs, paths.mapping)

    // Vérification de l'unicité des colonnes
    logger.info("Vérification de l'unicité des colonnes")
    val dupCols = flightsBronze.columns.groupBy(_.toLowerCase).collect {
      case (n, arr) if arr.length > 1 => n
    }
    require(dupCols.isEmpty, s"Duplicate columns: ${dupCols.mkString(", ")}")

    // Lecture et enrichissement météo
    logger.info("Lecture et enrichissement météo (WeatherBronze)")
    val weatherBronze =
      WeatherBronze.readAndEnrich(spark, paths.weatherInputs)

    // Écriture en Delta Lake
    logger.info("Écriture des tables BRONZE en Delta Lake")
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

    // Analyses QA sur les jeux Bronze
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

    logger.info("Analyse qualité Bronze : vols et météo")
    BronzeAnalysis.analyzeFlights(flightsBronze, qaOutDir)
    BronzeAnalysis.analyzeWeather(weatherBronze, qaOutDir)

    logger.info("Étape Bronze terminée avec succès.")
  }

  // =======================
  // Étape 2 : SILVER
  // =======================
  def runSilver(spark: SparkSession, paths: IOPaths): Unit = {
    logger.info("=== Étape SILVER ===")

    // Vérification de la présence des tables Bronze
    val bronzeFlightsExists = Readers.exists(spark, paths.bronzeFlights)
    val bronzeWeatherExists = Readers.exists(spark, paths.bronzeWeather)

    if (!bronzeFlightsExists && !bronzeWeatherExists) {
      logger.warn(
        "Aucune table Bronze trouvée — lancement automatique de runBronze()"
      )
      runBronze(spark, paths)
    } else if (!bronzeFlightsExists) {
      logger.warn("Table Bronze Flights absente — régénération via runBronze()")
      runBronze(spark, paths)
    } else if (!bronzeWeatherExists) {
      logger.warn("Table Bronze Weather absente — régénération via runBronze()")
      runBronze(spark, paths)
    } else {
      logger.info(
        "Toutes les tables Bronze sont présentes — passage direct à l'étape Silver."
      )
    }

    // Lecture des tables Bronze
    logger.info("Lecture des tables BRONZE (flights & weather)")
    val flightsBronze = Readers.readDelta(spark, paths.bronzeFlights)
    val weatherBronze = Readers.readDelta(spark, paths.bronzeWeather)

    // Nettoyage et enrichissement des vols
    logger.info("Nettoyage et enrichissement des vols (CleaningPlans)")
    val flightsSilver = CleaningPlans.cleanFlights(flightsBronze)

    // Écriture de la table Silver Flights
    logger.info("Écriture des données Silver Flights")
    Writers.writeDelta(
      flightsSilver.coalesce(2),
      paths.silverFlights,
      Seq("year", "month"),
      overwriteSchema = true
    )

    // Analyse QA Silver
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

    logger.info("Analyse qualité Silver : vols nettoyés")
    val flightsSilverCheck = Readers.readDelta(spark, paths.silverFlights)
    SilverAnalysis.analyzeFlights(flightsSilverCheck, silverQaDir)

    // Enrichissement météo (UTC simplifié)
    logger.info("Enrichissement météo (WeatherSlim.enrichWithUTC)")
    val weatherSlim =
      WeatherSlim.enrichWithUTC(spark, weatherBronze, paths.mapping)

    // Écriture de la météo Silver
    logger.info("Écriture des données Silver Weather Filtered")
    Writers.writeDelta(
      weatherSlim.coalesce(2),
      paths.silverWeatherFiltered,
      Seq("year", "month"),
      overwriteSchema = true
    )

    logger.info("Étape Silver terminée avec succès.")
  }

  // =======================
  // Étape 3 : GOLD
  // =======================
  def runGold(spark: SparkSession, paths: IOPaths, cfg: AppConfig): Unit = {
    logger.info("=== Étape GOLD ===")

    // Vérification de la présence des tables Silver
    val silverFlightsExists = Readers.exists(spark, paths.silverFlights)
    val silverWeatherExists = Readers.exists(spark, paths.silverWeatherFiltered)

    if (!silverFlightsExists && !silverWeatherExists) {
      logger.warn(
        "Aucune table Silver trouvée — lancement automatique de runSilver()"
      )
      runSilver(spark, paths)
    } else if (!silverFlightsExists) {
      logger.warn("Table Silver Flights absente — régénération via runSilver()")
      runSilver(spark, paths)
    } else if (!silverWeatherExists) {
      logger.warn("Table Silver Weather absente — régénération via runSilver()")
      runSilver(spark, paths)
    } else {
      logger.info(
        "Toutes les tables Silver sont présentes — passage direct à l'étape Gold."
      )
    }

    // Lecture des tables Silver
    logger.info("Lecture des tables SILVER (flights & weather)")
    val flightsPrepared = Readers.readDelta(spark, paths.silverFlights)
    val weatherSlimDF   = Readers.readDelta(spark, paths.silverWeatherFiltered)

    // Enrichissement des vols
    logger.info("Enrichissement des vols (FlightsEnriched)")
    val flightsEnriched = FlightsEnriched.build(flightsPrepared)

    // Jointure météo-vols
    logger.info("Construction de la jointure spatio-temporelle (BuildJT)")
    val jtOut = BuildJT.buildJT(flightsEnriched, weatherSlimDF, cfg.thMinutes)

    // Écriture du résultat Gold
    logger.info("Écriture de la table GOLD (Joint Table)")
    Writers.writeDelta(
      jtOut,
      paths.goldJT,
      Seq("year", "month"),
      overwriteSchema = true
    )

    logger.info(s"Table GOLD écrite : ${paths.goldJT}")

    // Sanity checks de base
    import spark.implicits._
    val jtCheck = Readers.readDelta(spark, paths.goldJT)

    logger.info(s"JT rows = ${jtCheck.count}")
    logger.info(
      s"JT distinct flight_key = ${jtCheck.select($"F.flight_key").distinct.count}"
    )

    jtCheck
      .agg(
        sum(when(col("F.dep_ts_utc").isNull, 1).otherwise(0)).as("null_dep"),
        sum(when(col("F.arr_ts_utc").isNull, 1).otherwise(0)).as("null_arr"),
        count(lit(1)).as("total")
      )
      .show(false)

    // Part des vols avec observations météo Wo / Wd
    val withFlags = jtCheck
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

    // === Analyse τ pour D1 ===

    jtCheck
      .select(
        col("F.arr_delay_new"),
        col("F.weather_delay"),
        col("F.nas_delay"),
        col("F.nas_weather_delay")
      )
      .show(5, truncate = false)

    TargetRatioAnalysis.run(
      jt = jtCheck,
      outDir = "analysis/targets",
      tauGrid = Seq(0.80, 0.85, 0.90, 0.92, 0.95, 0.98, 1.00),
      eps = 1.0,
      tolTau1Strict = 1e-6,
      tolTau1Loose = 0.01
    )

    // === Génération D1..D4 x Th via batch unique ===

    val goldBase = paths.goldJT.substring(0, paths.goldJT.lastIndexOf('/'))
    val tau      = 0.95
    val ths      = Seq(15, 30, 45, 60, 90)

    // 1) Clés équilibrées pour tous les jeux (léger)
    val keysAll =
      TargetBatch.buildKeysForThresholds(jtCheck, ths, tau, sampleSeed = 1234L)

    // 2) Un seul join pour ré-attacher JT complet (Wo/Wd inclus)
    val fullAll =
      TargetBatch.materializeAll(jtCheck, keysAll, includeLightCols = true)

    // 2bis) Schéma explicite pour la table targets
    // (à ajuster si tu veux plus/moins de colonnes)
    val targetsDf = fullAll.select(
      col("F"),  // struct vol
      col("Wo"), // météo origine
      col("Wd"), // météo destination
      col("C"),  // label binaire
      col("flight_key"),
      col("year"),
      col("month"),
      col("ds"),    // dataset : D1..D4
      col("th"),    // seuil minutes
      col("is_pos") // label D* (balancé)
    )

    // 3) Écriture unique et partitionnée ds/th/year/month
    val outRoot = s"$goldBase/targets"

    val toWrite =
      if (
        targetsDf.columns
          .contains("year") && targetsDf.columns.contains("month")
      )
        targetsDf.repartition(col("ds"), col("th"), col("year"), col("month"))
      else
        targetsDf.repartition(col("ds"), col("th"))

    Writers.writeDelta(
      toWrite,
      outRoot,
      Seq("ds", "th", "year", "month"),
      overwriteSchema = true
    )

    logger.info("Étape Gold terminée avec succès.")

    val targetsPath = outRoot
    TargetsInspection.inspectSlice(
      spark,
      targetsPath,
      dsValue = "D2",
      thValue = 60,
      n = 20
    )
  }

  // =======================
  // Étape 4 : SPARK ML
  // =======================
  def runModeling(
    spark: SparkSession,
    paths: IOPaths,
    cfg: AppConfig
  ): Unit = {

    logger.info("=== Étape Spark ML ===")

    // Vérification de la présence de la table Gold
    val goldJTExists = Readers.exists(spark, paths.goldJT)

    if (!goldJTExists) {
      logger.warn("Aucune table Gold trouvée — lancement automatique de runGold()")
      runGold(spark, paths, cfg)
    } else {
      logger.info("La table Gold est présente — passage direct à l'étape de modélisation.")
    }

    // Reconstruction du chemin des targets comme dans runGold
    val goldBase    = paths.goldJT.substring(0, paths.goldJT.lastIndexOf('/'))
    val targetsPath = s"$goldBase/targets"

    // Configuration de base pour les features
    val baseCfg = FeatureConfig(
      labelCol = "is_pos",
      testFraction = 0.2,
      seed = 42L
    )

    // =======================
    // Liste des expériences
    // =======================
    val experiments: Seq[ExperimentConfig] = Seq(
      // === Baseline (aucune météo)
      ExperimentConfig("D2", cfg.thMinutes, 0, 0, "Baseline_D2_th60_noWeather"),

      // === Étude 1 : impact du nombre d'heures météo ===
      // Origine seule
      ExperimentConfig("D2", cfg.thMinutes, 1, 0, "S1_origin_1h_D2_th60"),
      ExperimentConfig("D2", cfg.thMinutes, 3, 0, "S1_origin_3h_D2_th60"),
      ExperimentConfig("D2", cfg.thMinutes, 5, 0, "S1_origin_5h_D2_th60"),
      ExperimentConfig("D2", cfg.thMinutes, 7, 0, "S1_origin_7h_D2_th60"),
      ExperimentConfig("D2", cfg.thMinutes, 9, 0, "S1_origin_9h_D2_th60"),
      ExperimentConfig("D2", cfg.thMinutes, 11, 0, "S1_origin_11h_D2_th60"),

      // Destination seule
      ExperimentConfig("D2", cfg.thMinutes, 0, 1, "S1_dest_1h_D2_th60"),
      ExperimentConfig("D2", cfg.thMinutes, 0, 3, "S1_dest_3h_D2_th60"),
      ExperimentConfig("D2", cfg.thMinutes, 0, 5, "S1_dest_5h_D2_th60"),
      ExperimentConfig("D2", cfg.thMinutes, 0, 7, "S1_dest_7h_D2_th60"),
      ExperimentConfig("D2", cfg.thMinutes, 0, 9, "S1_dest_9h_D2_th60"),
      ExperimentConfig("D2", cfg.thMinutes, 0, 11, "S1_dest_11h_D2_th60"),

      // Origine + destination
      ExperimentConfig("D2", cfg.thMinutes, 7, 7, "S1_origin7h_dest7h_D2_th60"),

      // === Étude 2 : variation du seuil th ===
      ExperimentConfig("D2", 15, 7, 7, "S2_D2_th15_origin7h_dest7h"),
      ExperimentConfig("D2", 30, 7, 7, "S2_D2_th30_origin7h_dest7h"),
      ExperimentConfig("D2", 45, 7, 7, "S2_D2_th45_origin7h_dest7h"),
      ExperimentConfig("D2", 60, 7, 7, "S2_D2_th60_origin7h_dest7h"),
      ExperimentConfig("D2", 90, 7, 7, "S2_D2_th90_origin7h_dest7h"),

      // === Étude 3 : variation du dataset ===
      ExperimentConfig("D1", cfg.thMinutes, 7, 7, "S3_D1_th60_origin7h_dest7h"),
      ExperimentConfig("D3", cfg.thMinutes, 7, 7, "S3_D3_th60_origin7h_dest7h"),
      ExperimentConfig("D4", cfg.thMinutes, 7, 7, "S3_D4_th60_origin7h_dest7h")
    )

    // =======================
    // Fonction utilitaire pour exécuter une expérience
    // =======================
    def runOneExperiment(e: ExperimentConfig): Unit = {
      logger.info(
        s"=== Expérience ${e.tag} : ds=${e.ds}, th=${e.th}, originHours=${e.originHours}, destHours=${e.destHours} ==="
      )

      val (trainDF, testDF, extraNumCols) =
        FeatureBuilder.prepareDataset(
          spark,
          targetsPath,
          e.ds,
          e.th,
          baseCfg,
          e.originHours,
          e.destHours
        )

      ModelingPipeline.trainAndEvaluate(
        spark,
        trainDF,
        testDF,
        e.ds,
        e.th,
        extraNumCols,
        e.tag
      )
    }

    // =======================
    // Exécution de toutes les expériences
    // =======================
    experiments.foreach(runOneExperiment)

    logger.info("=== Étape Spark ML terminée ===")
  }

  // =======================
  // MAIN
  // =======================
  def main(args: Array[String]): Unit =
    try {
      logger.info("Lancement de l'application...")

      // Chargement de la configuration et initialisation Spark
      val cfg   = AppConfig.load()
      val spark = SparkBuilder.build(cfg)
      val paths = PathResolver.resolve(cfg)

      logger.info(s"Configuration chargée : ${cfg}")
      logger.info(s"IO paths resolved: $paths")

      // Option : exécuter une seule étape si argument fourni
      val allowedStages = Set("bronze", "silver", "gold", "ml", "all")
      val stage         = args.headOption.getOrElse("all").toLowerCase
      if (!allowedStages.contains(stage)) {
        logger.error(
          s"Valeur d'étape non supportée: '$stage'. Valeurs autorisées: ${allowedStages.mkString(", ")}"
        )
        sys.exit(1)
      }
      stage match {
        case "bronze" =>
          runBronze(spark, paths)

        case "silver" =>
          runSilver(spark, paths)

        case "gold" =>
          runGold(spark, paths, cfg)

        case "ml" =>
          runModeling(spark, paths, cfg)

        case "all" =>
          runBronze(spark, paths)
          runSilver(spark, paths)
          runGold(spark, paths, cfg)
          runModeling(spark, paths, cfg)
      }

      logger.info("Application terminée avec succès.")
      spark.stop()
    } catch {
      case e: Exception =>
        logger.error("Application failed", e)
        throw e
    }
}
