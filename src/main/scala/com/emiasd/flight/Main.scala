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
import com.emiasd.flight.spark.{IOPaths, PathResolver, SparkBuilder}
import com.emiasd.flight.ml.{FeatureBuilder, ModelingPipeline}
import com.emiasd.flight.ml.FeatureBuilder.FeatureConfig
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
    val bronzeFlightsExists = Readers.exists(paths.bronzeFlights)
    val bronzeWeatherExists = Readers.exists(paths.bronzeWeather)

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
    val silverFlightsExists = Readers.exists(paths.silverFlights)
    val silverWeatherExists = Readers.exists(paths.silverWeatherFiltered)

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
    import com.emiasd.flight.analysis.TargetRatioAnalysis
    // /!\ ss jamais utilisé => à supprimer ?
    // implicit val ss: SparkSession = spark  // implicite dispo pour TargetRatioAnalysis / TargetBatch

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
    import com.emiasd.flight.targets.TargetBatch

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
      col("F"),            // struct vol
      col("Wo"),           // météo origine
      col("Wd"),           // météo destination
      col("C"),            // label binaire
      col("flight_key"),
      col("year"),
      col("month"),
      col("ds"),           // dataset : D1..D4
      col("th"),           // seuil minutes
      col("is_pos")        // label D* (balancé)
    )

    // 3) Écriture unique et partitionnée ds/th/year/month
    val outRoot = s"$goldBase/targets"

    val toWrite =
      if (targetsDf.columns.contains("year") && targetsDf.columns.contains("month"))
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

    // ...
    import com.emiasd.flight.analysis.TargetsInspection
    val targetsPath = outRoot   // déjà correctement défini
    TargetsInspection.inspectSlice(
      spark,
      targetsPath,
      dsValue = "D2",
      thValue = 60,
      n = 20
    )

    // Inspection uniquement en mode Local, pour éviter le bruit en prod
    if (cfg.env == "Local") {
      TargetsInspection.inspectSlice(spark, targetsPath, dsValue = "D2", thValue = 60, n = 20)
    }
  }

  // =======================
  // Étape 4 : SPARK ML
  // =======================
  /**
   * Étape ML : préparation du dataset (D2, th=cfg.thMinutes) + entraînement RandomForest
   */
  def runModeling(
                   spark: SparkSession,
                   paths: IOPaths,
                   cfg: AppConfig
                 ): Unit = {
    logger.info("=== Étape Spark ML ===")

    // On retrouve la base gold comme dans runGold
    // ex : paths.goldJT = "/app/delta/gold/JT_th60"
    val goldBase    = paths.goldJT.substring(0, paths.goldJT.lastIndexOf('/'))
    val targetsPath = s"$goldBase/targets"

    val ds = "D2"          // dataset cible principal (comme dans TIST)
    val th = cfg.thMinutes // typiquement 60

    logger.info(s"=== Étape ML : préparation du dataset ds=$ds, th=$th ===")

    val (trainDF, testDF) =
      FeatureBuilder.prepareDataset(
        spark,
        targetsPath,
        ds,
        th,
        FeatureConfig(
          labelCol     = "is_pos", // label équilibré (TIST-like)
          testFraction = 0.2,
          seed         = 42L
        )
      )

    logger.info(s"=== Étape ML : entraînement RandomForest ds=$ds, th=$th ===")
    ModelingPipeline.trainAndEvaluate(spark, trainDF, testDF, ds, th)
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
      logger.info(s"Args = ${args.toList}")

      // val stage = args.headOption.getOrElse("all").toLowerCase
      val stage = args.headOption.getOrElse {
        logger.error(
          "Aucun stage fourni (attendu: bronze | silver | gold | all)"
        )
        sys.exit(2)
      }.replaceFirst("^--stage=", "").toLowerCase

      logger.info(s"Stage demandé = $stage")

      stage match {
        case "bronze" =>
          runBronze(spark, paths)

        case "silver" =>
          runSilver(spark, paths)

        case "gold" =>
          runGold(spark, paths, cfg)

        case "ml" =>
          // si tu veux être sûr que targets existe, tu peux faire :
          runGold(spark, paths, cfg)
          runModeling(spark, paths, cfg)

        case "all" =>
          runBronze(spark, paths)
          runSilver(spark, paths)
          runGold(spark, paths, cfg)
          runModeling(spark, paths, cfg)

        case other =>
          logger.warn(
            s"Étape inconnue '$other' — valeurs possibles: bronze, silver, gold, all"
          )
      }

      logger.info("Application terminée avec succès.")
      spark.stop()
    } catch {
      case e: Exception =>
        logger.error("Application failed", e)
        throw e
    }
}
