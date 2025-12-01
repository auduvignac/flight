package com.emiasd.flight

// =======================
// Imports
// =======================
import com.emiasd.flight.analysis.{BronzeAnalysis, SilverAnalysis, TargetsInspection, WeatherFeatureDiagnostics}
import com.emiasd.flight.bronze.{FlightsBronze, WeatherBronze}
import com.emiasd.flight.config.{AppConfig, Environment}
import com.emiasd.flight.io.{Readers, Writers}
import com.emiasd.flight.join.{BuildJT, FlightsEnriched}
import com.emiasd.flight.ml.FeatureBuilder.FeatureConfig
import com.emiasd.flight.ml.{ExperimentConfig, FeatureBuilder, ModelingPipeline}
import com.emiasd.flight.silver.{CleaningPlans, WeatherSlim}
import com.emiasd.flight.spark.{IOPaths, PathResolver}
import com.emiasd.flight.targets.TargetBatch
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scopt.OParser

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
  def runBronze(spark: SparkSession, paths: IOPaths, cfg: AppConfig): Unit = {
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

    // On aligne le partitionnement sur (year, month)
    // => un seul gros shuffle ciblé, bon parallélisme local & cluster
    val flightsBronzePart = flightsBronze.repartition(col("year"), col("month"))
    val weatherBronzePart = weatherBronze.repartition(col("year"), col("month"))

    Writers.writeDelta(
      flightsBronzePart,
      paths.bronzeFlights,
      Seq("year", "month"),
      overwriteSchema = true
    )
    Writers.writeDelta(
      weatherBronzePart,
      paths.bronzeWeather,
      Seq("year", "month"),
      overwriteSchema = true
    )


    // Analyses QA sur les jeux Bronze
    if (cfg.env == Environment.Local) {
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
    } else {
      logger.info("Env = Hadoop : on saute les CSV d’analyse Bronze pour éviter les problèmes de droits HDFS/local.")
    }

    logger.info("Étape Bronze terminée avec succès.")
  }


  // =======================
  // Étape 2 : SILVER
  // =======================
  def runSilver(spark: SparkSession, paths: IOPaths, cfg: AppConfig): Unit = {
    logger.info("=== Étape SILVER ===")

    // Vérification de la présence des tables Bronze
    val bronzeFlightsExists = Readers.exists(spark, paths.bronzeFlights)
    val bronzeWeatherExists = Readers.exists(spark, paths.bronzeWeather)

    if (!bronzeFlightsExists && !bronzeWeatherExists) {
      logger.warn(
        "Aucune table Bronze trouvée — lancement automatique de runBronze()"
      )
      runBronze(spark, paths, cfg)
    } else if (!bronzeFlightsExists) {
      logger.warn("Table Bronze Flights absente — régénération via runBronze()")
      runBronze(spark, paths, cfg)
    } else if (!bronzeWeatherExists) {
      logger.warn("Table Bronze Weather absente — régénération via runBronze()")
      runBronze(spark, paths, cfg)
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

    val flightsSilverPart =
      flightsSilver.repartition(col("year"), col("month"))

    Writers.writeDelta(
      flightsSilverPart,
      paths.silverFlights,
      Seq("year", "month"),
      overwriteSchema = true
    )

    // Analyse QA Silver
    // Analyse QA Silver (uniquement en local)
    if (cfg.env == Environment.Local) {
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
    }


    // Enrichissement météo (UTC simplifié)
    logger.info("Enrichissement météo (WeatherSlim.enrichWithUTC)")
    val weatherSlim =
      WeatherSlim.enrichWithUTC(spark, weatherBronze, paths.mapping)

    // Écriture de la météo Silver
    logger.info("Écriture des données Silver Weather Filtered")

    val weatherSlimPart =
      weatherSlim.repartition(col("year"), col("month"))

    Writers.writeDelta(
      weatherSlimPart,
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
    import spark.implicits._
    logger.info("=== Étape GOLD ===")

    // --- 1) Vérif Silver (inchangé) ---
    val silverFlightsExists = Readers.exists(spark, paths.silverFlights)
    val silverWeatherExists = Readers.exists(spark, paths.silverWeatherFiltered)

    if (!silverFlightsExists && !silverWeatherExists) {
      logger.warn("Aucune table Silver trouvée — lancement automatique de runSilver()")
      runSilver(spark, paths, cfg)
    } else if (!silverFlightsExists) {
      logger.warn("Table Silver Flights absente — régénération via runSilver()")
      runSilver(spark, paths, cfg)
    } else if (!silverWeatherExists) {
      logger.warn("Table Silver Weather absente — régénération via runSilver()")
      runSilver(spark, paths, cfg)
    } else {
      logger.info("Toutes les tables Silver sont présentes — passage direct à l'étape Gold.")
    }

    // --- 2) Lecture Silver & BuildJT ---
    logger.info("Lecture des tables SILVER (flights & weather)")
    val flightsPrepared = Readers.readDelta(spark, paths.silverFlights)
    val weatherSlimDF   = Readers.readDelta(spark, paths.silverWeatherFiltered)

    logger.info("Enrichissement des vols (FlightsEnriched)")
    val flightsEnriched = FlightsEnriched.build(flightsPrepared)

    logger.info("Construction de la jointure spatio-temporelle (BuildJT)")
    val jtOut = BuildJT.buildJT(flightsEnriched, weatherSlimDF, cfg.thMinutes)

    // --- 3) SLIMMER : ne garder que ce qui est utilisé plus loin ---
    // NOTE : on garde flight_key explicitement, même si présent dans F, pour simplifier
    val jtSlim = jtOut.select(
      col("F"),
      col("Wo"),
      col("Wd"),
      col("C"),
      col("F.flight_key").alias("flight_key"),
      col("year"),
      col("month")
    )

    logger.info("JT (slim) schema = " + jtSlim.schema.treeString)

    // --- 4) Répartition contrôlée & write Delta sur JT ---
    // On adapte le nombre de partitions au nombre de mois pour limiter la taille mémoire de chaque tâche.
    val monthsCount      = cfg.monthsF.distinct.size
    val numPartsForJT    = math.max(64 * monthsCount, 128)  // ex : 8 mois -> 512 partitions
    logger.info(s"JT (slim) repartition before write : months = $monthsCount, partitions = $numPartsForJT")

    val jtPartitionedSlim =
      jtSlim.repartition(numPartsForJT, col("year"), col("month"))

    logger.info("Écriture de la table GOLD (Joint Table)")
    Writers.writeDelta(
      jtPartitionedSlim,
      paths.goldJT,
      Seq("year", "month"),
      overwriteSchema = true
    )
    logger.info(s"Table GOLD écrite : ${paths.goldJT}")

    // --- 5) Choix de la source pour la suite ---
    // En Local : on relit pour vérifier le write
    // En Hadoop : on évite une relecture, on réutilise jtPartitionedSlim
    val jtForTargets =
      if (cfg.env == Environment.Local) {
        logger.info("Env = Local : relecture de JT depuis Delta pour vérification")
        Readers.readDelta(spark, paths.goldJT)
      } else {
        logger.info("Env = Hadoop : réutilisation de JT en mémoire pour les targets (pas de relecture)")
        jtPartitionedSlim
      }

    // --- 6) Sanity checks : uniquement en Local ---
    if (cfg.env == Environment.Local) {
      val jtCheck = jtForTargets

      logger.info(s"JT rows = ${jtCheck.count}")
      logger.info(
        s"JT distinct flight_key = ${jtCheck.select($"flight_key").distinct.count}"
      )

      jtCheck
        .agg(
          sum(when(col("F.dep_ts_utc").isNull, 1).otherwise(0)).as("null_dep"),
          sum(when(col("F.arr_ts_utc").isNull, 1).otherwise(0)).as("null_arr"),
          count(lit(1)).as("total")
        )
        .show(false)

      val withFlags = jtCheck
        .withColumn("hasWo", size($"Wo") > 0)
        .withColumn("hasWd", size($"Wd") > 0)

      withFlags
        .agg(
          avg(when($"hasWo", lit(1)).otherwise(lit(0))).as("pct_with_Wo"),
          avg(when($"hasWd", lit(1)).otherwise(lit(0))).as("pct_with_Wd")
        )
        .show(false)

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

      jtCheck
        .select(
          col("F.arr_delay_new"),
          col("F.weather_delay"),
          col("F.nas_delay"),
          col("F.nas_weather_delay")
        )
        .show(5, truncate = false)
    }

    // --- 7) Génération des targets (D1..D4 x Th) ---
    val goldBase = paths.goldJT.substring(0, paths.goldJT.lastIndexOf('/'))
    val tau      = 0.95
    val ths      = Seq(15, 30, 45, 60, 90)

    logger.info(s"TargetBatch.buildKeysForThresholds sur ths=$ths, tau=$tau")
    val keysAll =
      TargetBatch.buildKeysForThresholds(jtForTargets, ths, tau, sampleSeed = 1234L)

    logger.info("TargetBatch.materializeAll (ré-attache Wo/Wd/F) en cours...")
    val fullAll =
      TargetBatch.materializeAll(jtForTargets, keysAll, includeLightCols = true)

    // Sélection des colonnes pour la table finale des targets
    val targetsDfBase = fullAll.select(
      col("F"),
      col("Wo"),
      col("Wd"),
      col("C"),
      col("flight_key"),
      col("year"),
      col("month"),
      col("ds"),
      col("th"),
      col("is_pos")
    )

    // Pour l'écriture des targets, on repartitionne aussi pour éviter des partitions monstrueuses
    val numPartsTargets = math.max(32 * monthsCount, 128)  // un peu moins agressif que JT
    logger.info(s"Targets repartition before write : months = $monthsCount, partitions = $numPartsTargets")

    val targetsPartitioned =
      targetsDfBase.repartition(numPartsTargets, col("ds"), col("th"), col("year"), col("month"))

    val outRoot = s"$goldBase/targets"

    Writers.writeDelta(
      targetsPartitioned,
      outRoot,
      Seq("ds", "th", "year", "month"),
      overwriteSchema = true
    )

    logger.info("Étape Gold terminée avec succès.")

    // --- 8) Inspection détaillée uniquement en Local ---
    if (cfg.env == Environment.Local) {
      val targetsPath = outRoot
      TargetsInspection.inspectSlice(
        spark,
        targetsPath,
        dsValue = "D2",
        thValue = 60,
        n = 20
      )
    }
  }





  // =======================
  // Étape 4 : DIAGNOSTICS
  // =======================
  def runDiagnostics(
    spark: SparkSession,
    paths: IOPaths,
    cfg: AppConfig
  ): Unit = {
    logger.info("=== Étape DIAGNOSTICS ===")

    // Vérification de la présence de la table Gold
    val goldJTExists = Readers.exists(spark, paths.goldJT)

    if (!goldJTExists) {
      logger.warn(
        "Aucune table Gold trouvée — lancement automatique de runGold()"
      )
      runGold(spark, paths, cfg)
    } else {
      logger.info(
        "La table Gold est présente — passage direct aux diagnostics."
      )
    }

    // Extraction des paramètres pour les diagnostics
    val ds          = cfg.ds.getOrElse("D2")
    val th          = cfg.thMinutes
    val originHours = cfg.originHours.getOrElse(7)
    val destHours   = cfg.destHours.getOrElse(7)

    logger.info(
      s"Lancement des diagnostics météo : ds=$ds, th=$th, originHours=$originHours, destHours=$destHours"
    )

    WeatherFeatureDiagnostics.runDiagnostics(
      spark,
      paths.goldJT,
      paths.silverWeatherFiltered,
      paths.silverFlights,
      paths.bronzeWeather,
      ds,
      th,
      originHours,
      destHours
    )

    logger.info("=== Étape DIAGNOSTICS terminée ===")
  }

  // =======================
  // Étape 5 : SPARK ML
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
      logger.warn(
        "Aucune table Gold trouvée — lancement automatique de runGold()"
      )
      runGold(spark, paths, cfg)
    } else {
      logger.info(
        "La table Gold est présente — passage direct à l'étape de modélisation."
      )
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
    // Exécution conditionnelle des expériences
    // =======================
    (cfg.ds, cfg.originHours, cfg.destHours, cfg.tag) match {
      // Cas 1 : un scénario spécifique a été passé en argument CLI
      case (Some(ds), Some(origin), Some(dest), Some(tag)) =>
        val th        = cfg.thMinutes
        val singleExp = ExperimentConfig(ds, th, origin, dest, tag)
        logger.info(s"Exécution ciblée d'une seule expérience : $singleExp")
        runOneExperiment(singleExp)

      // Cas 2 : aucun paramètre CLI => exécution de tous les scénarios
      case _ =>
        logger.info(
          "Aucun scénario spécifique fourni — exécution de toutes les expériences définies."
        )
        experiments.foreach(runOneExperiment)
    }

    logger.info("=== Étape Spark ML terminée ===")
  }

  // =======================
  // MAIN
  // =======================
  def main(args: Array[String]): Unit = {
    try {
      logger.info("Lancement de l'application Flight Delay Prediction...")

      // Chargement de la configuration de base (.conf)
      val baseCfg = AppConfig.load()

      // Parsing scopt complet
      val builder = OParser.builder[AppConfig]
      val parser = {
        import builder._
        OParser.sequence(
          programName("flight-delay-pipeline"),
          head("Flight Delay Prediction", "v1.0"),

          // ENVIRONNEMENT
          opt[String]("env")
            .action((x, c) => c.copy(env = Environment.fromString(x)))
            .text("Environnement d'exécution (Local, Hadoop, CI, etc.)"),

          // INPUTS LOCAL
          opt[String]("deltaBase")
            .action((x, c) => c.copy(deltaBase = Some(x)))
            .text(
              "Base path des tables Delta (ex: /app/delta ou /app/delta-Exp)"
            ),
          opt[String]("inFlightsDir")
            .action((x, c) => c.copy(inFlightsDir = x))
            .text("Répertoire local des vols"),
          opt[String]("inWeatherDir")
            .action((x, c) => c.copy(inWeatherDir = x))
            .text("Répertoire local météo"),
          opt[String]("inMapping")
            .action((x, c) => c.copy(inMapping = x))
            .text("Fichier de mapping local"),

          // INPUTS HDFS
          opt[String]("hInFlightsDir")
            .action((x, c) => c.copy(hInFlightsDir = x))
            .text("Répertoire HDFS des vols"),
          opt[String]("hInWeatherDir")
            .action((x, c) => c.copy(hInWeatherDir = x))
            .text("Répertoire HDFS météo"),
          opt[String]("hInMapping")
            .action((x, c) => c.copy(hInMapping = x))
            .text("Mapping HDFS"),

          // OUTPUTS LOCAL
          opt[String]("deltaBronzeBase")
            .action((x, c) => c.copy(deltaBronzeBase = x))
            .text("Répertoire local Delta Bronze"),
          opt[String]("deltaSilverBase")
            .action((x, c) => c.copy(deltaSilverBase = x))
            .text("Répertoire local Delta Silver"),
          opt[String]("deltaGoldBase")
            .action((x, c) => c.copy(deltaGoldBase = x))
            .text("Répertoire local Delta Gold"),

          // OUTPUTS HDFS
          opt[String]("hDeltaBronzeBase")
            .action((x, c) => c.copy(hDeltaBronzeBase = x))
            .text("Répertoire HDFS Delta Bronze"),
          opt[String]("hDeltaSilverBase")
            .action((x, c) => c.copy(hDeltaSilverBase = x))
            .text("Répertoire HDFS Delta Silver"),
          opt[String]("hDeltaGoldBase")
            .action((x, c) => c.copy(hDeltaGoldBase = x))
            .text("Répertoire HDFS Delta Gold"),

          // PARAMÈTRES
          opt[Seq[String]]("monthsF")
            .valueName("m1,m2,...")
            .action((x, c) => c.copy(monthsF = x))
            .text("Liste des mois vols (ex: 01,02,03)"),
          opt[Seq[String]]("monthsW")
            .valueName("m1,m2,...")
            .action((x, c) => c.copy(monthsW = x))
            .text("Liste des mois météo (ex: 01,02,03)"),
          opt[Int]("th")
            .action((x, c) => c.copy(thMinutes = x))
            .text("Seuil de retard en minutes"),
          opt[Double]("missingnessThreshold")
            .action((x, c) => c.copy(missingnessThreshold = x))
            .text("Seuil de valeurs manquantes (0–1)"),

          // SPARK
          opt[String]("sparkMaster")
            .action((x, c) => c.copy(sparkMaster = x))
            .text("Master Spark (local[*], yarn, k8s, etc.)"),
          opt[String]("sparkAppName")
            .action((x, c) => c.copy(sparkAppName = x))
            .text("Nom de l'application Spark"),
          opt[String]("sparkSqlExtensions")
            .action((x, c) => c.copy(sparkSqlExtensions = x))
            .text("Extensions Spark SQL"),
          opt[String]("sparkSqlCatalog")
            .action((x, c) => c.copy(sparkSqlCatalog = x))
            .text("Nom du catalogue Spark"),
          opt[Map[String, String]]("sparkConfs")
            .valueName("k1=v1,k2=v2,...")
            .action((x, c) => c.copy(sparkConfs = x))
            .text("Configurations Spark supplémentaires"),

          // MODÉLISATION
          opt[String]("stage")
            .action((x, c) => c.copy(stage = x.toLowerCase))
            .text(
              "Étape à exécuter : bronze, silver, gold, diagnostics, ml, all"
            ),
          opt[String]("ds")
            .action((x, c) => c.copy(ds = Some(x)))
            .text("Dataset (D1, D2, D3, D4)"),
          opt[Int]("originHours")
            .action((x, c) => c.copy(originHours = Some(x)))
            .text("Heures météo origine"),
          opt[Int]("destHours")
            .action((x, c) => c.copy(destHours = Some(x)))
            .text("Heures météo destination"),
          opt[String]("tag")
            .action((x, c) => c.copy(tag = Some(x)))
            .text("Tag unique de l'expérience"),
          help("help").text("Affiche cette aide et quitte")
        )
      }

      // Fusion de la config + arguments CLI
      OParser.parse(parser, args, baseCfg) match {
        case Some(cfg) =>
          AppConfig.logConfig(cfg)
          runPipeline(cfg)

        case None =>
          sys.exit(1)
      }

    } catch {
      case e: Exception =>
        logger.error("Échec de l'application", e)
        throw e
    }
  }

  // ============================================================
  // PIPELINE PRINCIPAL
  // ============================================================
  def runPipeline(cfg: AppConfig): Unit = {
    logger.info(s"Étape demandée : ${cfg.stage}")

    val spark = {
      val b = SparkSession
        .builder()
        .appName(cfg.sparkAppName)
        .master(cfg.sparkMaster)
        .config("spark.sql.extensions", cfg.sparkSqlExtensions)
        .config("spark.sql.catalog.spark_catalog", cfg.sparkSqlCatalog)

      // Log + application des configs utilisateur
      cfg.sparkConfs.foreach { case (k, v) =>
        logger.info(s"[SparkConfig] Applying $k = $v")
        b.config(k, v)
      }

      b.getOrCreate()
    }

    // Parallélisme par défaut si non défini dans sparkConfs / application.conf
    if (!cfg.sparkConfs.contains("spark.sql.shuffle.partitions")) {
      val defaultShuffle =
        cfg.env match {
          case Environment.Hadoop => "192" // cluster : plus de tâches
          case _                  => "64"  // local/Docker : un peu plus modéré
        }
      logger.info(
        s"[SparkConfig] spark.sql.shuffle.partitions non défini — fallback à $defaultShuffle pour ${cfg.env}"
      )
      spark.conf.set("spark.sql.shuffle.partitions", defaultShuffle)
    }


    val paths = PathResolver.resolve(cfg)

    logger.info(s"[Paths] bronzeFlights=${paths.bronzeFlights}")
    logger.info(s"[Paths] silverFlights=${paths.silverFlights}")
    logger.info(s"[Paths] goldJT=${paths.goldJT}")

    cfg.stage.toLowerCase match {
      case "bronze"      => runBronze(spark, paths, cfg)
      case "silver"      => runSilver(spark, paths, cfg)
      case "gold"        => runGold(spark, paths, cfg)
      case "diagnostics" => runDiagnostics(spark, paths, cfg)
      case "ml"          => runModeling(spark, paths, cfg)
      case "all" =>
        runBronze(spark, paths, cfg)
        runSilver(spark, paths, cfg)
        runGold(spark, paths, cfg)
        runModeling(spark, paths, cfg)
      case other =>
        logger.error(
          s"Étape inconnue: $other (bronze, silver, gold, diagnostics, ml, all)"
        )
        sys.exit(1)
    }

    spark.stop()
    logger.info("Application terminée avec succès.")
  }
}
