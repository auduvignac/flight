// com/emiasd/flight/ml/FeatureBuilder.scala
package com.emiasd.flight.ml

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FeatureBuilder {

  val logger = Logger.getLogger(getClass.getName)
  /**
   * Configuration pour la préparation dataset
   */
  case class FeatureConfig(
                            labelCol: String   = "is_pos", // tu peux mettre "C" si tu veux le label brut
                            testFraction: Double = 0.2,
                            seed: Long          = 42L
                          )

  /**
   * Flatten de la table targets (gold/targets) vers une table de features "flat".
   * Pour l'instant on ne prend que des features vols (sans météo).
   */
  def buildFlatFeatures(df: DataFrame, cfg: FeatureConfig)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // 1) Sélection et flatten du struct F
    val base = df.select(
      col("flight_key"),
      col("ds"),
      col("th"),
      col(cfg.labelCol).cast("double").as("label"),

      // >>> champs présents dans F selon ton message d'erreur
      col("F.carrier").as("carrier_id"),
      col("F.origin_airport_id").as("origin_id"),
      col("F.dest_airport_id").as("dest_id"),
      col("F.flnum").as("flnum"),
      col("F.crs_dep_scheduled_hhmm").as("crs_dep_time"),

      // >>> year / month sont au niveau racine (hors struct F)
      col("year"),
      col("month").cast("int").as("month")
    )

    // 2) time-feature : crs_dep_time (HHMM) -> minutes depuis minuit
    val withDepMinutes = base.withColumn(
      "dep_minutes",
      (col("crs_dep_time") / 100).cast("int") * 60 + (col("crs_dep_time") % 100)
    )

    // 3) Nettoyage de base : on évite les nulls critiques
    val cleaned = withDepMinutes
      .na.drop("any", Seq(
        "label",
        "carrier_id",
        "origin_id",
        "dest_id",
        "flnum",
        "year",
        "month",
        "dep_minutes"
      ))

    cleaned
  }


  /**
   * Fonction demandée :
   *
   * Lit gold/targets, filtre sur ds/th, construit les features,
   * puis renvoie (trainDF, testDF) prêts pour Spark ML.
   */
  def prepareDataset(
                      spark: SparkSession,
                      targetsPath: String,
                      ds: String,
                      th: Int,
                      cfg: FeatureConfig = FeatureConfig()
                    ): (DataFrame, DataFrame) = {

    implicit val s: SparkSession = spark

    // 0) Lecture de la table targets
    val raw = spark.read.format("delta").load(targetsPath)

    // 1) Filtre sur le dataset et le seuil
    val slice = raw
      .filter(col("ds") === lit(ds) && col("th") === lit(th))

    // (optionnel) petit log
    val n = slice.count()
    logger.info(s"[FeatureBuilder] Slice ds=$ds, th=$th -> $n lignes avant features")

    // 2) Construction des features "flat"
    val features = buildFlatFeatures(slice, cfg)

    val n2 = features.count()
    logger.info(s"[FeatureBuilder] Slice ds=$ds, th=$th -> $n2 lignes après nettoyage features")

    // 3) Split train / test
    val Array(train, test) = features.randomSplit(
      Array(1.0 - cfg.testFraction, cfg.testFraction),
      seed = cfg.seed
    )

    logger.info(
      s"[FeatureBuilder] Split train/test (ds=$ds, th=$th): " +
        s"train=${train.count()}, test=${test.count()}"
    )

    (train, test)
  }
}
