// com/emiasd/flight/ml/FeatureBuilder.scala
package com.emiasd.flight.ml

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object FeatureBuilder {
  val logger = Logger.getLogger(getClass.getName)

  /**
   * Configuration pour la préparation dataset
   */
  case class FeatureConfig(
    labelCol: String = "is_pos", // ou "C" si tu veux le label brut
    testFraction: Double = 0.2,
    seed: Long = 42L
  )

  // Variables météo numériques qu'on va exploiter
  private val wxNumericVars: Seq[String] = Seq(
    "vis",
    "tempC",
    "dewC",
    "rh",
    "windKt",
    "windDir",
    "altim",
    "slp",
    "stnp",
    "precip"
  )

  /**
   * Noms des colonnes de features météo en fonction du nombre d'heures
   */
  def weatherFeatureNames(originHours: Int, destHours: Int): Array[String] = {
    val originCols =
      for {
        h <- 0 until originHours
        v <- wxNumericVars
      } yield s"o_h${h}_${v}"

    val destCols =
      for {
        h <- 0 until destHours
        v <- wxNumericVars
      } yield s"d_h${h}_${v}"

    (originCols ++ destCols).toArray
  }

  /**
   * Flatten de la table targets vers une table de features "vol" sans météo.
   */
  def buildFlatFeatures(df: DataFrame, cfg: FeatureConfig): DataFrame = {

    val base = df.select(
      col("flight_key"),
      col("ds"),
      col("th"),
      col(cfg.labelCol).cast("double").as("label"),

      // champs présents dans F
      col("F.carrier").as("carrier_id"),
      col("F.origin_airport_id").as("origin_id"),
      col("F.dest_airport_id").as("dest_id"),
      col("F.flnum").as("flnum"),
      col("F.crs_dep_scheduled_hhmm").as("crs_dep_time"),

      // year/month sont au niveau racine
      col("year"),
      col("month").cast("int").as("month"),

      // on garde les arrays météo pour addWeatherFeatures
      col("Wo"),
      col("Wd")
    )

    // CRS_DEP_TIME (HHMM) -> minutes depuis minuit
    val withDepMinutes = base.withColumn(
      "dep_minutes",
      (col("crs_dep_time") / 100).cast("int") * 60 + (col("crs_dep_time") % 100)
    )

    // On NE met PAS Wo/Wd dans la liste de colonnes critiques pour le drop
    val cleaned = withDepMinutes.na.drop(
      "any",
      Seq(
        "label",
        "carrier_id",
        "origin_id",
        "dest_id",
        "flnum",
        "year",
        "month",
        "dep_minutes"
      )
    )

    cleaned
  }

  /**
   * Ajout dynamique des features météo Wo/Wd originHours = nombre
   * d'observations à l’origine (Wo) destHours = nombre d'observations à
   * destination (Wd)
   */
  def addWeatherFeatures(
    df: DataFrame,
    originHours: Int,
    destHours: Int
  ): DataFrame = {

    def addSide(
      dfIn: DataFrame,
      prefix: String,
      arrCol: String,
      hours: Int
    ): DataFrame =
      (0 until hours).foldLeft(dfIn) { (acc, h) =>
        val structCol = col(arrCol).getItem(h)
        wxNumericVars.foldLeft(acc) { (dfAcc, v) =>
          val fieldName = s"${prefix}_${v}"       // ex: o_vis, d_tempC
          val outCol    = s"${prefix}_h${h}_${v}" // ex: o_h0_vis
          dfAcc.withColumn(
            outCol,
            coalesce(structCol.getField(fieldName).cast("double"), lit(0.0))
          )
        }
      }

    val withOrigin =
      if (originHours > 0) addSide(df, "o", "Wo", originHours) else df
    val withDest =
      if (destHours > 0) addSide(withOrigin, "d", "Wd", destHours)
      else withOrigin

    withDest.drop("Wo", "Wd")
  }

  /**
   * Fonction principale :
   *   - lit gold/targets
   *   - filtre ds/th
   *   - construit features vols + météo (optionnelle)
   *   - split train/test
   *   - renvoie (train, test, extraNumColsMeteo)
   */
  def prepareDataset(
    spark: SparkSession,
    targetsPath: String,
    ds: String,
    th: Int,
    cfg: FeatureConfig = FeatureConfig(),
    originHours: Int = 0,
    destHours: Int = 0
  ): (DataFrame, DataFrame, Array[String]) = {

    val raw = spark.read.format("delta").load(targetsPath)

    val slice = raw
      .filter(col("ds") === lit(ds) && col("th") === lit(th))

    val n = slice.count()
    logger.info(
      s"[FeatureBuilder] Slice ds=$ds, th=$th -> $n lignes avant features (originHours=$originHours, destHours=$destHours)"
    )

    val baseFeatures = buildFlatFeatures(slice, cfg)

    // Ajout éventuel de la météo
    val withWx =
      if (originHours > 0 || destHours > 0)
        addWeatherFeatures(baseFeatures, originHours, destHours)
      else
        baseFeatures

    val n2 = withWx.count()
    logger.info(
      s"[FeatureBuilder] Slice ds=$ds, th=$th -> $n2 lignes après features (originHours=$originHours, destHours=$destHours)"
    )

    val extraNumCols = weatherFeatureNames(originHours, destHours)

    val Array(train, test) = withWx.randomSplit(
      Array(1.0 - cfg.testFraction, cfg.testFraction),
      seed = cfg.seed
    )

    logger.info(
      s"[FeatureBuilder] Split train/test (ds=$ds, th=$th, originHours=$originHours, destHours=$destHours): " +
        s"train=${train.count()}, test=${test.count()}"
    )

    (train, test, extraNumCols)
  }
}
