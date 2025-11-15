// com/emiasd/flight/ml/FeatureBuilder.scala
package com.emiasd.flight.ml

import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object FeatureBuilder {
  val logger = Logger.getLogger(getClass.getName)
  /**
   * Configuration pour la préparation dataset
   */
  case class FeatureConfig(
                            labelCol: String    = "is_pos", // ou "C" si tu veux le label brut
                            testFraction: Double = 0.2,
                            seed: Long           = 42L
                          )

  // Variables météo numériques qu'on va exploiter
  private val wxNumericVars: Seq[String] = Seq(
    "vis", "tempC", "dewC", "rh",
    "windKt", "windDir",
    "altim", "slp", "stnp", "precip"
  )

  // =====================================================================
  // 1) Noms des features météo agrégés
  // =====================================================================
  /*
   * Noms des colonnes de features météo agrégés.
   *
   * On utilise des agrégats simples par côté (origine/destination) :
   *  - vis_avg, temp_avg, wind_avg, precip_sum
   *  - has_precip, has_heavy_precip, has_low_vis
   *  - has_ts, has_sn, has_fg
   *
   * => 10 features par côté.
   */

  def weatherFeatureNames(originHours: Int, destHours: Int): Array[String] = {
    val perSide = Seq(
      "vis_avg",
      "temp_avg",
      "wind_avg",
      "precip_sum",
      "has_precip",
      "has_heavy_precip",
      "has_low_vis",
      "has_ts",
      "has_sn",
      "has_fg"
    )

    val originCols =
      if (originHours > 0) perSide.map(n => s"o_$n") else Seq.empty

    val destCols =
      if (destHours > 0) perSide.map(n => s"d_$n") else Seq.empty

    (originCols ++ destCols).toArray
  }

  /**
   * Flatten de la table targets vers une table de features "vol" sans météo.
   */
  def buildFlatFeatures(df: DataFrame, cfg: FeatureConfig)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

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
    val cleaned = withDepMinutes.na.drop("any", Seq(
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



  // =====================================================================
  // 2) Ajout des features météo agrégés Wo / Wd
  // =====================================================================
  /**
   * Ajout de features météo agrégés à partir de Wo / Wd.
   *
   * originHours = taille de la fenêtre à l'origine  (Wo)
   * destHours   = taille de la fenêtre à destination (Wd)
   *
   * Pour chaque côté, on crée :
   *  - vis_avg, temp_avg, wind_avg, precip_sum
   *  - has_precip, has_heavy_precip, has_low_vis
   *  - has_ts, has_sn, has_fg
   *
   * Pas de min/max => pas de Infinity / -Infinity dans les features.
   */
  def addWeatherFeatures(
                          df: DataFrame,
                          originHours: Int,
                          destHours: Int
                        )(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    // ============================================================
    // Étape 0 : on dérive Wo_win / Wd_win en fonction des heures
    // ============================================================
    // - si originHours > 0 : on garde les originHours premières obs de Wo
    // - sinon : on garde Wo tel quel
    // - idem pour destHours / Wd
    //
    // slice(array, start, length) :
    //   start = 1 (Spark indexe à 1 pour slice)
    //   length = originHours / destHours
    val dfWithSlices =
      df
        .withColumn(
          "Wo_win",
          when(lit(originHours) > 0,
            slice(col("Wo"), 1, originHours)
          ).otherwise(col("Wo"))
        )
        .withColumn(
          "Wd_win",
          when(lit(destHours) > 0,
            slice(col("Wd"), 1, destHours)
          ).otherwise(col("Wd"))
        )

    // ============================================================
    // Helper générique pour un côté (origine / destination)
    // ============================================================
    //
    // prefix = "o" (origine) ou "d" (destination)
    // arrCol = "Wo_win" ou "Wd_win"
    // hours  = taille de fenêtre (1,3,5,7,9,11)
    def addSide(prefix: String, arrCol: String, hours: Int)(dfIn: DataFrame): DataFrame = {
      if (hours <= 0) return dfIn

      // Séquence de struct: Wo_win[h] / Wd_win[h]
      val rows: Seq[Column] = (0 until hours).map(h => col(arrCol).getItem(h))

      // === Helpers numériques ===
      def numCols(fieldSuffix: String): Seq[Column] =
        rows.map(_.getField(s"${prefix}_$fieldSuffix").cast("double"))

      def safeSum(cols: Seq[Column]): Column =
        cols
          .map(c => coalesce(c, lit(0.0)))
          .reduce((c1, c2) => c1 + c2)

      def anyCond(cols: Seq[Column])(f: Column => Column): Column =
        cols.map(f).reduce((c1, c2) => c1.or(c2))

      // Colonnes numériques par variable
      val visCols    = numCols("vis")
      val tempCols   = numCols("tempC")
      val windCols   = numCols("windKt")
      val precipCols = numCols("precip")

      // Colonnes catégorielles (type météo)
      val wxCols: Seq[Column] =
        rows.map(_.getField(s"${prefix}_wxType").cast("string"))

      // === Agrégats numériques (moyennes / somme) ===
      val visAvgCol    = safeSum(visCols)    / lit(hours.toDouble)
      val tempAvgCol   = safeSum(tempCols)   / lit(hours.toDouble)
      val windAvgCol   = safeSum(windCols)   / lit(hours.toDouble)
      val precipSumCol = safeSum(precipCols)

      // === Flags continus ===
      val hasPrecipCol = anyCond(precipCols) { c =>
        coalesce(c, lit(0.0)) > lit(0.0)
      }

      val hasHeavyPrecipCol = anyCond(precipCols) { c =>
        coalesce(c, lit(0.0)) >= lit(2.0) // seuil "forte pluie"
      }

      val hasLowVisCol = anyCond(visCols) { c =>
        c.isNotNull.and(c < lit(1.0)) // visibilité très faible
      }

      // === Flags catégoriels à partir de wxType ===
      val hasTsCol = anyCond(wxCols) { c =>
        c.isNotNull.and(c.like("%TS%"))
      }

      val hasSnCol = anyCond(wxCols) { c =>
        c.isNotNull.and(c.like("%SN%"))
      }

      val hasFgCol = anyCond(wxCols) { c =>
        c.isNotNull.and(c.like("%FG%"))
      }

      // === Application des colonnes au DataFrame ===
      dfIn
        .withColumn(s"${prefix}_vis_avg",    coalesce(visAvgCol,    lit(0.0)))
        .withColumn(s"${prefix}_temp_avg",   coalesce(tempAvgCol,   lit(0.0)))
        .withColumn(s"${prefix}_wind_avg",   coalesce(windAvgCol,   lit(0.0)))
        .withColumn(s"${prefix}_precip_sum", coalesce(precipSumCol, lit(0.0)))
        .withColumn(s"${prefix}_has_precip",       hasPrecipCol.cast("double"))
        .withColumn(s"${prefix}_has_heavy_precip", hasHeavyPrecipCol.cast("double"))
        .withColumn(s"${prefix}_has_low_vis",      hasLowVisCol.cast("double"))
        .withColumn(s"${prefix}_has_ts",           hasTsCol.cast("double"))
        .withColumn(s"${prefix}_has_sn",           hasSnCol.cast("double"))
        .withColumn(s"${prefix}_has_fg",           hasFgCol.cast("double"))
    }

    // ============================================================
    // Application conditionnelle aux deux côtés sur Wo_win/Wd_win
    // ============================================================
    val withOrigin =
      if (originHours > 0) addSide("o", "Wo_win", originHours)(dfWithSlices) else dfWithSlices

    val withDest =
      if (destHours > 0) addSide("d", "Wd_win", destHours)(withOrigin) else withOrigin

    // Optionnel : on nettoie les colonnes intermédiaires
    withDest
      .drop("Wo_win", "Wd_win") // on garde Wo/Wd si besoin ailleurs
  }


  /**
   * Fonction principale :
   *  - lit gold/targets
   *  - filtre ds/th
   *  - construit features vols + météo (optionnelle)
   *  - split train/test
   *  - renvoie (train, test, extraNumColsMeteo)
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

    implicit val s: SparkSession = spark

    val raw = spark.read.format("delta").load(targetsPath)

    val slice = raw
      .filter(col("ds") === lit(ds) && col("th") === lit(th))

    val n = slice.count()
    logger.info(s"[FeatureBuilder] Slice ds=$ds, th=$th -> $n lignes avant features (originHours=$originHours, destHours=$destHours)")

    val baseFeatures = buildFlatFeatures(slice, cfg)

    // Ajout éventuel de la météo
    val withWxBase =
      if (originHours > 0 || destHours > 0)
        addWeatherFeatures(baseFeatures, originHours, destHours)
      else
        baseFeatures

    // 🔹 On cache le DataFrame final de features le temps du split / des counts
    val withWx = withWxBase.persist()

    val n2 = withWx.count()
    logger.info(s"[FeatureBuilder] Slice ds=$ds, th=$th -> $n2 lignes après features (originHours=$originHours, destHours=$destHours)")

    val extraNumCols = weatherFeatureNames(originHours, destHours)

    val Array(train, test) = withWx.randomSplit(
      Array(1.0 - cfg.testFraction, cfg.testFraction),
      seed = cfg.seed
    )

    // (optionnel mais utile : train / test sont réutilisés dans ModelingPipeline)
    // train.cache()
    // test.cache()

    logger.info(
      s"[FeatureBuilder] Split train/test (ds=$ds, th=$th, originHours=$originHours, destHours=$destHours): " +
        s"train=${train.count()}, test=${test.count()}"
    )

    // 🔹 On libère la mémoire du DF intermédiaire
    withWx.unpersist()

    (train, test, extraNumCols)
  }
}

