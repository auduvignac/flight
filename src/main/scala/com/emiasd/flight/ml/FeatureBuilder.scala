// com/emiasd/flight/ml/FeatureBuilder.scala
package com.emiasd.flight.ml

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

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

    // --------------------------------------------------------------
    // 1) Colonnes brutes horaires + colonnes _isna
    // --------------------------------------------------------------
    val originRaw =
      for {
        h      <- 0 until originHours
        v      <- wxNumericVars    // ex: vis, tempC, windKt, precip
        suffix <- Seq("", "_isna") // ex: o_h0_vis, o_h0_vis_isna
      } yield s"o_h${h}_${v}$suffix"

    val destRaw =
      for {
        h      <- 0 until destHours
        v      <- wxNumericVars
        suffix <- Seq("", "_isna")
      } yield s"d_h${h}_${v}$suffix"

    // --------------------------------------------------------------
    // 2) Colonnes agrégées
    // --------------------------------------------------------------
    val aggNames = Seq(
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

    val originAgg =
      if (originHours > 0) aggNames.map(n => s"o_$n")
      else Seq.empty

    val destAgg =
      if (destHours > 0) aggNames.map(n => s"d_$n")
      else Seq.empty

    // --------------------------------------------------------------
    // 3) Fusion
    // --------------------------------------------------------------
    (originRaw ++ destRaw ++ originAgg ++ destAgg).toArray
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

  // =====================================================================
  // 2) Ajout des features météo agrégés Wo / Wd
  // =====================================================================
  /**
   * Ajout de features météo agrégés à partir de Wo / Wd.
   *
   * originHours = taille de la fenêtre à l'origine (Wo) destHours = taille de
   * la fenêtre à destination (Wd)
   *
   * Pour chaque côté, on crée :
   *   - vis_avg, temp_avg, wind_avg, precip_sum
   *   - has_precip, has_heavy_precip, has_low_vis
   *   - has_ts, has_sn, has_fg
   *
   * Pas de min/max => pas de Infinity / -Infinity dans les features.
   */
  def addWeatherFeatures(
    df: DataFrame,
    originHours: Int,
    destHours: Int
  ): DataFrame = {

    // ============================================================
    // 0. Slices Wo/Wd selon les heures désirées
    // ============================================================
    val dfWithSlices =
      df
        .withColumn(
          "Wo_win",
          when(lit(originHours) > 0, slice(col("Wo"), 1, originHours))
            .otherwise(col("Wo"))
        )
        .withColumn(
          "Wd_win",
          when(lit(destHours) > 0, slice(col("Wd"), 1, destHours))
            .otherwise(col("Wd"))
        )

    // ============================================================
    // 1. Fonction fusionnée addSide
    // ============================================================
    def addSide(prefix: String, arrCol: String, hours: Int)(
      dfIn: DataFrame
    ): DataFrame =
      if (hours <= 0) {
        dfIn
      } else {

        // colonnes struct pour chaque heure
        val rows: Seq[Column] =
          (0 until hours).map(h => col(arrCol).getItem(h))

        // -----------------------------
        // 1) Colonnes brutes + flags isna
        // -----------------------------
        val dfWithRaw: DataFrame =
          (0 until hours).foldLeft(dfIn) { (acc, h) =>
            val struct = rows(h)

            wxNumericVars.foldLeft(acc) { (acc2, v) =>
              val fieldName = s"${prefix}_${v}"       // ex: o_vis
              val outCol    = s"${prefix}_h${h}_${v}" // ex: o_h0_vis

              acc2
                .withColumn(
                  outCol,
                  coalesce(struct.getField(fieldName).cast("double"), lit(0.0))
                )
                .withColumn(
                  s"${outCol}_isna",
                  when(struct.getField(fieldName).isNull, lit(1.0))
                    .otherwise(lit(0.0))
                )
            }
          }

        // -----------------------------
        // 2) Helpers pour les agrégats
        // -----------------------------
        def numCols(fieldSuffix: String): Seq[Column] =
          rows.map(_.getField(s"${prefix}_$fieldSuffix").cast("double"))

        def safeSum(cols: Seq[Column]): Column =
          cols.map(c => coalesce(c, lit(0.0))) match {
            case Seq()      => lit(0.0)
            case head +: tl => tl.foldLeft(head)(_ + _)
          }

        def anyCond(cols: Seq[Column])(f: Column => Column): Column =
          cols.map(f) match {
            case Seq()      => lit(false)
            case head +: tl => tl.foldLeft(head)(_ or _)
          }

        // colonnes numériques par type
        val visCols    = numCols("vis")
        val tempCols   = numCols("tempC")
        val windCols   = numCols("windKt")
        val precipCols = numCols("precip")

        // colonnes catégorielles
        val wxCols =
          rows.map(_.getField(s"${prefix}_wxType").cast("string"))

        // -----------------------------
        // 3) Agrégats météo version 2
        // -----------------------------
        val visAvgCol    = safeSum(visCols) / lit(hours.toDouble)
        val tempAvgCol   = safeSum(tempCols) / lit(hours.toDouble)
        val windAvgCol   = safeSum(windCols) / lit(hours.toDouble)
        val precipSumCol = safeSum(precipCols)

        val hasPrecipCol = anyCond(precipCols)(c => coalesce(c, lit(0.0)) > 0.0)
        val hasHeavyPrecipCol =
          anyCond(precipCols)(c => coalesce(c, lit(0.0)) >= 2.0)
        val hasLowVisCol = anyCond(visCols)(c => c.isNotNull.and(c < 1.0))

        val hasTsCol = anyCond(wxCols)(c => c.isNotNull.and(c.like("%TS%")))
        val hasSnCol = anyCond(wxCols)(c => c.isNotNull.and(c.like("%SN%")))
        val hasFgCol = anyCond(wxCols)(c => c.isNotNull.and(c.like("%FG%")))

        // -----------------------------
        // 4) Ajout des colonnes agrégées
        // -----------------------------
        dfWithRaw
          .withColumn(s"${prefix}_vis_avg", coalesce(visAvgCol, lit(0.0)))
          .withColumn(s"${prefix}_temp_avg", coalesce(tempAvgCol, lit(0.0)))
          .withColumn(s"${prefix}_wind_avg", coalesce(windAvgCol, lit(0.0)))
          .withColumn(s"${prefix}_precip_sum", coalesce(precipSumCol, lit(0.0)))
          .withColumn(s"${prefix}_has_precip", hasPrecipCol.cast("double"))
          .withColumn(
            s"${prefix}_has_heavy_precip",
            hasHeavyPrecipCol.cast("double")
          )
          .withColumn(s"${prefix}_has_low_vis", hasLowVisCol.cast("double"))
          .withColumn(s"${prefix}_has_ts", hasTsCol.cast("double"))
          .withColumn(s"${prefix}_has_sn", hasSnCol.cast("double"))
          .withColumn(s"${prefix}_has_fg", hasFgCol.cast("double"))
      }

    // ============================================================
    // 2. Application à origine et destination
    // ============================================================
    val withOrigin =
      if (originHours > 0) addSide("o", "Wo_win", originHours)(dfWithSlices)
      else dfWithSlices

    val withDest =
      if (destHours > 0) addSide("d", "Wd_win", destHours)(withOrigin)
      else withOrigin

    // ============================================================
    // 3. Nettoyage
    // ============================================================
    withDest.drop("Wo_win", "Wd_win")
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
    destHours: Int = 0,
    inMemoryTargets: Option[DataFrame] = None
  ): (DataFrame, DataFrame, Array[String]) = {

    val raw = inMemoryTargets.getOrElse {
      spark.read.format("delta").load(targetsPath)
    }

    val slice = raw
      .filter(col("ds") === lit(ds) && col("th") === lit(th))

    val n = slice.count()
    logger.info(
      s"[FeatureBuilder] Slice ds=$ds, th=$th -> $n lignes avant features (originHours=$originHours, destHours=$destHours)"
    )

    val baseFeatures = buildFlatFeatures(slice, cfg)

    // Ajout éventuel de la météo
    val withWxBase =
      if (originHours > 0 || destHours > 0)
        addWeatherFeatures(baseFeatures, originHours, destHours)
      else
        baseFeatures

    // Mise en cache du DataFrame final de features le temps du split / des counts
    val withWx = withWxBase.persist()

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

    // Libération mémoire du DataFrame intermédiaire
    withWx.unpersist()

    (train, test, extraNumCols)
  }
}
