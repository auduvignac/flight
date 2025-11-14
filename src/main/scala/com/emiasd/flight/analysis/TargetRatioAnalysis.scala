// com/emiasd/flight/analysis/TargetRatioAnalysis.scala
package com.emiasd.flight.analysis

import com.emiasd.flight.util.SparkSchemaUtils.hasPath
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.Locale

object TargetRatioAnalysis {

  val logger = Logger.getLogger(getClass.getName)

  private def firstExisting(
    df: DataFrame,
    candidates: Seq[String]
  ): Option[String] =
    candidates.find(p => hasPath(df.schema, p))

  /**
   * Analyse des ratios "météo" pour calibrer D1 et compter τ=1. Gère
   * automatiquement les colonnes imbriquées dans F (ex: F.arr_delay_new) ou
   * plates.
   *
   * @param jt
   *   DataFrame joint (F, Wo, Wd + arr_delay_new, weather_delay, nas_delay,
   *   [nas_weather_delay?], [alpha_nas?])
   * @param outDir
   *   dossier de sortie pour CSV (ex. "analysis/targets")
   * @param tauGrid
   *   liste de seuils τ à tester (par défaut 0.80 → 1.00)
   * @param eps
   *   tolérance absolue (min) dans le dénominateur (1.0 minute conseillé)
   * @param tolTau1Strict
   *   tolérance autour de 1.0 pour la version "strict" (égalité approchée), ex.
   *   1e-6
   * @param tolTau1Loose
   *   tolérance "réaliste" pour τ=1.0 (afin d’absorber arrondis), ex. 0.01
   */
  def run(
    jt: DataFrame,
    outDir: String,
    tauGrid: Seq[Double] = Seq(0.80, 0.85, 0.90, 0.92, 0.95, 0.98, 1.00),
    eps: Double = 1.0,
    tolTau1Strict: Double = 1e-6,
    tolTau1Loose: Double = 0.01
  )(): Unit = {

    // 1) chemins candidats (imbriqués F.* et plats), casse tolérée
    val arrP = firstExisting(
      jt,
      Seq(
        "F.arr_delay_new",
        "F.ARR_DELAY_NEW",
        "arr_delay_new",
        "ARR_DELAY_NEW"
      )
    )
    val wdP = firstExisting(
      jt,
      Seq(
        "F.weather_delay",
        "F.WEATHER_DELAY",
        "weather_delay",
        "WEATHER_DELAY"
      )
    )
    val nasP = firstExisting(
      jt,
      Seq("F.nas_delay", "F.NAS_DELAY", "nas_delay", "NAS_DELAY")
    )
    val naswP = firstExisting(
      jt,
      Seq(
        "F.nas_weather_delay",
        "F.NAS_WEATHER_DELAY",
        "nas_weather_delay",
        "NAS_WEATHER_DELAY"
      )
    )
    val alphaP = firstExisting(jt, Seq("F.alpha_nas", "alpha_nas"))

    // 2) contrôles minimaux : il nous faut au moins ARR + WEATHER + NAS
    val missing = Seq(
      "ARR_DELAY_NEW" -> arrP,
      "WEATHER_DELAY" -> wdP,
      "NAS_DELAY"     -> nasP
    ).collect { case (n, None) => n }
    require(
      missing.isEmpty,
      s"Colonnes manquantes dans JT: ${missing.mkString(", ")}. " +
        "Assure-toi que F contient arr_delay_new, weather_delay, nas_delay (ou versions plates)."
    )

    // 3) aplanissement et normalisation
    val base0 = jt
      .withColumn(
        "ARR_DELAY_NEW",
        coalesce(col(arrP.get).cast("double"), lit(0.0))
      )
      .withColumn(
        "WEATHER_DELAY",
        coalesce(col(wdP.get).cast("double"), lit(0.0))
      )
      .withColumn("NAS_DELAY", coalesce(col(nasP.get).cast("double"), lit(0.0)))
      // si nas_weather_delay est fourni, on l'utilise ; sinon on tentera alpha*NAS
      .withColumn(
        "NAS_WEATHER_DELAY_raw",
        naswP
          .map(p => col(p).cast("double"))
          .getOrElse(typedLit(None: Option[Double]))
      )
      .withColumn(
        "alpha_nas",
        coalesce(
          alphaP
            .map(col)
            .map(_.cast("double"))
            .getOrElse(typedLit(None: Option[Double])),
          lit(0.58)
        )
      )
      .withColumn("ARR_DELAY_NEW", greatest(col("ARR_DELAY_NEW"), lit(0.0)))
      .withColumn("WEATHER_DELAY", greatest(col("WEATHER_DELAY"), lit(0.0)))
      .withColumn("NAS_DELAY", greatest(col("NAS_DELAY"), lit(0.0)))
      .persist()

    // 4) NAS météo + métriques dérivées
    val base = base0
      .withColumn(
        "NAS_WEATHER_DELAY",
        coalesce(
          col("NAS_WEATHER_DELAY_raw"),
          col("alpha_nas") * col("NAS_DELAY")
        )
      )
      // ARR_NASW = ARR_DELAY_NEW - NAS_non_meteo
      .withColumn(
        "ARR_NASW",
        col("ARR_DELAY_NEW") - (col("NAS_DELAY") - col("NAS_WEATHER_DELAY"))
      )
      .withColumn("ARR_NASW", greatest(col("ARR_NASW"), lit(0.0)))
      .withColumn(
        "ratio_adj",
        (col("WEATHER_DELAY") + col("NAS_WEATHER_DELAY")) / greatest(
          col("ARR_NASW"),
          lit(eps)
        )
      )
      .withColumn(
        "has_meteo",
        col("WEATHER_DELAY") > 0.0 || col("NAS_WEATHER_DELAY") > 0.0
      )
      .persist()

    val totalRows     = base.count()
    val rowsWithMeteo = base.filter(col("has_meteo")).count()

    // 5) Comptages τ=1
    val countTau1Strict = base
      .filter(
        col("has_meteo") && col("ARR_NASW") > 0.0 && abs(
          col("ratio_adj") - 1.0
        ) <= tolTau1Strict
      )
      .count()

    val countTau1Loose = base
      .filter(
        col("has_meteo") && col("ARR_NASW") > 0.0 && col(
          "ratio_adj"
        ) >= (1.0 - tolTau1Loose)
      )
      .count()

    // ==== 6) Volumes pour une grille de τ (D1_proxy = ratio_adj ≥ τ) ====
    // Locale-agnostic + sans action dans une transformation (OK SPARK-28702)
    val dfs =
      new DecimalFormat("0.00", DecimalFormatSymbols.getInstance(Locale.ROOT))
    def fmtTau(t: Double): String = dfs.format(t) // "0.80"
    def safeColName(t: Double): String =
      "tau_" + fmtTau(t).replace('.', '_') // "tau_0_80"

    val taus = tauGrid.distinct.sorted

    // Agrégation: une colonne par τ (un seul job)
    val aggExprs = taus.map { t =>
      sum(
        when(
          col("has_meteo") && col("ARR_NASW") > 0.0 && col("ratio_adj") >= lit(
            t
          ),
          1
        )
          .otherwise(0)
      ).cast("long").as(safeColName(t))
    }
    val aggRow = base.agg(aggExprs.head, aggExprs.tail: _*)

    // Dépivotage colonnes → lignes (tau, count) via stack, en injectant la valeur τ au format "0.80"
    val pairs =
      taus.map(t => s"'${fmtTau(t)}', `${safeColName(t)}`").mkString(",")
    val countsByTau = aggRow
      .selectExpr(s"stack(${taus.size}, $pairs) as (tau_str, count)")
      .withColumn("tau", col("tau_str").cast("double"))
      .select("tau", "count")
      .orderBy("tau")

    // 7) Histogramme du ratio_adj (buckets 0.05)
    val bucketUdf = udf { r: Double =>
      if (r.isNaN || r.isInfinite) "NaN/Inf"
      else if (r < 0.0) "<0.00"
      else if (r >= 1.20) ">=1.20"
      else {
        val k  = Math.floor(r / 0.05).toInt
        val lo = k * 0.05
        val hi = lo + 0.05
        f"[$lo%1.2f,$hi%1.2f)"
      }
    }

    val hist = base
      .select(col("ratio_adj"))
      .withColumn("bucket", bucketUdf(col("ratio_adj")))
      .groupBy("bucket")
      .count()
      .orderBy("bucket")

    // 8) Sauvegardes + logs
    val out = if (outDir.endsWith("/")) outDir.dropRight(1) else outDir
    base.unpersist(); base0.unpersist()

    logger.info("=== TARGET RATIO ANALYSIS ===")
    logger.info(f"Total rows                      : $totalRows%,d")
    logger.info(f"Rows with any meteo (has_meteo) : $rowsWithMeteo%,d")
    logger.info(f"tau=1 strict  (±$tolTau1Strict): $countTau1Strict")
    logger.info(
      s"tau=1 loose   (≥${dfs.format(1.0 - tolTau1Loose)}): $countTau1Loose"
    )

    countsByTau
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$out/counts_by_tau")

    hist
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$out/hist_ratio_adj")

    logger.info("=== Counts by tau ==="); countsByTau.show(truncate = false)
    logger.info("=== Histogram ratio_adj ==="); hist.show(200, truncate = false)
  }
}
