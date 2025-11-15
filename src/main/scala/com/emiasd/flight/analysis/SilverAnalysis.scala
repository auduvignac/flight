package com.emiasd.flight.analysis

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SilverAnalysis {

  val logger = Logger.getLogger(getClass.getName)

  // ===== Public entrypoint =====
  def analyzeFlights(df: DataFrame, outDir: String): Unit = {
    logger.info("=== ANALYSE FLIGHTS SILVER ===")

    // Cache global pour éviter de rescanner la table à chaque helper
    val dfCached = df.cache()

    logger.info(s"Rows = ${dfCached.count()}")

    // s'assure que le dossier d'output existe côté driver (utile en local)
    new java.io.File(outDir).mkdirs()

    val nulls = nullsReport(dfCached)
    nulls.show(false)
    writeCsv(nulls, s"$outDir/nulls_report")

    val uniques = uniquesReport(dfCached)
    uniques.show(false)
    writeCsv(uniques, s"$outDir/uniques_report")

    val quants = quantilesReport(dfCached)
    quants.show(false)
    writeCsv(quants, s"$outDir/quantiles_report")

    val rules = delayRules(dfCached)
    rules.show(false)
    writeCsv(rules, s"$outDir/delay_rules")

    // échantillon des violations pour examen rapide
    val viol = sampleViolations(dfCached)
    viol.show(20, truncate = false)
    writeCsv(viol, s"$outDir/delay_rules_violations_sample")

    // Libération de la mémoire
    dfCached.unpersist()
  }


  // ===== Helpers =====

  private def writeCsv(df: DataFrame, path: String): Unit =
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)

  /** Nulls + NaN par colonne (NaN seulement pour Double/Float). */
  private def nullsReport(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val total = df.count()

    val rows = df.schema.fields.map { f =>
      val c       = col(f.name)
      val nullCnt = df.filter(c.isNull).count()
      val nanCnt = f.dataType match {
        case DoubleType | FloatType => df.filter(isnan(c)).count()
        case _                      => 0L
      }
      (f.name, f.dataType.simpleString, nullCnt, nanCnt, total)
    }

    rows.toSeq
      .toDF("column", "dtype", "nulls", "nans", "total")
      .withColumn("null_rate", round(col("nulls") / col("total"), 6))
      .withColumn("nan_rate", round(col("nans") / col("total"), 6))
      .orderBy(desc("nulls"))
  }

  /**
   * Cardinalités approximatives sur quelques colonnes-clés présentes en Silver.
   */
  private def uniquesReport(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val candidates = Seq(
      "FL_NUM",
      "origin_airport_id",
      "dest_airport_id",
      "year",
      "month"
    ).filter(df.columns.contains)

    if (candidates.isEmpty) {
      Seq(("__no_columns__", 0L)).toDF("column", "approx_distinct")
    } else {
      // On évite df.agg(...) : on utilise select(...) puis collect()
      val sel = df.select(
        candidates.map(c => approx_count_distinct(col(c)).alias(c)): _*
      )
      val row = sel.collect()(0) // une seule ligne avec N colonnes
      val out = candidates.map { c =>
        // getLong si la fonction renvoie Long, sinon fallback en Int
        val v =
          if (!row.isNullAt(row.fieldIndex(c))) {
            row.schema(c).dataType match {
              case LongType    => row.getLong(row.fieldIndex(c))
              case IntegerType => row.getInt(row.fieldIndex(c)).toLong
              case _ =>
                Option(row.get(row.fieldIndex(c)))
                  .map(_.toString.toLong)
                  .getOrElse(0L)
            }
          } else 0L
        (c, v)
      }
      out.toDF("column", "approx_distinct").orderBy(desc("approx_distinct"))
    }
  }

  /** Quantiles robustes sur les colonnes de délai (si présentes). */
  private def quantilesReport(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val q = Array(0.5, 0.9, 0.95, 0.99)
    val cols =
      Seq("ARR_DELAY_NEW", "WEATHER_DELAY", "NAS_DELAY", "total_weather_delay")
        .filter(df.columns.contains)

    val rows = cols.map { c =>
      val qs = df.stat.approxQuantile(c, q, 0.01)
      // qs.length == q.length
      (c, qs(0), qs(1), qs(2), qs(3))
    }

    rows.toDF("column", "p50", "p90", "p95", "p99")
  }

  /**
   * Règle métier :
   *   - if (WEATHER_DELAY == 0 && NAS_DELAY == 0) then ARR_DELAY_NEW < 15 (OK
   *     sinon VIOL)
   *   - if (WEATHER_DELAY > 0 || NAS_DELAY > 0) then ARR_DELAY_NEW >= 15 (OK
   *     sinon VIOL) S’applique en Silver (après cast / nettoyage et filtrage
   *     des vols annulés/divertis).
   */
  private def delayRules(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val bothZero_arrLT15 =
      df.filter(
        $"WEATHER_DELAY" === 0.0 && $"NAS_DELAY" === 0.0 && $"ARR_DELAY_NEW" < 15
      ).count
    val bothZero_arrGE15_viols =
      df.filter(
        $"WEATHER_DELAY" === 0.0 && $"NAS_DELAY" === 0.0 && $"ARR_DELAY_NEW" >= 15
      ).count

    val nonZero_arrGE15 =
      df.filter(
        ($"WEATHER_DELAY" =!= 0.0 || $"NAS_DELAY" =!= 0.0) && $"ARR_DELAY_NEW" >= 15
      ).count
    val nonZero_arrLT15_viols =
      df.filter(
        ($"WEATHER_DELAY" =!= 0.0 || $"NAS_DELAY" =!= 0.0) && $"ARR_DELAY_NEW" < 15
      ).count

    val total = df.count()

    Seq(
      ("bothZero -> arr<15 (OK)", bothZero_arrLT15),
      ("bothZero -> arr>=15 (VIOL)", bothZero_arrGE15_viols),
      ("nonZero -> arr>=15 (OK)", nonZero_arrGE15),
      ("nonZero -> arr<15 (VIOL)", nonZero_arrLT15_viols),
      ("total rows", total)
    ).toDF("check", "rows")
  }

  /** Échantillon des violations pour inspection. */
  private def sampleViolations(df: DataFrame): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val baseSelect = Seq(
      "FL_DATE",
      "FL_NUM",
      "origin_airport_id",
      "dest_airport_id",
      "ARR_DELAY_NEW",
      "WEATHER_DELAY",
      "NAS_DELAY",
      "total_weather_delay"
    ).filter(df.columns.contains).map(col)

    val v1 = df
      .filter(
        $"WEATHER_DELAY" === 0.0 && $"NAS_DELAY" === 0.0 && $"ARR_DELAY_NEW" >= 15
      )
      .select(baseSelect: _*)
      .withColumn("rule", lit("bothZero->arr>=15"))
      .limit(50)

    val v2 = df
      .filter(
        ($"WEATHER_DELAY" =!= 0.0 || $"NAS_DELAY" =!= 0.0) && $"ARR_DELAY_NEW" < 15
      )
      .select(baseSelect: _*)
      .withColumn("rule", lit("nonZero->arr<15"))
      .limit(50)

    v1.unionByName(v2)
  }
}
