// com/emiasd/flight/targets/TargetBuilder.scala
package com.emiasd.flight.targets

import com.emiasd.flight.util.SparkSchemaUtils.hasPath
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

object TargetBuilder {

  private def existingCols(
    df: DataFrame,
    candidates: Seq[String]
  ): Seq[Column] =
    candidates.filter(p => hasPath(df.schema, p)).map(col)

  private def coalesceAny(cols: Seq[Column], defaultCol: Column): Column =
    if (cols.isEmpty) defaultCol else cols.reduce((a, b) => coalesce(a, b))

  /**
   * Aplatis le minimum + dérive métriques météo + extrait une clé join
   * (flight_key).
   */
  private def normalizeLight(jt: DataFrame, thMinutes: Int): DataFrame = {
    val arrC = Seq(
      "F.arr_delay_new",
      "F.ARR_DELAY_NEW",
      "arr_delay_new",
      "ARR_DELAY_NEW"
    )
    val wdC = Seq(
      "F.weather_delay",
      "F.WEATHER_DELAY",
      "weather_delay",
      "WEATHER_DELAY"
    )
    val nasC   = Seq("F.nas_delay", "F.NAS_DELAY", "nas_delay", "NAS_DELAY")
    val alphaC = Seq("F.alpha_nas", "alpha_nas")
    val naswC = Seq(
      "F.nas_weather_delay",
      "F.NAS_WEATHER_DELAY",
      "nas_weather_delay",
      "NAS_WEATHER_DELAY"
    )
    val keyC = Seq("F.flight_key", "flight_key")

    val arr = greatest(
      coalesceAny(existingCols(jt, arrC).map(_.cast("double")), lit(0.0)),
      lit(0.0)
    ).as("ARR_DELAY_NEW")
    val wd = greatest(
      coalesceAny(existingCols(jt, wdC).map(_.cast("double")), lit(0.0)),
      lit(0.0)
    ).as("WEATHER_DELAY")
    val nas = greatest(
      coalesceAny(existingCols(jt, nasC).map(_.cast("double")), lit(0.0)),
      lit(0.0)
    ).as("NAS_DELAY")
    val alpha =
      coalesceAny(existingCols(jt, alphaC).map(_.cast("double")), lit(0.58))
        .as("alpha_nas")
    val naswOpt = existingCols(jt, naswC).headOption.map(_.cast("double"))
    val key = existingCols(jt, keyC).headOption.getOrElse {
      throw new IllegalArgumentException(
        "TargetBuilder: clé 'F.flight_key' (ou 'flight_key') introuvable dans JT"
      )
    }.as("flight_key")

    jt.select(
      key,
      arr,
      wd,
      nas,
      alpha,
      naswOpt
        .getOrElse(col("NAS_DELAY") * col("alpha_nas"))
        .as("NAS_WEATHER_DELAY")
    ).withColumn(
      "ARR_NASW",
      greatest(
        col("ARR_DELAY_NEW") - (col("NAS_DELAY") - col("NAS_WEATHER_DELAY")),
        lit(0.0)
      )
    ).withColumn(
      "has_meteo",
      col("WEATHER_DELAY") > 0.0 || col("NAS_WEATHER_DELAY") > 0.0
    ).withColumn("C", when(col("ARR_DELAY_NEW") >= thMinutes, 1).otherwise(0))
      .withColumn(
        "ratio_adj",
        (col("WEATHER_DELAY") + col("NAS_WEATHER_DELAY")) / greatest(
          col("ARR_NASW"),
          lit(1.0)
        )
      )
  }

  /**
   * Construit des **jeux de clés équilibrées** (sans Wo/Wd) pour D1..D4 à un
   * seuil métier `thMinutes`.
   */
  def buildKeysOnly(
    jt: DataFrame,
    thMinutes: Int,
    tau: Double, // ex. 0.95
    sampleSeed: Long = 42L,
    persistLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  ): Map[String, DataFrame] = {

    val light = normalizeLight(jt, thMinutes)
      .select(
        "flight_key",
        "ARR_DELAY_NEW",
        "WEATHER_DELAY",
        "NAS_DELAY",
        "NAS_WEATHER_DELAY",
        "ARR_NASW",
        "has_meteo",
        "ratio_adj",
        "C"
      )
      .persist(persistLevel)

    // Positifs (C==1) par dataset
    val d1Pos: Column =
      col("C") === 1 && col("has_meteo") && col("ARR_NASW") > 0.0 && col(
        "ratio_adj"
      ) >= tau
    val d2Pos: Column = col("C") === 1 && (col("WEATHER_DELAY") > 0.0 || col(
      "NAS_WEATHER_DELAY"
    ) >= thMinutes)
    val d3Pos: Column = col("C") === 1 && (col("WEATHER_DELAY") > 0.0 || col(
      "NAS_WEATHER_DELAY"
    ) > 0.0)
    val d4Pos: Column = col("C") === 1

    val row = light
      .agg(
        sum(when(d1Pos, 1).otherwise(0)).cast("long").as("cnt_d1"),
        sum(when(d2Pos, 1).otherwise(0)).cast("long").as("cnt_d2"),
        sum(when(d3Pos, 1).otherwise(0)).cast("long").as("cnt_d3"),
        sum(when(d4Pos, 1).otherwise(0)).cast("long").as("cnt_d4"),
        sum(when(col("C") === 0, 1).otherwise(0)).cast("long").as("cnt_neg")
      )
      .first()

    val cntD1  = row.getAs[Long]("cnt_d1")
    val cntD2  = row.getAs[Long]("cnt_d2")
    val cntD3  = row.getAs[Long]("cnt_d3")
    val cntD4  = row.getAs[Long]("cnt_d4")
    val cntNeg = math.max(1L, row.getAs[Long]("cnt_neg"))

    val neg = light
      .filter(col("C") === 0)
      .select("flight_key", "C")
      .persist(persistLevel)

    def balancedKeys(pos: DataFrame, posCount: Long): DataFrame =
      if (posCount <= 0L) {
        neg.sample(false, 1e-6, sampleSeed).withColumn("is_pos", lit(0))
      } else {
        val frac = math.min(1.0, posCount.toDouble / cntNeg.toDouble)
        val negS = if (frac >= 1.0) neg else neg.sample(false, frac, sampleSeed)
        pos
          .select("flight_key", "C")
          .withColumn("is_pos", lit(1))
          .unionByName(negS.withColumn("is_pos", lit(0)))
      }

    val D1_keys = balancedKeys(light.filter(d1Pos), cntD1)
    val D2_keys = balancedKeys(light.filter(d2Pos), cntD2)
    val D3_keys = balancedKeys(light.filter(d3Pos), cntD3)
    val D4_keys = balancedKeys(light.filter(d4Pos), cntD4)

    neg.unpersist(); light.unpersist()

    Map("D1" -> D1_keys, "D2" -> D2_keys, "D3" -> D3_keys, "D4" -> D4_keys)
  }

  /**
   * Ré-attache JT complet (Wo/Wd inclus) à un jeu de clés (utile
   * debug/inspection ponctuelle).
   */
  def materializeWithJT(
    jt: DataFrame,
    keys: DataFrame,
    includeLightCols: Boolean = true
  ): DataFrame = {
    val jtWithKey = jt.withColumn(
      "flight_key",
      coalesce(col("F.flight_key"), col("flight_key"))
    )
    val kCols = if (includeLightCols) keys.columns.toSeq else Seq("flight_key")
    jtWithKey.join(keys.select(kCols.map(col): _*), Seq("flight_key"), "inner")
  }

  @deprecated(
    "Utiliser TargetBatch.buildKeysForThresholds + TargetBatch.materializeAll (un seul join/écriture).",
    "v1.0"
  )
  def buildAll(
    jt: DataFrame,
    thMinutes: Int,
    tau: Double,
    sampleSeed: Long = 42L,
    persistLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  ): Map[String, DataFrame] =
    throw new UnsupportedOperationException(
      "Deprecated: utilisez TargetBatch.* (un seul join & write partitionné)."
    )
}
