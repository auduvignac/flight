package com.emiasd.flight.targets

import com.emiasd.flight.util.SparkSchemaUtils.hasPath
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object TargetBatch {

  def buildKeysForThresholds(
    jt: DataFrame,
    ths: Seq[Int],
    tau: Double,
    sampleSeed: Long = 42L
  ): DataFrame = {
    val parts = ths.map { th =>
      val m = TargetBuilder.buildKeysOnly(
        jt,
        th,
        tau,
        sampleSeed = sampleSeed,
        // IMPORTANT : MEMORY_AND_DISK pour éviter les recalculs massifs
        persistLevel = StorageLevel.MEMORY_AND_DISK
      )
      m.map { case (name, df) =>
        df.withColumn("ds", lit(name))
          .withColumn("th", lit(th))
          .select("flight_key", "ds", "th", "is_pos", "C")
      }.reduce(_.unionByName(_))
    }

    parts
      .reduce(_.unionByName(_))
      // Idem : on accepte le spill disque plutôt que de recalculer le DAG
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
   * Matérialise tous les datasets d'un coup via un **seul** join JT ⨝
   * broadcast(keysAll).
   */
  def materializeAll(
    jt: DataFrame,
    keysAll: DataFrame,
    includeLightCols: Boolean = true
  ): DataFrame = {

    // Ajoute une colonne "flight_key" de façon SAFE (sans référencer une colonne absente)
    // A REVOIR POUR SIMPLIFIER SCHEMA JT AVEC LOCALISATION DE FLIGHT_KEY
    val jtWithKey =
      if (hasPath(jt.schema, "flight_key")) {
        // Il existe déjà une racine flight_key
        jt.withColumn(
          "flight_key",
          col(jt.schema.fieldNames.find(_.equalsIgnoreCase("flight_key")).get)
        )
      } else if (hasPath(jt.schema, "F.flight_key")) {
        jt.withColumn("flight_key", col("F.flight_key"))
      } else {
        throw new IllegalArgumentException(
          "TargetBatch.materializeAll: impossible de trouver 'F.flight_key' ni 'flight_key' dans JT."
        )
      }

    val right =
      if (includeLightCols)
        keysAll.select("flight_key", "ds", "th", "is_pos")
      else keysAll.select("flight_key", "ds", "th")

    jtWithKey.join(
      org.apache.spark.sql.functions.broadcast(right),
      Seq("flight_key"),
      "inner"
    )
  }
}
