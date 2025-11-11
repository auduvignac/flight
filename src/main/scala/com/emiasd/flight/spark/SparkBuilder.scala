// com/emiasd/flight/spark/SparkBuilder.scala
package com.emiasd.flight.spark

import com.emiasd.flight.config.AppConfig
import org.apache.spark.sql.SparkSession

object SparkBuilder {
  def build(cfg: AppConfig): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(cfg.sparkAppName)
      .config("spark.sql.extensions", cfg.sparkSqlExtensions)
      .config("spark.sql.catalog.spark_catalog", cfg.sparkSqlCatalog)
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.sql.files.maxPartitionBytes", "268435456")

    val withMaster = cfg.sparkMaster match {
      case "yarn" => builder
      case m      => builder.master(m)
    }
    cfg.sparkConfs
      .foldLeft(withMaster) { case (b, (k, v)) => b.config(k, v) }
      .getOrCreate()
  }
}
