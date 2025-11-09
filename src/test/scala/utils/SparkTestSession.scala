package utils

import org.apache.spark.sql.SparkSession
import org.scalatest.Suite

/**
 * SparkSession unique, partagé par tous les tests. Il n'est pas arrêté à la fin de chaque suite (la
 * JVM s'en charge).
 */
trait SparkTestSession { this: Suite =>

  // SparkSession global et paresseux (singleton)
  @transient lazy val spark: SparkSession = SparkSessionSingleton.spark
}

private object SparkSessionSingleton {
  @transient lazy val spark: SparkSession =
    SparkSession
      .builder()
      .appName("TemplateTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
}
