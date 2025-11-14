package com.emiasd.flight.util

import org.apache.spark.sql.types.StructType

object SparkSchemaUtils {

  /**
   * Vérifie la présence d'un champ dans un schéma Spark, insensible à la casse
   * et compatible StructType imbriqués.
   */
  def hasPath(schema: StructType, path: String): Boolean = {
    val parts = path.split("\\.")
    def loop(st: StructType, i: Int): Boolean =
      if (i >= parts.length) true
      else
        st.find(_.name.equalsIgnoreCase(parts(i))) match {
          case Some(f) =>
            f.dataType match {
              case s: StructType => loop(s, i + 1)
              case _             => i == parts.length - 1
            }
          case None => false
        }
    loop(schema, 0)
  }
}
