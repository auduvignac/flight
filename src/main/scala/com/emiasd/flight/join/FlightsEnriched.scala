// com/emiasd/flight/join/FlightsEnriched.scala
package com.emiasd.flight.join

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FlightsEnriched {

  /** Ajoute une clé de vol stable à partir du schéma réel des vols. */
  def build(flights: DataFrame): DataFrame = {
    val f = flights
      .withColumn("FL_DATE_STR", date_format(col("FL_DATE"), "yyyy-MM-dd"))
      .withColumn("FL_NUM_STR", lpad(col("FL_NUM").cast("string"), 4, "0"))

    f.withColumn(
      "flight_key",
      concat_ws(
        "-",
        col("OP_CARRIER_AIRLINE_ID").cast("string"),
        col("FL_NUM_STR"),
        col("FL_DATE_STR"),
        col("origin_airport_id").cast("string"),
        col("dest_airport_id").cast("string")
      )
    ).drop("FL_DATE_STR", "FL_NUM_STR")
  }
}
