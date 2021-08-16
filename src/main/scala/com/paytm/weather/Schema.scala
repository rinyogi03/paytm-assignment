package com.paytm.weather

import org.apache.spark.sql.types.{StructField, _}

object Schema {

  def getSchema(name: String): StructType =
    name match {
      case "countries"  => countrySchema
      case "stations" => stationSchema
      case "weather"    => weatherSchema
    }

  val countrySchema: StructType = StructType(
    Array(
      StructField("COUNTRY_ABBR", StringType, nullable = false),
      StructField("COUNTRY_PART_1", StringType, nullable = false),
      StructField("COUNTRY_PART_2", StringType, nullable = true),
      StructField("COUNTRY_PART_3", StringType, nullable = true),
      StructField("COUNTRY_PART_4", StringType, nullable = true),
      StructField("COUNTRY_PART_5", StringType, nullable = true),
      StructField("COUNTRY_PART_6", StringType, nullable = true)
    )
  )

  val stationSchema: StructType = StructType(
    Array(
      StructField("STN_NO", StringType, nullable = false),
      StructField("COUNTRY_ABBR", StringType, nullable = false)
    )
  )

  val weatherSchema: StructType = StructType(
    Array(
      StructField("STN---", StringType, nullable = true),
      StructField("WBAN", StringType, nullable = true),
      StructField("YEARMODA", StringType, nullable = true),
      StructField("TEMP", DoubleType, nullable = false),
      StructField("DEWP", StringType, nullable = true),
      StructField("SLP", StringType, nullable = true),
      StructField("STP", StringType, nullable = true),
      StructField("VISIB", StringType, nullable = true),
      StructField("WDSP", StringType, nullable = true),
      StructField("MXSPD", StringType, nullable = true),
      StructField("GUST", StringType, nullable = true),
      StructField("MAX", StringType, nullable = true),
      StructField("MIN", StringType, nullable = true),
      StructField("PRCP", StringType, nullable = true),
      StructField("SNDP", StringType, nullable = true),
      StructField("FRSHTT", StringType, nullable = true)
    )
  )
}
