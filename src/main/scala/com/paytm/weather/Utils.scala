package com.paytm.weather

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.paytm.weather.Schema.getSchema

object Utils {

  def readData(loadPath: String, schemaName: String)(
    implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "false")
       .schema(getSchema(schemaName))
      .csv(loadPath)
      .cache()

  }


}
