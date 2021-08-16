package com.paytm.weather

import com.paytm.weather.Configuration.Config
import com.paytm.weather.Utils.readData
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

object WeatherAnalysis {

  @transient lazy val log: Logger =
    org.apache.log4j.LogManager.getLogger(APP_NAME)

  val APP_NAME = "paytm-assignment"


  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config](APP_NAME) {
      head(APP_NAME)
      opt[String]('w', "input-weather")
        .required()
        .valueName("inputWeather")
        .action((x, c) => c.copy(inputWeather = x))
        .text("Input weather info")
      opt[String]('s', "input-stations")
        .required()
        .valueName("inputStations")
        .action((x, c) => c.copy(inputStations = x))
        .text("Input stations info")
      opt[String]('c', "input-countries")
        .required()
        .valueName("inputCountries")
        .action((x, c) => c.copy(inputCountries = x))
        .text("input countries info")
    }

    parser.parse(args, Config()) match {
      case Some(options) =>
        implicit val spark: SparkSession = SparkSession
          .builder()
          .config("spark.sql.session.timeZone", "UTC")
          .config("spark.speculation", "false")
          .config("mapreduce.fileoutputcommitter.algorithm.version", "2")
          .appName(APP_NAME)
          .master("local")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        log.info("Created spark session")

        process(options)
        spark.stop()

      case None =>
        log.error("Unable to parse options provided")
    }
  }

  def process(options: Config)(
    implicit spark: SparkSession): Unit = {

    val stations = loadStations()
    //  stations.show();
    val countries = loadCountries()
    //  countries.show();
    val weather = loadWeatherData()
    //  weather.show()

    val complete = prepareData(stations, countries, weather)
    //  complete.show(20)

    val weatherOverDay = getWeatherByDay(complete)
    //  weatherOverDay.show()

    // Which country had the hottest average mean temperature over the year?
    weatherOverDay.groupBy("COUNTRY_FULL")
      .agg(avg("TEMP").alias("AVG_TEMP"))
      .select(max(struct("AVG_TEMP", "COUNTRY_FULL")).alias("s"))
      .select("s.*")
      .show()

    //  Which country had the most consecutive days of tornadoes/funnel cloud formations?
    val windowOverCountryByDate = Window.partitionBy(col("COUNTRY_FULL")).orderBy(col("YEARMODA"))

    val tornodoList = weatherOverDay
      .withColumn(
        "consecutive",
        collect_list("TORNADO_STATUS").over(windowOverCountryByDate)
      )
      .withColumn("rank", rank().over(windowOverCountryByDate))
    // .cache()


    //  tornodoList.show()

    //    tornodoList
    //      .groupBy("COUNTRY_FULL")
    //      .agg(max("rank"), last("consecutive").as("consecutive"))
    //   //   .show()

    val maxDaysTornodoUdf =
      spark.udf.register("getMaxConsecutiveDays", getMaxConsecutiveDays)

    tornodoList
      .groupBy("COUNTRY_FULL")
      .agg(max("rank"), last("consecutive").as("consecutive"))
      .withColumn("maxDays", maxDaysTornodoUdf(col("consecutive")))
      .sort(col("maxDays").desc)
      .limit(1)
      .show()


    // Which country had the second highest average mean wind speed over the year?
    val windowOverAverage = Window.orderBy(col("AVG_WDSP").desc)

    weatherOverDay.groupBy("COUNTRY_FULL")
      .agg(avg("WDSP").alias("AVG_WDSP"))
      .select(col("*"), dense_rank().over(windowOverAverage).as("rank"))
      .filter(col("rank") === 2)
      .show()

  }

  def loadStations()(
    implicit spark: SparkSession): DataFrame = {
    readData("input/stationlist.csv", "stations")
  }

  def loadCountries()(
    implicit spark: SparkSession): DataFrame = {

    // handle cases for countries like multiple names like ANTIGUA, ST. KITTS, NEVIS, BARBUDA
    readData("input/countrylist.csv", "countries")
      .withColumn(
        "COUNTRY_FULL",
        concat(
          when(col("COUNTRY_PART_1").isNotNull, col("COUNTRY_PART_1")).otherwise(lit("")),
          when(col("COUNTRY_PART_2").isNotNull, col("COUNTRY_PART_2")).otherwise(lit("")),
          when(col("COUNTRY_PART_3").isNotNull, col("COUNTRY_PART_3")).otherwise(lit("")),
          when(col("COUNTRY_PART_4").isNotNull, col("COUNTRY_PART_4")).otherwise(lit("")),
          when(col("COUNTRY_PART_5").isNotNull, col("COUNTRY_PART_5")).otherwise(lit("")),
          when(col("COUNTRY_PART_6").isNotNull, col("COUNTRY_PART_6")).otherwise(lit("")))
      ).select(col("COUNTRY_ABBR"),
      col("COUNTRY_FULL"))
  }

  def loadWeatherData()(
    implicit spark: SparkSession): DataFrame = {

    readData("data/2019/part-*.csv.gz", "weather").coalesce(1)

  }

  def prepareData(stations: DataFrame, countries: DataFrame, weather: DataFrame)(
    implicit spark: SparkSession): DataFrame = {

    weather
      .join(stations, weather("STN---") === stations("STN_NO"))
      .join(countries, stations("COUNTRY_ABBR") === countries("COUNTRY_ABBR"))
      .select(col("COUNTRY_FULL"),
        col("STN_NO"),
        col("YEARMODA"),
        col("WDSP"),
        col("TEMP"),
        col("FRSHTT"))

  }

  def getWeatherByDay(complete: DataFrame)(
    implicit spark: SparkSession): DataFrame = {

    // cleanup data. replace missing values with 0
    complete
      .withColumn("TEMP", when(col("TEMP").equalTo(9999.9), 0).otherwise(col("TEMP")))
      .withColumn("YEAR", year(to_timestamp(col("YEARMODA"), "yyyymmdd")))
      .withColumn("TORNADO_STATUS", substring(col("FRSHTT"), -1, 1))
      .withColumn("WDSP", when(col("WDSP").equalTo(999.9), 0).otherwise(col("WDSP")))
      .cache()
  }

  def getMaxConsecutiveDays = (tornodoList: Seq[Any]) => {
    var count = 0
    var maxDays = 0
    tornodoList.foreach(x => {

      if (x == "0") {
        count = 0
      } else {
        count += 1
        maxDays = Math.max(maxDays, count)
      }
    })
    maxDays
  }

}
