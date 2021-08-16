package com.paytm.weather

object Configuration {

  case class Config(
      inputWeather: String = "",
      inputStations: String = "",
      inputCountries: String = ""
  )

}
