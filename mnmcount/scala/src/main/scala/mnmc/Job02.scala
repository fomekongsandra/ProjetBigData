package main.scala.mnmc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object Job02 {
  def main(args: Array[String]): Unit = {
    // Configuration Spark
    val spark = SparkSession.builder.appName("TemperatureConversion").getOrCreate()

    // Chemin du fichier JSON sur HDFS
    val hdfs_path = "hdfs://localhost:9000/user/datalakemeteo/raw/meteo.json"

    // Lire les données depuis HDFS
    val weather_df = spark.read.json(hdfs_path)

    // Afficher le schéma du DataFrame
    weather_df.printSchema()

    // Effectuer la conversion Kelvin en Celsius
    val formatted_df = weather_df.select(
      col("dt_txt"),
      col("main.temp"),
      expr("(main.temp - 273.15)").alias("temp_celsius"),
      col("weather.description")
    )

    
 // Convertir temperature  Kelvin enCelsius
    val temperature = datajour
      .select(explode(col("list")).as("list_element"))
      .select(
        expr("CAST(list_element.main.temp_min AS DOUBLE) - 273.15")
          .as("celsius")
      )

    // Afficher le DataFrame résultant
    formatted_df.show(truncate = false)

    // Sauvegarder les résultats dans HDFS
    val formatted_hdfs_path = "hdfs://localhost:9000/user/datalakemeteo/formatted"
    formatted_df.write.mode("overwrite").json(formatted_hdfs_path)

    println(s"Les données formatées ont été sauvegardées dans HDFS : $formatted_hdfs_path")

    // Arrêter la session Spark
    spark.stop()
  }
}
