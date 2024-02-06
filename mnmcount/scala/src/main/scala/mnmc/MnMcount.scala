// scalastyle:off println

package main.scala.mnmc

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object MnMcount{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MnMcount")
      .getOrCreate()

    // Charger les données JSON dans un DataFrame
    val moviesDF = spark.read.option("multiline", true).option("inferSchema",true).json(s"hdfs://localhost:9000/user/datalakefilm/films/all_popular_movies.json")

    // Convertir la colonne 'release_date' en format DateType
    val moviesWithYearDF = moviesDF.withColumn("year", year(col("release_date")))
    //supprimer les null dans la colonne year
    val filteredColumnsDF = moviesWithYearDF.filter("year is not null")
    //lister les totaux des films par annee
    val moviesCountByYearDF = filteredColumnsDF.groupBy("year").agg(count("*").alias("film_count")).orderBy("year")
    // la langue des films les plus regardes par annee
    val selectedColumnsDF = moviesWithYearDF.select("genre_ids","original_language","original_title","popularity","vote_average","vote_count","year")
    val mostWatchedLanguageDF = selectedColumnsDF.groupBy("year", "original_language").agg(max("vote_count").alias("max_vote_count")).orderBy(col("year"), desc("max_vote_count"))

    

    val outputPath = "hdfs://localhost:9000/user/datalakefilm/films/analyse_data"
    val outputPath2 = "hdfs://localhost:9000/user/datalakefilm/films/analyse2_data"
    moviesCountByYearDF.write.mode(SaveMode.Overwrite).parquet(outputPath)
    mostWatchedLanguageDF.write.mode(SaveMode.Overwrite).parquet(outputPath2)



    // Arrêt de la session Spark
    spark.stop()
  }
}


