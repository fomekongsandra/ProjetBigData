spark-submit --class main.scala.mnmc.MnMcount /home/ubuntu/Downloads/mnmcount/scala/target/scala-2.12/main-scala-mnmc_2.12-1.0.jar


val cheminpath =
      "hdfs://localhost:9000/user/datalake/meteoUsage/Datajours4/"+currentDate+".json"

    


    val JointureDF = s"$cheminpath/jointureDF"
    temp_dateJoin.write.mode("overwrite").parquet(JointureDF)

    val JointureDF2 = s"$cheminpath/jointureDF2"
    temp_dateJoin.write.mode("overwrite").csv(JointureDF2)

 

    // Other commented-out code...
  
spark-submit --class main.scala.chapter2.MnMcount C:\Users\pasca\Documents\GitHub\PR7BIGDATA\processe\job.01\mnmcount\scala\target\scala-2.12\main-scala-mnm_2.12-1.0.jar C:/Users/pasca/Documents/GitHub/PR7BIGDATA/getData/storage/Data15jours4.json

spark-submit --class main.scala.chapter2.Elastik --packages org.elasticsearch:elasticsearch-spark-30_2.12:7.17.16 C:\Users\pasca\Documents\GitHub\PR7BIGDATA\processe\job.01\mnmcount\scala\target\scala-2.12\main-scala-mnm_2.12-1.0.jar

spark-submit --class main.scala.mnm.MnMcount --packages org.elasticsearch:elasticsearch-spark-30_2.12:7.17.16 /Users/juniortemgoua/Downloads/mnmcount/scala/target/scala-2.12/main-scala-mnm_2.12-1.0.jar

spark-submit --class main.scala.chapter2.Elastik --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0 C:\Users\pasca\Documents\GitHub\PR7BIGDATA\processe\job.01\mnmcount\scala\target\scala-2.12\main-scala-mnm_2.12-1.0.jar 

//ubuntu
spark-submit --class main.scala.mnmc.MnMcount /home/ubuntu/Downloads/mnmcount/processe/scala/target/scala-2.12/main-scala-mnmc_2.12-1.0.jar /home/ubuntu/Downloads/mnmcount/processe/storage

//hdfs

su hadoop
start-dfs.sh
start-yarn.sh
//pour les droits

hdfs dfs -chmod 777 /user/datalake
hdfs dfs -shown ubuntu:unbuntu /user/datalake

//airflow
airflow standalone 
    //pour l'import
    from airflow.providers.apache.spark.operators.spark_submit 
    pip install apache-airflow-providers-apache-spark
// pour zipe
zip -r nomduzip.zip nom du dossier

// pour vider le cache a chaque 
nom du dataframe.cache
df.cache()
// pour repartitionner le disque 
val partionedDF = df.repartition(8)

lien GitHub
