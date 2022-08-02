import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object transforming extends App{
  val t1 = System.nanoTime

  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)
  println("Created spark session.")

  println("df1")
  val df1 = spark.read.format("csv").option("header", "true").load("D:\\Revature\\DowloadDataScala\\CSVs\\co00001.csv") // s"s3a://$bucket/time_series_covid_19_confirmed.csv
  df1.createOrReplaceTempView("Co1Imp")
  df1.show()
//  df1.write.mode("overwrite").saveAsTable("Co1")
//  spark.sql("SELECT * FROM Co1").show()

  println("df2")
  val df2 = spark.read.format("csv").option("header", "true").load("D:\\Revature\\DowloadDataScala\\CSVs\\co00002.csv") // s"s3a://$bucket/time_series_covid_19_confirmed.csv
  df2.createOrReplaceTempView("Co2Imp")

//  df2.write.mode("overwrite").saveAsTable("Co2")
//  spark.sql("SELECT * FROM Co2").show()

  println("df2M")
  val df2M = df2.drop("chariter", "cifsn", "fileid", "stusab")
  df2M.createOrReplaceTempView("Co2ImpM")
  df2M.show()

  println("export1")
  var export1 = df1.join(df2M,"LOGRECNO")
  export1.show()
    //spark.sql(f"SELECT * FROM Co1Imp tbl1, Co2ImpM tbl2 where tbl1.LOGRECNO == tbl2.LOGRECNO")
  file.outputcsv("CoCombine1",export1)


  spark.sql("SHOW DATABASES").show()
  spark.sql("SHOW TABLES").show()

  val duration = (System.nanoTime - t1)
  println("Code Lasted: " + (duration/1000000000) + " Seconds")
}
