import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

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

// ------------------------------------------------------Modifications--------------------------------------------------------------------------------

  println("df2M")
  var df2M = df2.drop("chariter", "cifsn", "fileid", "stusab", "LOGRECNO")
  //df2M = df2M.withColumnRenamed("LOGRECNO","LOGRECNO2")

  var df1M = df1.withColumn("id1", monotonically_increasing_id)  // Makes id column to join
  df2M = df2M.withColumn("id2", monotonically_increasing_id)    // Then it will be dropped

  df1M.createOrReplaceTempView("Co1ImpM")
  df1M.show()

  df2M.createOrReplaceTempView("Co2ImpM")
  df2M.show()

  var export1 = df1M.join(df2M,col("id1")===col("id2"),"inner") //Joines with id column then drops it.
    .drop("id1","id2")

  file.outputcsv("CoCombine1",export1)
  export1.show()

  spark.sql("SHOW DATABASES").show()
  spark.sql("SHOW TABLES").show()

  val duration = (System.nanoTime - t1)
  println("Code Lasted: " + (duration/1000000000) + " Seconds")
}
