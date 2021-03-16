import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.commons.lang3.StringUtils
import java.net.{URI, URLDecoder}
object Streamed {


  def main(args: Array[String]): Unit = {


    import  org.apache.spark.sql.functions._
        val spark= SparkSession.builder
     .master("local")
     .appName("parseRealLogs")
     .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val lev_udf =spark.udf.register("compare", (str1:String, str2:String)  =>{
		val  distance = StringUtils.getLevenshteinDistance(str1, str2)
		val  maxLen = Math.max(str1.length, str2.length())
		 (maxLen - distance) * 1d / maxLen
	})

    val decode_udf= spark.udf.register("decodeQuery", (input: String) => URLDecoder.decode(input,"UTF-8" ))

     //DF Schema
    val simpleSchema = StructType(Array(
            StructField("anonymizedQuery",StringType,true),
            StructField("timestamp",TimestampType,true),
            StructField("sourceCategory",StringType,true),
            StructField("user_agent", StringType, true)
            ))

    /*
    //Read The DF from the TSV source
    val organic_DF= spark.read.format("csv").
                option("header", "true").  // Does the file have a header line?
                option("delimiter", "\t"). // Set delimiter to tab or comma.
                schema(simpleSchema).
                load("/home/ragab/Downloads/Angela_readings/WIKIDATA/WidkiData/2017-06-12_2017-07-09_organic.tsv")



    val df_with_year_and_month = organic_DF
                                         .withColumn("year", year(col("timestamp").cast("timestamp")))
                                         .withColumn("month", month(col("timestamp").cast("timestamp")))
                                         .withColumn("day",  dayofmonth(col("timestamp").cast("timestamp")))





    df_with_year_and_month.groupBy("month" , "day").count().show(50)
    */

/*
    df_with_year_and_month.coalesce(1)
    .write
    .partitionBy("month","day")
    .mode("append")
    .option("header",true)
    .csv("/home/ragab/Downloads/Angela_readings/WIKIDATA/WidkiData/")
*/

    val stream= spark.readStream.schema(simpleSchema)
    .option("maxFilePerTrigger",1).csv("/home/ragab/Downloads/Angela_readings/WIKIDATA/WidkiData/")




    stream.createTempView("AnonDF")

    //decode the encoded Query, alongside with getting only the first day of the logs.
    val clean_DF=spark.sql(
      """
        |SELECT anonymizedQuery AS query, timestamp, sourceCategory, user_agent
        |FROM AnonDF
        |""".stripMargin)
      //.withColumn("id", row_number().over(Window.orderBy(monotonically_increasing_id()))-1)

    //cross join the clean_DF to itself
   // val df_new = clean_DF.select("id","query").crossJoin(clean_DF.select(col("query").alias("second_query"), col("id").alias("second_id")))


    // add the levenshtine similarity column to the dataframe
   // val df_new_with_dist = df_new.withColumn("Levenshtein_distance", lev_udf(col("query"),col("second_query")))


    //df_new_with_dist.createTempView("df_dist")
   // spark.sql("SELECT * FROM df_dist").show()



    //val destCount=stream.groupBy("sourceCategory").count()

    Thread.sleep(1000)

    val activity_query=clean_DF.writeStream.queryName("dest_counts")
      .format("console")
      .outputMode("append")
      .start()



    activity_query.awaitTermination()







  }

}
