import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import java.net.{URI, URLDecoder}

object RealLogs {


  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._
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

    //Read The DF from the TSV source
    val organic_DF= spark.read.format("csv").
                option("header", "true").  // Does the file have a header line?
                option("delimiter", "\t"). // Set delimiter to tab or comma.
                schema(simpleSchema).
                load("/home/ragab/Downloads/Angela_readings/WIKIDATA/WidkiData/2017-06-12_2017-07-09_organic.tsv")


    organic_DF.createTempView("AnonDF")

    //decode the encoded Query, alongside with getting only the first day of the logs.
    val clean_DF=spark.sql(
      """
        |SELECT decodeQuery(anonymizedQuery) AS query, timestamp, sourceCategory, user_agent
        |FROM AnonDF
        |WHERE timestamp > '2017-06-12 00:00:00' AND timestamp <= '2017-06-12 01:00:00'
        |""".stripMargin).
      withColumn("id", row_number().over(Window.orderBy(monotonically_increasing_id()))-1)

    println(clean_DF.count())

    //cross join the clean_DF to itself
    val df_new = clean_DF.select("id","query").crossJoin(clean_DF.select(col("query").alias("secondQuery"), col("id").alias("secondID")))


    // add the levenshtine similarity column to the dataframe
    val df_new_with_dist = df_new.withColumn("Levenshtein_distance", lev_udf(col("query"),col("secondQuery")))


    df_new_with_dist.createTempView("df_dist")
    spark.sql("SELECT * FROM df_dist").show()


    //Filter out the similarities less than a threshold
    val min_dist_df = spark.sql("SELECT * FROM df_dist WHERE id != secondID  AND  Levenshtein_distance >= 0.90")
    min_dist_df.createTempView("MinDF")

    val df_ids= spark.sql("SELECT id,collect_list(secondID) AS similarQueryIDs FROM MinDF group by id") //to show only the query id and list of query_ids
    df_ids.coalesce(1).write.parquet("/home/ragab/Downloads/Angela_readings/WIKIDATA/WidkiData/DF_IDS.parquet")
    val df_result= spark.sql("SELECT id,first(query) AS query ,collect_list(secondID) AS similarQueryIDs,collect_list(secondQuery) AS similarQueries FROM MinDF group by id ")
    df_result.coalesce(1).write.parquet("/home/ragab/Downloads/Angela_readings/WIKIDATA/WidkiData/DF_Results.parquet")








  }
}
