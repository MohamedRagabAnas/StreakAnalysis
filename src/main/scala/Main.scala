import org.apache.spark.sql.{Column, SparkSession}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.DoubleType;

object Main {


  def compare(str1:String, str2:String)  ={
		val  distance = StringUtils.getLevenshteinDistance(str1, str2)
    println(distance)
		val  maxLen = Math.max(str1.length, str2.length())
		 (maxLen - distance) * 1d / maxLen
	}


  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.functions._


    val spark= SparkSession.builder
     .master("local")
     .appName("StreakAnalysis")
     .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val lev_udf =spark.udf.register("compare", (str1:String, str2:String)  =>{
		val  distance = StringUtils.getLevenshteinDistance(str1, str2)
		val  maxLen = Math.max(str1.length, str2.length())
		 (maxLen - distance) * 1d / maxLen
	})


    //spark.catalog.listFunctions.filter("name like '%comapre%' ").show(false)

    /*
    val Rdd = spark.sparkContext.parallelize(
      Seq((0, "SELECT ?x ?y WHERE {?x ?p ?y}"),
          (1, "SELECT ?x ?y WHERE {?x ?p1 ?y}"),
          (2, "SELECT ?x ?y WHERE {?x ?p ?y. ?x ?p2 ?y}"),
          (3, "SELECT ?x ?y WHERE {?x ?predicate ?y. ?x ?p2 ?y. ?x ?p3 ?y.  }"),
          (4, "SELECT ?x ?y WHERE {?x ?predicate ?y. ?x ?p2 ?y. ?x ?p3 ?y.  }"),
          (5, "SELECT ?x ?y WHERE {?x ?predicate ?y. ?x ?p2 ?y. ?x ?p3 ?o.  }"),
          (6, "SELECT ?x ?y WHERE {?x ?p2 ?y}")
      ))

     */





    val Rdd =spark.sparkContext.parallelize(
      Seq(
      (0,"SELECT * WHERE { ?x a ?y.  ?i p ?j} LIMIT 20 "),
      (1,"SELECT * WHERE { ?i p ?j  ?x a ?z }"),
      (2,"SELECT * WHERE { ?i p ?j.  ?x a ?z } LIMIT 1"),
      (3,"SELECT * WHERE { ?x a ?z }"),
      (4,"SELECT * WHERE { ?i p ?j.  ?x a ?z } LIMIT 10"),
      (5,"SELECT * WHERE { ?x a ?y.  ?i p ?j} LIMIT 2000 OFFSET 3"),
      (6,"SELECT * WHERE { ?x a ?y.  ?i p ?j} LIMIT 2000 OFFSET 7"),
      (7,"SELECT * WHERE { ?x a ?y.  ?i p ?j} LIMIT 2000 OFFSET 7")
      ))




    val sentencesDF = spark.createDataFrame(Rdd).toDF("id","sentence")

    val df_new = sentencesDF.crossJoin(sentencesDF.select(col("sentence").alias("second_Sentence"), col("id").alias("second_id")))


    //val df_new_with_dist = df_new.withColumn("levehstein_distance", levenshtein(col("sentence"), col("second_Sentence"))) //using the default levenshtine spark function (return distances)
    val df_new_with_dist = df_new.withColumn("levehstein_distance", lev_udf(col("sentence"),col("second_Sentence"))) //or we use the UDF we created above an that was regsistered in Spark_session


    df_new_with_dist.createTempView("df_dist")
    spark.sql("SELECT * FROM df_dist").show()

    val min_dist_df = spark.sql("SELECT * FROM df_dist WHERE id != second_id  AND  levehstein_distance>=0.75")
    min_dist_df.createTempView("MinDF")


    spark.sql("SELECT id,collect_list(second_id) FROM MinDF group by id ").show() //to show only the query id and list of query_ids
    spark.sql("SELECT id,first(sentence),collect_list(second_id),collect_list(second_Sentence) FROM MinDF group by id ").show(false)









/*
  val min_dist_df = df_new_with_dist.where(col("id") =!= col("second_id"))
                                    .groupBy(col("id").alias("second_id"))
                                    .agg(min(col("levehstein_distance")).alias("levehstein_distance"))
                                    .filter(col("levehstein_distance")<=15)
    min_dist_df.show()

    df_new_with_dist.join(min_dist_df,df_new_with_dist("second_id")=== min_dist_df("second_id") && df_new_with_dist("levehstein_distance")=== min_dist_df("levehstein_distance"), "inner")
                    .withColumn("similar_flag", concat(concat(col("id"), lit("_"), min_dist_df("second_id"))))
                    .select("id", "sentence", "similar_flag").show(false)
     */









  }

}
