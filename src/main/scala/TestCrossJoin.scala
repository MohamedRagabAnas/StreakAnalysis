import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import java.net.{URI, URLDecoder}

object TestCrossJoin {


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


   val df2= df_new.filter("id != second_id")

    df2.dropDuplicates(Seq("id","second_id")).orderBy("id","second_id").show(100)


    //val df_new_with_dist = df_new.withColumn("levehstein_distance", levenshtein(col("sentence"), col("second_Sentence"))) //using the default levenshtine spark function (return distances)
    val df_new_with_dist = df_new.withColumn("levehstein_distance", lev_udf(col("sentence"),col("second_Sentence"))) //or we use the UDF we created above an that was regsistered in Spark_session


    df_new_with_dist.createTempView("df_dist")
    spark.sql("SELECT * FROM df_dist").show()

    val min_dist_df = spark.sql("SELECT * FROM df_dist WHERE id != second_id  AND  levehstein_distance>=0.75")
    min_dist_df.createTempView("MinDF")



  }

}
