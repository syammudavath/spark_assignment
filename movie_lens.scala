// Databricks notebook source
//defining the schedma for each dataset
import org.apache.spark.rdd.RDD
case class Rating(user_ID: Integer, movie_ID: Integer, rating: Integer, timestamp: String)
case class Movie(movie_ID: Integer, title: String, genre: String)
case class User(user_ID: Integer, gender: String, age: Integer, occupation: String, zip_code: String)
//parsing the data for rating
def parseRatings(row: String): Rating = {
    val splitted = row.split("::").map(_.trim).toList
    return Rating(splitted(0).toInt, splitted(1).toInt, splitted(2).toInt, splitted(3))
}

// COMMAND ----------

//parsing the data for movies
def parseMovies(row: String): Movie = {
    val splitted = row.split("::").map(_.trim).toList
    return Movie(splitted(0).toInt, splitted(1), splitted(2))
}

val movies = sc.textFile("data/movies.dat").map(element => parseMovies(element)).cache

// COMMAND ----------

//loading data for both dataset and converting as dataframe
val movies = sc.textFile("s3://hp-bigdata-prod-enrichment/shyam/movies.dat").map(element => parseMovies(element)).toDF()

val ratings = sc.textFile("s3://hp-bigdata-prod-enrichment/shyam/ratings.dat").map(element => parseRatings(element)).toDF()


// COMMAND ----------

// joining movie data frame with rating data frame
val join_movieRating_df = movies.join(ratings, "movie_ID").cache()

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
val windowSpec = Window.partitionBy("user_ID").orderBy("rating")



// COMMAND ----------

import org.apache.spark.sql.functions._

// adding and 3 new columns max, min and average rating for that movie from the ratings data.
val max_avg_min_df=  join_movieRating_df.select("movie_ID", "rating").groupBy("movie_ID").agg(max("rating") as ("maximum_rating"), min("rating") as ("minimum_rating"), avg("rating") as ("average_rating")).orderBy(asc("movie_ID"))

// new dataframe which contains each user’s (userId in the ratings data) top 3
// movies based on their rating.
val max_avg_min_df1=join_movieRating_df.withColumn("top3",dense_rank().over(windowSpec))
max_avg_min_df1.where(col("top3")>=2).orderBy(desc("top3")).show()




// COMMAND ----------

// writing final dtaframe to s3 location
max_avg_min_df.write.repartition(20).format("csv").save("path")
max_avg_min_df1.write.repartition(20).format("csv").save("path")


// COMMAND ----------


