from pyspark.sql import SparkSession

import collections

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()

rating_lines = spark.read.csv("../../../Data/spark_fun/ml-latest-small/ml-latest-small/ratings.csv",header=True,inferSchema=True).cache()
movie_lines = spark.read.csv("../../../Data/spark_fun/ml-latest-small/ml-latest-small/movies.csv",header=True,inferSchema=True).cache()

rating_lines.createOrReplaceTempView("rating_table")
movie_lines.createOrReplaceTempView("movie_table")
# movie_lines.show()
rating_info = spark.sql("SELECT movieId,avg(rating) as rat FROM rating_table group by movieId order by rat").cache()
movie_info = spark.sql("SELECT mt.movieId,mt.title FROM movie_table mt,rating_table rt where mt.movieId=rt.movieId group by mt.movieId,mt.title").cache()

movie_info.createOrReplaceTempView("movie_table")
rating_info.createOrReplaceTempView("rating_table")

rating_info = spark.sql("select mt.title,rt.rat from rating_table rt,movie_table mt where mt.movieId=rt.movieId order by rat")

# print movie_info.collect()
for teen in rating_info.collect():
    print(teen)
#
# # We can also use functions instead of SQL queries:
# rating_lines.groupBy("movieId").avg("rating").show()
#
spark.stop()
