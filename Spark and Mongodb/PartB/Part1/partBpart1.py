
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
#import pandas as pd
from pyspark.sql.functions import explode
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews") \
    .getOrCreate()

df = spark.read.format("com.mongodb.spark.sql").load()

dataFr = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
dataFr.createOrReplaceTempView("temp")
final_reviews_df = spark.sql("SELECT asin, count(*) as reviewCount, avg(overall) as rating FROM temp group by asin having reviewCount >= 100")

#get the metadata collection
metadata_df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
                            .option("uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.metadata") \
                            .load()

#Write SQL to retrive only required columns from metadata collection
columns_needed_from_metadata = ["asin","title","categories"]
final_metadata_df = metadata_df.select(columns_needed_from_metadata)
#final_metadata_df.show()

#join reviews and metadata collections
rm_df = final_reviews_df.join(final_metadata_df,final_reviews_df.asin == final_metadata_df.asin)
#rm_df.show()

#split the categories array into multiple rows
exploded_rm_df = rm_df.withColumn("categories", explode(rm_df.categories))
#exploded_rm_df.show()

#Window specifications to group by categories and sort rating
window_spec = Window.partitionBy(exploded_rm_df['categories']).orderBy(exploded_rm_df['rating'].desc())
#Use window specifications to get the highest of ratings
df_final = exploded_rm_df.withColumn("tempRank", func.dense_rank().over(window_spec)) \
                .where(func.col("tempRank") == 1) \
                .drop("tempRank").orderBy("categories") \
                .select("categories", "title", "reviewCount", "rating").collect()

file = open("output_Harshini.txt", "w")

output = map(lambda x : "%s\t%s\t%d\t%f" % (x.categories, x.title, x.reviewCount, x.rating), df_final)
for each_item in output:
        file.write(each_item + '\n')
