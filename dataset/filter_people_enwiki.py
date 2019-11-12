import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

LOCAL_PATH = "hdfs:///user/gullon/"
# LOCAL_PATH = "../data/"

# WIKIDATA_PEOPLE = "people_wikidata_sample.json"
WIKIDATA_PEOPLE = "people_wikidata.json"

ENWIKI = "hdfs:///datasets/enwiki-20191001/enwiki-20191001-pages-articles-multistream.xml"
# ENWIKI = "../data/enwiki_5000_lines.xml"

# absolute path of the file - CHANGE WHEN RUN LOCAL
df = spark.read.format("com.databricks.spark.xml") \
				.options(rowTag="page") \
				.load(ENWIKI)

# print(df.show())
print(df.printSchema())

df = df.filter("redirect._title is null")
df_enwiki_title = df.select("title", "revision.text._VALUE").toDF("title", "text")
# print(df_enwiki_title.show())
print("================================")
print("enwiki tite and text done")
print("================================")


# read the wikidata codes
df_wikidata = spark.read.json(LOCAL_PATH + WIKIDATA_PEOPLE)
# print(df_wikidata.show())

df_biographies = df_wikidata.join(df_enwiki_title,['title'],how='inner')

print("================================")
print("enwiki and wikidata join done")
print("================================")

# print(df_biographies.show())

# save the df
people_df.write.mode('overwrite').json(LOCAL_PATH + "biographies_wikipedia.json")

# woohoo!
print("!!!!!!!!!!!!!!!")
