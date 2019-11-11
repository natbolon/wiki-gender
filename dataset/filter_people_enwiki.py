import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

ENWIKI = "enwiki_overview.json"
WIKIDATA_PEOPLE = "people_wikidata_sample.json"

# absolute path of the file - CHANGE WHEN RUN LOCAL
PATH = "/media/sofia/DATA/EPFL/Applied Data Analysis/Project/wiki-gender/data/"

# absolute path of the file - CHANGE WHEN RUN LOCAL
file = os.path.join(PATH, "enwiki_5000_lines.xml")
df = spark.read.format("com.databricks.spark.xml") \
				.options(rowTag="page") \
				.load(file)

print(df.show())
print(df.printSchema())

df = df.filter("redirect._title is null")
df_enwiki_title = df.select("title", "revision.text._VALUE").toDF("title", "text")
print(df_enwiki_title.show())

# read the wikidata codes
df_wikidata = spark.read.json(PATH + WIKIDATA_PEOPLE) 
print(df_wikidata.show())


df_biographies = df_wikidata.join(df_enwiki_title,['title'],how='inner')


print(df_biographies.show())

# save the df
people_df.write.mode('overwrite').json(PATH + "people_wikidata.json")

# woohoo!
print("!!!!!!!!!!!!!!!")