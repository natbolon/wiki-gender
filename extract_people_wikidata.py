import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json

WIKIDATA_VALUE_JSON = "wikidata_value.json"
WIKIDATA_PEOPLE_JSON = "wikidata_people.json"

# absolute path of the file - CHANGE WHEN RUN LOCAL
LOCAL_PATH = "hdfs:///user/kypraiou/"
WIKI_DATA = "hdfs:///datasets/wikidatawiki/wikidatawiki-20170301-pages-articles-multistream.xml"

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

df = spark.read.format("com.databricks.spark.xml") \
				.options(rowTag="page") \
				.load(WIKI_DATA)

# select only those that start with Q: it corresponds to a legal page
df  = df.where(col('title').startswith("Q")) 

# we want to access revision, text, _VALUE 
df_text_value = df.select("revision.text._VALUE", "title")
# print(df_text_value.show())

# convert to json and save it 
df_text = df_text_value.select("_VALUE")
df_text.write.mode('overwrite').json(PATH + WIKIDATA_VALUE_JSON)


##### readjson.py
read_text = spark.read.json(PATH + WIKIDATA_VALUE_JSON)
# print(read_text.printSchema())

read_text = read_text.select("_VALUE")

# print(read_text.printSchema())


##### try the parallel - https://stackoverflow.com/questions/41107835/pyspark-parse-a-column-of-json-strings/51072232
new_df = spark.read.json(read_text.rdd.map(lambda r: r["_VALUE"]))

#### SAVE THE SCHEMA
# print(new_df.printSchema())
# v = new_df._jdf.schema().treeString()
# sc.parallelize([v]).saveAsTextFile(PATH+"schema.txt")

# print(new_df.select("claims.P31.mainsnak.datavalue.value.numeric-id", "labels.en.value", "sitelinks.enwiki.title").show())


# Filter on people
people_df = new_df.filter(array_contains(new_df['claims']['P31']['mainsnak']['datavalue']['value']['numeric-id'],5)) 
# print(people_df.select("claims.P31.mainsnak.datavalue.value.numeric-id", "labels.en.value", "sitelinks.enwiki.title").show())

# save the df
people_df.write.mode('overwrite').json(PATH + WIKIDATA_PEOPLE_JSON)


# woohoo!
print("!!!!!!!!!!!!!!!")