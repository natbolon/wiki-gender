import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pandas as pd
import json

WIKIDATA_VALUE_JSON = "wikidata_value.json"
WIKIDATA_PEOPLE_JSON = "wikidata_people.json"

LOCAL_PATH = "hdfs:///user/kypraiou/"
# LOCAL_PATH = "/media/sofia/DATA/EPFL/Applied Data Analysis/"
# WIKI_DATA = "hdfs:///datasets/wikidatawiki/wikidatawiki-20170301-pages-articles-multistream.xml"
WIKI_DATA = "../data/wikidata_5000_lines.xml"
QID_DATA = "../data/qid_people_wikidata.csv"
# WIKI_DATA = "/media/sofia/DATA/EPFL/Applied Data Analysis/first_1000_lines_wikidata.xml"

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# df = spark.read.format("com.databricks.spark.xml") \
# 				.options(rowTag="page") \
# 				.load(WIKI_DATA)

# # we want to access revision, text, _VALUE 
# df_text_value = df.select("revision.text._VALUE", "title")

# # convert to json and save it 
# df_text = df_text_value.select("_VALUE")
# df_text.write.mode('overwrite').json(LOCAL_PATH + WIKIDATA_VALUE_JSON)

# print("saved wiki data value json")

# read the json with the info on the people
df = spark.read.format("com.databricks.spark.xml") \
				.options(rowTag="page") \
				.load(WIKI_DATA)

# we want to access revision, text, _VALUE 
df = df.select("revision.text._VALUE", "title")
print(df.show())

# read the wikidata codes
qid_df = pd.read_csv(QID_DATA)
wikidata_qid = qid_df.qid.values.tolist()
wikidata_qid = wikidata_qid[:500]

# merge on Q-code
people_df = df.where(df.title.isin(wikidata_qid))

print(people_df.show())

##### try the parallel - https://stackoverflow.com/questions/41107835/pyspark-parse-a-column-of-json-strings/51072232
people_df = spark.read.json(people_df.rdd.map(lambda r: r["_VALUE"]))


# print(df.select("claims.P31.mainsnak.datavalue.value.numeric-id", "labels.en.value", "sitelinks.enwiki.title", "claims.P21.mainsnak.datavalue.value.id").show())
people_df = people_df.select("labels.en.value", "sitelinks.enwiki.title", "claims.P21.mainsnak.datavalue.value.id").toDF("label", "title", "gender")
print(people_df.show())

# # save the df
people_df.write.mode('overwrite').json("../data/people_wikidata.json")

# woohoo!
print("!!!!!!!!!!!!!!!")