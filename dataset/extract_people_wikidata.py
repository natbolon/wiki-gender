import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json


# LOCAL_PATH = "hdfs:///user/kypraiou/"
LOCAL_PATH = "../data/"

WIKI_DATA = "../data/wikidata_5000_lines.xml"
QID_DATA = "../data/qid_people_wikidata.csv"

# WIKI_DATA = "hdfs:///datasets/wikidatawiki/wikidatawiki-20170301-pages-articles-multistream.xml"
# QID_DATA = LOCAL_PATH + "qid_people_wikidata.csv"

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


# read the json with the info on the people
df = spark.read.format("com.databricks.spark.xml") \
				.options(rowTag="page") \
				.load(WIKI_DATA)

# we want to access revision, text, _VALUE 
df = df.select("revision.text._VALUE", "title")

# read the wikidata codes
qid_df = spark.read.csv(QID_DATA, header=True)
wikidata_qid = qid_df.select("qid").rdd.flatMap(lambda x: x).collect()
wikidata_qid = wikidata_qid[:500]

# merge on Q-code
people_df = df.where(df.title.isin(wikidata_qid))
print(people_df.show())

people_df = spark.read.json(people_df.rdd.map(lambda r: r["_VALUE"]))
people_df = people_df.select("labels.en.value", "sitelinks.enwiki.title", "claims.P21.mainsnak.datavalue.value.id").toDF("label", "title", "gender")
print(people_df.show())

# save the df
people_df.write.mode('overwrite').json(LOCAL_PATH + "people_wikidata.json")

# woohoo!
print("!!!!!!!!!!!!!!!")