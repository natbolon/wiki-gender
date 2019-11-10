import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import pandas as pd
import json

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

WIKI_CODES = "wikidata_codes_people.csv"
WIKI_SAMPLE = "sample_200.json"

# absolute path of the file - CHANGE WHEN RUN LOCAL
PATH = "/media/sofia/DATA/EPFL/Applied Data Analysis/"


# read the wikidata codes
df = pd.read_csv(PATH + WIKI_CODES) 
wikidata_qid = df.qid.values.tolist()

# read the json with the info on the people
read_text = spark.read.json(PATH + WIKI_SAMPLE)
read_text = read_text.select("_VALUE")

##### try the parallel - https://stackoverflow.com/questions/41107835/pyspark-parse-a-column-of-json-strings/51072232
df = spark.read.json(read_text.rdd.map(lambda r: r["_VALUE"]))

# select the first 100 people ids
wikidata_qid = wikidata_qid[:100]

people_df = df.where(df.id.isin(wikidata_qid))

# print(people_df.show())
# print(people_df.select("claims.P31.mainsnak.datavalue.value.numeric-id", "labels.en.value", "sitelinks.enwiki.title", "claims.P21.mainsnak.datavalue.value.id").show())

people_df = people_df.select("labels.en.value", "sitelinks.enwiki.title", "claims.P21.mainsnak.datavalue.value.id").toDF("label", "wiki title", "gender")
print(people_df.show())

# save the df
people_df.write.mode('overwrite').json(PATH + "people_wikidata.json")

# woohoo!
print("!!!!!!!!!!!!!!!")