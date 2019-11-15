import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
from params import *


def main(**params):
	params = dict(
		**params
	)

	local = params['local']

	if local:
		LOCAL_PATH = "../data/"
		WIKIDATA_PEOPLE = os.path.join(LOCAL_PATH, "people_wikidata_sample.json")
		ENWIKI = os.path.join(LOCAL_PATH, "enwiki_5000_lines.xml")

	else: # running in the cluster
		LOCAL_PATH = "hdfs:///user/gullon/"
		WIKIDATA_PEOPLE = os.path.join(LOCAL_PATH, "people_wikidata.json")
		ENWIKI = "hdfs:///datasets/enwiki-20191001/enwiki-20191001-pages-articles-multistream.xml"

	spark = SparkSession.builder.getOrCreate()
	sc = spark.sparkContext

	df = spark.read.format("com.databricks.spark.xml") \
					.options(rowTag="page") \
					.load(ENWIKI)

	print(df.printSchema())

	df = df.filter("redirect._title is null")
	df_enwiki_title = df.select("title", "revision.text._VALUE").toDF("wiki-title", "text")

	if local:
		print(df_enwiki_title.show())
	else:
		print("="*50)
		print("Got enwiki tite and text")
		print("="*50)

	# read the wikidata codes
	df_wikidata = spark.read.json(WIKIDATA_PEOPLE)

	df_biographies = df_wikidata.join(df_enwiki_title,['wiki-title'],how='inner')

	if local:
		print(df_biographies.show())
	else:
		print("="*50)
		print("Join enwiki and wikidata done")
		print("="*50)

	# save the df
	df_biographies.write.mode('overwrite').json(os.path.join(LOCAL_PATH, "biographies_wikipedia.json"))

	# woohoo!
	print("!!!!!!!!!!!!!!!")


if __name__ == '__main__':
	main(**vars(parse_arguments()))
