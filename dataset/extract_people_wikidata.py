import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
from params import *


def only_enwiki(line): 
	d = json.loads(line._VALUE)
	try:
		sl = d['sitelinks']
		l = d['labels']
		c = d['claims']
		if "enwiki" in sl and "en" in l and "P21" in c:
			return True
		else:
			return False
	except KeyError: 
		return False


def main(**params):
	params = dict(
		**params
	)

	local = params['local']

	if local:
		LOCAL_PATH = "../data/"
		WIKI_DATA = os.path.join(LOCAL_PATH, "wikidata_5000_lines.xml")
		QID_DATA = os.path.join(LOCAL_PATH, "qid_people_wikidata.csv")

	else: # running in the cluster
		LOCAL_PATH = "hdfs:///user/gullon/"
		WIKI_DATA = "hdfs:///datasets/wikidatawiki/wikidatawiki-20170301-pages-articles-multistream.xml"
		QID_DATA = os.path.join(LOCAL_PATH, "qid_people_wikidata.csv")

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
	qid_df = qid_df.select("qid", "gender").toDF("title", "gender")

	people_df = qid_df.join(df,['title'],how='inner')

	# wikidata_qid = qid_df.select("qid").rdd.flatMap(lambda x: x).collect()
	#
	# if local:
	# 	wikidata_qid = wikidata_qid[:500]
	#
	# # merge on Q-code
	# people_df = df.where(df.title.isin(wikidata_qid))

	people_df.printSchema()

	if local:
		print(people_df.show())
	else:
		print("="*50)
		print("Wikidata filtered by Q code!")
		print("="*50)

	# people_df.write.mode('overwrite').json(os.path.join(LOCAL_PATH, "error.json"))

	people_filtered = people_df.select("_VALUE").rdd.filter(only_enwiki)

	people_fil_df = spark.read.json(people_filtered.map(lambda r: r["_VALUE"]))

	people_fil_df = people_fil_df.select("labels.en.value", "sitelinks.enwiki.title", "claims.P21.mainsnak.datavalue.value.id").toDF("label", "title", "gender")

	if local:
		people_fil_df.show()
	else:
		print("="*50)
		print("Select label, title and gender Wikidata!")
		print("="*50)

	# save the df
	people_fil_df.write.mode('overwrite').json(os.path.join(LOCAL_PATH, "people_wikidata.json"))

	# woohoo!
	print("!!!!!!!!!!!!!!!")


if __name__ == '__main__':
	main(**vars(parse_arguments()))
