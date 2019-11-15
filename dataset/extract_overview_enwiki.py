import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re
from params import *


def get_overview(text):
	starting_expr = re.compile("'''")
	ending_expr = re.compile("==")
	ending_expr_cat = re.compile("\[\[Category:") # entries that don't have another section 
	
	try:
		starting_idx = starting_expr.search(text).span()[0]
	except:
		starting_idx = None
	try:
		ending_idx = ending_expr.search(text).span()[0]
	except:
		try:
			ending_idx = ending_expr_cat.search(text).span()[0]
		except:
			ending_idx = None
	return text[starting_idx:ending_idx]


def main(**params):
	params = dict(
		**params
	)

	local = params['local']

	if local:
		LOCAL_PATH = "../data/"
		BIO_WIKI = os.path.join(LOCAL_PATH, "biographies_wikipedia.json")
	else: # running in the cluster
		LOCAL_PATH = "hdfs:///user/gullon/"
		BIO_WIKI = os.path.join(LOCAL_PATH, "biographies_wikipedia.json")

	spark = SparkSession.builder.getOrCreate()
	sc = spark.sparkContext

	df = spark.read.json(BIO_WIKI)

	print(df.printSchema())

	overview_df = df.rdd.map(lambda r: Row(r["id"], r["name"], r["wiki-title"], r["gender"], r["occupation"], get_overview(r["text"]))).toDF()
	overview_df = overview_df.toDF("id", "name", "wiki-title", "gender", "occupation", "overview")

	if local:
		print(overview_df.show())
		print(overview_df.count())
	else:
		print("="*50)
		print("Got enwiki overview")
		print(overview_df.count())
		print("="*50)

	# filter people without overview
	overview_df = overview_df.filter(col("overview") != '')

	if local:
		print(overview_df.count())
	else:
		print("="*50)
		print("Got overview filtered")
		print(overview_df.count())
		print("="*50)

	# save the df
	overview_df.write.mode('overwrite').json(os.path.join(LOCAL_PATH, "overview_wikipedia.json"))

	# woohoo!
	print("!!!!!!!!!!!!!!!")


if __name__ == '__main__':
	main(**vars(parse_arguments()))
