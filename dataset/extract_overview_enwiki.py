import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re
from params import *


def get_overview(text, starting_expr, ending_expr):
	starting_idx = starting_expr.search(text).span()[0]
	ending_idx = ending_expr.search(text).span()[0]
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

	starting_expr = re.compile("'''")
	ending_expr = re.compile("==")

	overview_df = df.rdd.map(lambda r: Row(r["title"], r['label'], r['gender'], get_overview(r["text"], starting_expr, ending_expr))).toDF()
	overview_df = overview_df.toDF("title", "label", "gender", "overview")

	if local:
		print(overview_df.show())
	else:
		print("="*50)
		print("Got enwiki overview")
		print("="*50)

	# save the df
	overview_df.write.mode('overwrite').json(os.path.join(LOCAL_PATH, "overview_wikipedia.json"))

	# woohoo!
	print("!!!!!!!!!!!!!!!")


if __name__ == '__main__':
	main(**vars(parse_arguments()))
