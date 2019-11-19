import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re
from params import *


def main(**params):
	params = dict(
		**params
	)

	local = params['local']

	if local:
		LOCAL_PATH = "../../data/"
		WIKI_DATA = os.path.join(LOCAL_PATH, "wikipedia_sample.json")
	else: # running in the cluster
		LOCAL_PATH = "hdfs:///user/gullon/"
		WIKI_DATA = os.path.join(LOCAL_PATH, "overview_wikipedia.json")

	spark = SparkSession.builder.getOrCreate()
	sc = spark.sparkContext

	df = spark.read.json(WIKI_DATA)

	# eplode the gender column (create multiple entries for people with a list of genders)
	df = df.withColumn("gender", split(regexp_replace(regexp_replace(regexp_replace(regexp_replace(df['gender'], '\\[', ''), '\\]', ''), ' ', ''),"'", ""), ","))

	# filter people with only 1 gender
	df = df.filter(size(col("gender")) == 1)
	df = df.withColumn("gender", df['gender'][0])

	if local:
		df.select("gender").distinct().show()
	else:
		print("="*50)
		print("Got gender column exploded")
		df.select("gender").distinct().show()
		print("="*50)

	# filter and keep only people with female (Q6581072) and male (Q6581097) genders
	df_fem_male = df.filter((col("gender") == 'Q6581097') | (col("gender") == 'Q6581072'))

	if local:
		df_fem_male.select("gender").distinct().show()
		print(df_fem_male.count())
		print(df_fem_male.select("id").distinct().count())
	else:
		print("="*50)
		print("Got data filtered by gender")
		df_fem_male.select("gender").distinct().show()
		print(df_fem_male.count())
		print(df_fem_male.select("id").distinct().count())
		print("="*50)

	# save the df
	df_fem_male.write.mode('overwrite').json(os.path.join(LOCAL_PATH, "wikipedia_male_female.json"))

	# woohoo!
	print("!!!!!!!!!!!!!!!")


if __name__ == '__main__':
	main(**vars(parse_arguments()))