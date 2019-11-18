from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from params import *
import os
import json


def main(**params):
	params = dict(
		**params
	)

	local = params['local']

	if local:
		LOCAL_PATH = "../data/"
		WIKI_DATA = os.path.join(LOCAL_PATH, "wikipedia_sample.json")

	else: # running in the cluster
		LOCAL_PATH = "hdfs:///user/gullon/"
		WIKI_DATA = os.path.join(LOCAL_PATH, "overview_wikipedia.json")

	# create the session
	spark = SparkSession.builder.getOrCreate()
	# create the context
	sc = spark.sparkContext

	df = spark.read.json(WIKI_DATA)

	if local:
		df.show()

	df.printSchema()

	df = df.withColumn("occupation", explode(split(regexp_replace(regexp_replace(regexp_replace(regexp_replace(df['occupation'], '\\[', ''), '\\]', ''), ' ', ''),"'", ""), ",")))

	df = df.filter(col("occupation") != '')

	occupation_counts = df.groupBy("occupation").agg(count("*").alias("count")).sort(desc("count"))
	occupation_counts.show()

	occupation_counts.repartition(1).write.mode('overwrite').json(os.path.join(LOCAL_PATH, "occupation_counts.json"))

	# woohoo!
	print("!!!!!!!!!!!!!!!")


if __name__ == '__main__':
	main(**vars(parse_arguments()))