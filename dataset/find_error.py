import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
import sys


def only_enwiki(line): 
	d = json.loads(line._VALUE)
	if 'enwiki' in d['sitelinks']: 
		return True
	else:
		return False


def main():
	FILE = "../data/error_sample.json"

	spark = SparkSession.builder.getOrCreate()
	sc = spark.sparkContext

	df = spark.read.json(FILE)

	people_filtered = df.select("_VALUE").rdd.filter(only_enwiki)

	people_df = spark.read.json(people_filtered.map(lambda r: r["_VALUE"]))

	people_df.show()

	people_df = people_df.select("labels.en.value", "sitelinks.enwiki.title", "claims.P21.mainsnak.datavalue.value.id").toDF("label", "title", "gender")

	people_df.show()

	# save the df
	# people_df.write.mode('overwrite').json(os.path.join(LOCAL_PATH, "people_wikidata.json"))

	# woohoo!
	print("!!!!!!!!!!!!!!!")



if __name__ == '__main__':
	main()
