import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re
from params import *


def clear_overview(dirty_overview):
    # remove the <ref> </ref>
    overview = re.sub('<ref.*</ref>', '', dirty_overview)
    overview = re.sub('<ref.*/>', '', overview)

    # remove {{ }} and what is inside
    overview = re.sub('[\{].*[\}]', '', overview)
    
    # reomve [[File: ]] and [[Image: ]] and what is inside
    overview = re.sub('\[\[File:.*?\]\]', '', overview)
    overview = re.sub('\[\[Image:.*?\]\]', '', overview)

    # remove [[ ]] and keep what is inside and for the cases like [[abc | def]] keep only def and remove the rest
    overview = re.sub(r'\[\[(?:[^\]|]*\|)?([^\]|]*)\]\]', r'\1', overview)

    # remove ''' ''' 
    overview = re.sub('\'{2,3}', '', overview)

    # remove \n
    overview = re.sub('\n', '', overview)
    
    return overview


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

	overview = text[starting_idx:ending_idx]
	return clear_overview(overview)


def main(**params):
	params = dict(
		**params
	)

	local = params['local']

	if local:
		LOCAL_PATH = "../../data/"
		BIO_WIKI = os.path.join(LOCAL_PATH, "biographies_wikipedia.json")
	else: # running in the cluster
		LOCAL_PATH = "hdfs:///user/gullon/"
		BIO_WIKI = os.path.join(LOCAL_PATH, "biographies_wikipedia.json")

	spark = SparkSession.builder.getOrCreate()
	sc = spark.sparkContext

	df = spark.read.json(BIO_WIKI)

	print(df.printSchema())

	overview_df = df.rdd.map(lambda r: Row(r["id"], r["name"], r["wiki-title"], r["gender"], r["occupation"], clear_overview(get_overview(r["text"])))).toDF()
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
