import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re

from paths import *

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# absolute path of the file - CHANGE WHEN RUN LOCAL
file = os.path.join(DATA_PATH, "enwiki_5000_lines.xml")
df = spark.read.format("com.databricks.spark.xml") \
				.options(rowTag="page") \
				.load(file)

print(df.show())

print(df.printSchema())

df = df.filter("redirect._title is null")

title_revision = df.select("title", "revision.text._VALUE").toDF("title", "text")

#title_revision = title_revision.where(~col("text").startswith("#REDIRECT"))

print(title_revision.show())

starting_expr = re.compile("'''")
ending_expr = re.compile("==")

def get_overview(text):
	starting_idx = starting_expr.search(text).span()[0]
	ending_idx = ending_expr.search(text).span()[0]
	return text[starting_idx:ending_idx]

overview = title_revision.rdd.map(lambda r: Row(r["title"], get_overview(r["text"]))).toDF()

overview = overview.toDF("title", "overview")

print(overview.show())

# woohoo!
print("!!!!!!!!!!!!!!!")
