import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

from paths import *

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# absolute path of the file - CHANGE WHEN RUN LOCAL
file = os.path.join(DATA_PATH, "first_1000_lines_wikidata.xml")
df = spark.read.format("com.databricks.spark.xml") \
				.options(rowTag="page") \
				.load(file)

print(df.show())

# select only those that start with Q: it corresponds to a legal page
df_rev  = df.where(col('title').startswith("Q"))
print(df_rev.show())

# we are interested in the revision that has all the text
df_rev = df_rev.select("revision", "title")
print(df_rev.show())

# try to access from the schema the text (and then save it as a json file)
print(df_rev.printSchema())

# just to see the first line :p
print(df_rev.take(1))

# woohoo!
print("!!!!!!!!!!!!!!!")
