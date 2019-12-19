from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import udf

import numpy as np

# Create function to translate a code into a category
def translate(mapping):
    def translate_(col):
        return mapping.get(col, "other")
    return udf(translate_, StringType())

# get total number of adjectives per overview
def get_nb_adjs(list_adj):
    return len(list_adj)

# compute statistics
def stats_nb_adj(df, gender):
    count_adj = df.agg(mean(col("nb-adjs")), stddev(col("nb-adjs"))).collect()
    print('{}\t| Average num of adjectives: {:.2f} | Std of the num of adjectives: {:.2f}'.\
          format(gender, count_adj[0][0], count_adj[0][1]) )

# function to get the name of the tables in registerTempTanble
def spark_sql(df, df_name, query, spark):
	df.registerTempTable(df_name)

	result = spark.sql(query)
	return result

# function to get the name of the tables in registerTempTanble
def spark_sql_pd(df, df_name, query, spark):
	return spark_sql(df, df_name, query, spark).toPandas()

# Compute length (number of words) of an overview
tokens_len = udf(lambda s: len(s), IntegerType())

# creates a dataframe with the ratio on the adjectives for an overview
def ratio_adj_overview(df_adj_stats, spark):

	# Query to convert a spark data frame into a pandas data frame, the data frame contains the variables id,
	# overview_len, adjective_len, adjective_ratio_overview
	query = """
	SELECT DISTINCT id, overview_len, adjective_len, adjective_ratio_overview 
	FROM df_adj_stats
	WHERE overview_len>0
	ORDER BY adjective_ratio_overview
	"""

	return spark_sql_pd(df_adj_stats, "df_adj_stats", query, spark)

def adj_stats_df(df_nlp):
	# Remove punctuation and compute overview's length, adjective's length, and the percentage of adjectives per overview
	df_adj_stats = df_nlp.withColumn('overview', regexp_replace(regexp_replace(df_nlp['overview'],\
	                        r'[^\w\s]',''), '\s\s+', ' '))
	df_adj_stats = df_adj_stats.withColumn('overview_len', tokens_len(df_adj_stats['overview']))\
	               .withColumn('adjective_len', tokens_len(df_adj_stats['adjectives']))
	df_adj_stats = df_adj_stats.withColumn('adjective_ratio_overview',\
	                        df_adj_stats['adjective_len']/df_adj_stats['overview_len'])

	return df_adj_stats



def subjectivity_strength(df):
    """
    compute number of adjectives per strength and subjectivity used in the overviews
    """
    # count adj per strength and subjectivity
    overview_subjectivity = df.groupBy('strength', 'subjectivity').\
    agg(sum('count').alias('sum_')).orderBy(desc('sum_'))
    
    overview_subjectivity = overview_subjectivity.replace('', 'None')
    
    # compute percentage of adj in the strongly subjective category
    strong_adj = df.where((col("strength") == "strongsubj")).\
    groupBy('strength', 'subjectivity').agg({'count':'sum'}).\
    where((col("subjectivity") == "positive") | (col("subjectivity") == "negative"))
    
    strong_adj = strong_adj.\
    withColumn("percentage", 100*strong_adj['sum(count)']/ df.agg({'count':'sum'}).\
               collect()[0][0])

    return overview_subjectivity, strong_adj