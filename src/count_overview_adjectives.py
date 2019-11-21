import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re
from params import *
import json

import nltk
import spacy
import string
from nltk.corpus import stopwords
from collections import Counter
from pyspark.ml.feature import StopWordsRemover, Tokenizer, RegexTokenizer, CountVectorizer


nlp = spacy.load('en')


def is_adjective(token):
    if not token.is_stop:
        if token.pos_ == 'ADJ':
            return True
        else:
            return False
    return False

def get_adjectives(overview):
    
    doc = nlp(overview)
    adjs = [token.lemma_ for token in doc if is_adjective(token)]
    return adjs


def filter_adjectives(adjList, subjectivity_lexicon):
    filtered_adj = []
    
    for adj in adjList:
        if adj in subjectivity_lexicon:
            filtered_adj.append(adj)
    
    return filtered_adj


def main(**params):
    params = dict(
        **params
    )

    local = 'local'

    if local:
        LOCAL_PATH = "../data/"
        WIKI_DATA = os.path.join(LOCAL_PATH, "wikipedia_female_sample.json")
    else: # running in the cluster
        LOCAL_PATH = "hdfs:///user/gullon/"
        WIKI_DATA = os.path.join(LOCAL_PATH, "wikipedia_female_sample.json")

    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # read the subjectivity lexicon to get the adjectives
    with open('../data/subjectivity_dictionary.json') as json_file:
        subjectivity_dictionary = json.load(json_file)

    df = spark.read.json(WIKI_DATA)
    print(df.printSchema())
    print(df.show())

    ## filter with pyspark functions
    # convert message from string to array of words
    regexTokenizer = RegexTokenizer(inputCol="overview", outputCol="overviewArray", pattern="\W")

    # filter stop words 
    remover = StopWordsRemover(inputCol="overviewArray", outputCol="overviewArrayFiltered")

    # apply transformations on messages DF keeping only the information we are interested in
    filteredBody = remover.transform(regexTokenizer.transform(df.select('name', 'overview')))
    filteredBody.show()

    
    filteredBody = filteredBody.withColumn("overviewFiltered", 
            concat_ws(',', filteredBody.overviewArrayFiltered).alias('overviewFiltered'))
    filteredBody.show()



    # get the adjectives
    udf_myFunction = udf(get_adjectives) # if the function returns an int
    df_with_adj = filteredBody.withColumn("adjectives", udf_myFunction("overviewFiltered")) #"_3" being the column name of the column you want to consider
    df_with_adj.show()

    exit()

    # df_with_adj = df.withColumn("adjectives", get_adjectives(df.overview))
    # df_with_adj.show()

    df.withColumn('word', explode(split(col('overview'), ' ')))\
        .groupBy('word')\
        .count()\
        .sort('count', ascending=False)\
        .show()

    

    # woohoo!
    print("!!!!!!!!!!!!!!!")


if __name__ == '__main__':
    main(**vars(parse_arguments()))