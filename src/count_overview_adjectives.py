from __future__ import unicode_literals
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from params import *
import os
import json
import spacy
from pyspark.sql.types import StringType
import sys



def is_noun(token):
    # check the token is not a stop word
    if not token.is_stop:
        # check the token is a noun (common noun)
        return token.pos_ == 'NOUN'


def is_verb(token):
    # check the token is not a stop word
    if not token.is_stop:
        # check the token is a noun (common noun)
        return token.pos_ == 'VERB'


def is_adjective(token):
    # check the token is not a stop word
    if not token.is_stop:
        # check the token is an adjective
        return token.pos_ == 'ADJ'


def get_adjectives(overview):
    global nlp
    if not nlp:
        nlp = spacy.load('en_core_web_sm')
    # get data to perform nlp analysis
    doc = nlp(overview)
    # get lemma of the adjectives that are in the subjectivity lexicon
    adjs = [token.text for token in doc if is_adjective(token)]
    return adjs

def get_nouns(overview):
    global nlp
    if not nlp:
        nlp = spacy.load('en_core_web_sm')
    # get data to perform nlp analysis
    doc = nlp(overview)
    # get lemma of the adjectives that are in the subjectivity lexicon
    nouns = [token.lemma_ for token in doc if is_noun(token)]
    return nouns


def main(**params):
    params = dict(
        **params
    )

    local = params['local']
    female = params['female']
    male = params['male']

    assert female ^ male, "you have to specify either female --f OR male --m"

    if female:
        GENDER = "female"
    if male:
        GENDER = "male"

    if local:
        LOCAL_PATH = "../data/"
        WIKIPEDIA = os.path.join(LOCAL_PATH, "wikipedia_"+ GENDER +"_sample.json")
    else: # running in the cluster
        LOCAL_PATH = "hdfs:///user/gullon/"
        WIKIPEDIA = os.path.join(LOCAL_PATH, "wikipedia_"+ GENDER +".json")

    global nlp
    nlp = None

    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # read the subjectivity lexicon to get the adjectives
    SUBJECTIVITY = os.path.join(LOCAL_PATH, "subjectivity_dictionary.json")
    subjectivity_lexicon = spark.read.json(SUBJECTIVITY)
    subjectivity_lexicon = subjectivity_lexicon.select("word").toDF("adjectives")
    
    if local:
        subjectivity_lexicon.show()

    df = spark.read.json(WIKIPEDIA)

    if local:
        df.show()
        df = df.sample(False, 0.003, seed=0)

    COMMON_GENDER = os.path.join(LOCAL_PATH, "dict_common_gender_nouns.json")

    common_gender_nouns_txt = sc.textFile(COMMON_GENDER)
    common_gender_nouns = eval(common_gender_nouns_txt.collect()[0])

    # get the adjectives
    udf_get_adj = udf(get_adjectives, StringType())
    # get the nouns
    udf_get_noun = udf(get_nouns, StringType())

    df_with_adj = df.withColumn("adjectives", udf_get_adj("overview"))
    df_with_noun = df.withColumn("nouns", udf_get_noun("overview"))

    if local:
        df_with_adj.select("id", "adjectives").show()
        df_with_noun.select("id", "nouns").show()
    else:
        print("="*50)
        print("Got adjectives and nouns!")
        print("="*50)

    df_with_adj = df_with_adj.withColumn("adjectives", explode(split(regexp_replace(regexp_replace\
        (regexp_replace(regexp_replace(df_with_adj['adjectives'], '\\[', ''), '\\]', ''), ' ', ''),"'", ""), ",")))
    df_with_noun = df_with_noun.withColumn("nouns", explode(split(regexp_replace(regexp_replace(regexp_replace\
                    (regexp_replace(df_with_noun['nouns'], '\\[', ''), '\\]', ''), ' ', ''),"'", ""), ",")))

    df_with_adj = df_with_adj.filter(col("adjectives") != '')
    df_with_noun = df_with_noun.filter(col("nouns") != '')

    df_filtered_adj = subjectivity_lexicon.join(df_with_adj, ['adjectives'], how='inner')
    df_filtered_adj = df_filtered_adj.dropDuplicates()
    adjectives_count = df_filtered_adj.groupBy("adjectives").agg(count("*").alias("count")).sort(desc("count"))

    if local:
        df_filtered_adj.show()
        adjectives_count.show()
    else:
        print("="*50)
        print("Adjectives filtered!")
        print("="*50)

    def get_common_gender_noun(noun):
        return common_gender_nouns.get(noun, noun)

    # remove gender from nouns
    udf_gender_nouns = udf(get_common_gender_noun, StringType())
    df_filtered_noun = df_with_noun.withColumn("nouns", udf_gender_nouns("nouns"))
    df_filtered_noun = df_filtered_noun.dropDuplicates()
    nouns_count = df_filtered_noun.groupBy("nouns").agg(count("*").alias("count")).sort(desc("count"))

    if local:
        df_filtered_noun.show()
        nouns_count.show()
    else:
        print("="*50)
        print("Nouns filtered!")
        print("="*50)

    df_adj_list = df_filtered_adj.groupBy("id").agg(collect_list(df_filtered_adj['adjectives']).alias("adjectives"))
    df_noun_list = df_filtered_noun.groupBy("id").agg(collect_list(df_filtered_noun['nouns']).alias("nouns"))

    df_semifinal = df_adj_list.join(df_noun_list, ['id'], how='inner')
    df_final = df_semifinal.join(df, ['id'], how='inner')
    
    if local:
        df_final.show()
    else:
        print("="*50)
        print("Got final dataframe!")
        print("="*50)

    # save the df
    df_final.write.mode('overwrite').json(os.path.join(LOCAL_PATH, "wikipedia_" + GENDER + "_nouns_adjectives.json"))
    adjectives_count.write.mode('overwrite').json(os.path.join(LOCAL_PATH, "count_" + GENDER + "_adjectives.json"))
    nouns_count.write.mode('overwrite').json(os.path.join(LOCAL_PATH, "count_" + GENDER + "_nouns.json"))

    # woohoo!
    print("!!!!!!!!!!!!!!!")


if __name__ == '__main__':
    main(**vars(parse_arguments()))