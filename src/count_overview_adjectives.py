from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from params import *
import os
import json
import spacy


# nlp = spacy.load('en')
nlp = spacy.load('en_core_web_sm')


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
    # get data to perform nlp analysis
    doc = nlp(overview)
    # get lemma of the adjectives that are in the subjectivity lexicon
    adjs = [token.lemma_ for token in doc if is_adjective(token)]
    return adjs


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


    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # read the subjectivity lexicon to get the adjectives
    SUBJECTIVITY = os.path.join(LOCAL_PATH, "subjectivity_dictionary.json")
    subjectivity_lexicon = spark.read.json(SUBJECTIVITY)
    subjectivity_lexicon = subjectivity_lexicon.select("word").toDF("adjectives")
    
    if local:
        subjectivity_lexicon.show()

    df_fem = spark.read.json(WIKIPEDIA)

    if local:
        df_fem.show()
        df_fem = df_fem.sample(False, 0.003, seed=0)

    # get the adjectives
    udf_get_adj = udf(get_adjectives)

    df_with_adj = df_fem.withColumn("adjectives", udf_get_adj("overview"))

    if local:
        df_with_adj.select("id", "adjectives").show()
    else:
        print("="*50)
        print("Got adjectives!")
        print("="*50)

    df_with_adj = df_with_adj.withColumn("adjectives", explode(split(regexp_replace(regexp_replace\
        (regexp_replace(regexp_replace(df_with_adj['adjectives'], '\\[', ''), '\\]', ''), ' ', ''),"'", ""), ",")))

    df_with_adj = df_with_adj.filter(col("adjectives") != '')

    df_filtered_adj = subjectivity_lexicon.join(df_with_adj, ['adjectives'], how='inner')
    adjectives_count = df_filtered_adj.groupBy("adjectives").agg(count("*").alias("count")).sort(desc("count"))
    df_filtered_adj = df_filtered_adj.dropDuplicates()

    if local:
        df_filtered_adj.show()
        adjectives_count.show()
    else:
        print("="*50)
        print("Adjectives filtered!")
        print("="*50)

    df_adj_list = df_filtered_adj.groupBy("id").agg(collect_list(df_filtered_adj['adjectives']).alias("adjectives"))
    df_final = df_adj_list.join(df_fem, ['id'], how='inner')
    
    if local:
        df_final.show()
    else:
        print("="*50)
        print("Got final dataframe!")
        print("="*50)

    # save the df
    df_final.write.mode('overwrite').json(os.path.join(LOCAL_PATH, "wikipedia_" + GENDER + "_adjectives.json"))
    adjectives_count.repartition(1).write.mode('overwrite').json(os.path.join(LOCAL_PATH, "count_" + GENDER + "_adjectives.json"))

    # woohoo!
    print("!!!!!!!!!!!!!!!")


if __name__ == '__main__':
    main(**vars(parse_arguments()))