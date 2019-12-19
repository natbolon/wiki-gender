from pyspark.sql.functions import *
from sklearn.metrics import confusion_matrix, roc_auc_score

from empath import Empath




def most_n_common(df, num=100):
    """
    get first N most common adjectives/nouns and return 
    """
    first_100 = df.orderBy(desc("count")).limit(num).toPandas()
    return first_100

def filter_occupation(data_df, occupation_field):
    return data_df[data_df['field'] == occupation_field]

def vocabulary_common_adj(most_common_adj_fem, most_common_adj_male, num=100):
    # get most common adjectives for male and female
    first_100_adj_male = most_n_common(most_common_adj_male, num)
    first_100_adj_fem = most_n_common(most_common_adj_fem, num)

    # create vocabulary for the model with the set of adjectives previous found
    most_common_adj = set()
    most_common_adj.update(first_100_adj_male['adjectives'].tolist())
    most_common_adj.update(first_100_adj_fem['adjectives'].tolist())
    most_common_adj = list(most_common_adj)
    
    return most_common_adj

def most_common_adj_field(df_pd, num=100):
    
    df_pd = df_pd.explode('adjectives')
    df_pd = df_pd[['id', 'adjectives']]
    df_pd = df_pd.drop_duplicates()
    first_100_adj = df_pd.groupby(['adjectives'], as_index=False).count().rename(columns={'id':'count'}).\
                            sort_values(by='count', ascending=False).iloc[:num]
    return first_100_adj

def vocabulary_common_adj_field(most_common_adj_fem, most_common_adj_male, num=100):
    # get most common adjectives for male and female
    first_100_adj_male = most_common_adj_field(most_common_adj_male, num)
    first_100_adj_fem = most_common_adj_field(most_common_adj_fem, num)

    # create vocabulary for the model with the set of adjectives previous found
    most_common_adj = set()
    most_common_adj.update(first_100_adj_male['adjectives'].tolist())
    most_common_adj.update(first_100_adj_fem['adjectives'].tolist())
    most_common_adj = list(most_common_adj)
    
    return most_common_adj


# import importlib
# import foo #import the module here, so that it can be reloaded.
# importlib.reload(foo)

def print_confusion_matrix(y_test, y_pred):
	# confusion matrix (true - rows, pred - cols)
	cm = confusion_matrix(y_test, y_pred)

	print('Confusion Matrix:\n')
	print('T\P \t male \t female')
	print('male \t', cm[0,0], '\t', cm[0,1])
	print('female \t', cm[1,0], '\t', cm[1,1])
	print("\nT - True, P - Predicted")

def print_accuracy_model(lr, X_test, y_test):
	# accuracy
	print("\n\nAccuracy of the model in test dataset: {:.3f}".format(lr.score(X_test, y_test)))
	print("AUC ROC of the model in test dataset: {:.3f}".format(roc_auc_score(y_test, lr.predict_proba(X_test)[:,1])))


def analyze_tokens(word_list, topk = 10):
	lexicon = Empath()

	word_list_analyzed = lexicon.analyze(word_list, normalize=True)

	return sorted(word_list_analyzed.items(), key=lambda kv: kv[1], reverse = True)[:topk]


