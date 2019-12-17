import matplotlib.pyplot as plt
import matplotlib
from wordcloud import WordCloud, STOPWORDS
from PIL import Image
import numpy as np

def word_to_color(subjectivity_dictionary):
    word_to_color_dict = dict()

    for word in subjectivity_dictionary:
        if subjectivity_dictionary[word][1] == "positive":
            word_to_color_dict[word] = '#377eb8' # blue
        if subjectivity_dictionary[word][1] == "negative":
            word_to_color_dict[word] = '#ff7f00' # orange
        if subjectivity_dictionary[word][1] == "neutral":
            word_to_color_dict[word] = '#999999' # grey
    return word_to_color_dict


def generate_wordcloud(mask_image, adj_dict, word_to_color_dict):

    def color_func(word, *args, **kwarg):

        try:
            color = word_to_color_dict[word]
        except KeyError:
            color = '#000000' # black
        return color


    mask_ = np.array(Image.open(mask_image))
    wc = WordCloud(background_color="white", max_words=500, mask=mask_, 
               contour_width=3, contour_color='peru', color_func=color_func)
    # generate word cloud
    wc.generate_from_frequencies(adj_dict[0])
    
    return wc


def viz_wordcloud(wc_male, wc_fem):
    fig,ax = plt.subplots(1,2, figsize=(20,20))
    ax[0].imshow(wc_male, cmap=plt.cm.gray, interpolation="bilinear")
    ax[0].axis("off")
    ax[1].imshow(wc_fem, cmap=plt.cm.gray, interpolation="bilinear")
    ax[1].axis("off")
    plt.show()


############
# import importlib
# import word_cloud
# importlib.reload(word_cloud)

# word_to_color_dict = word_to_color(subjectivity_dictionary)

# wc_male   = generate_wordcloud("../data/male.png", adj_male_dict, word_to_color_dict)