# Gender biases in Wikipedia
Study of linguistic biases in the overview of biographies about men and women in the English Wikipedia

# Abstract
Wikipedia has become one of the greatest sources of information in the last decade. Among its articles, more than one million correspond to biographies. Our aim is to analyze how men and women are portrayed and if there exist any biases in the language used to describe them.  

By using the biography articles in English Wikipedia, the goal of this study is to look for possible asymmetries in the way a person is described based on its gender. Moreover, we want to study the relation between these possible biases and the context in which this person is described: year, topic to which the person is related, culture, etc. with the idea of better understanding gender biases in our language.

To handle those categories which are outside of the binary classification, we will perform a first evaluation of the available data for completeness of the study.

# Research questions
- Is there an asymmetry in the way men and women are portrayed in the wikipedia?
- How subjective are the adjectives used to describe the people?
- Is there evidence of linguistic bias based on the gender of the person being described?

# Dataset
We will use the overview of biographies about men and women in the English Wikipedia. The overview (also known as lead section) is the first section of an article. According to Wikipedia, it should stand on its own as a concise overview of the article’s topic. Wikipedia editors need to focus on what they consider most important about the person, and biases are likely to play a role in this selection process.

The idea is to filter the articles of the wikipedia and keep only the biographies (we expect it will be approximately 25% of the whole wikipedia).


# A list of internal milestones up until project milestone 2
- Collect the dataset (select from the cluster the biographies, save them local) [DONE]
- Take overview from men and women and the basic information [DONE]
- Compute statistics about the biographies (by gender and occupation) [DONE]
- Explore the dataset and perform an initial NLP analysis [DONE]
- Explore possibilities for the data visualization [NEXT STEPS]
- Try to predict whether a given words (gender neutral) from a biography are from a man or a woman [DONE]

## Milestone 2 available [here](gender_biases_in_wikipedia.ipynb): 
`gender_biases_in_wikipedia.ipynb`

# Next steps
- Validate our results by checking the ratio of the most correlated adjective to a gender (like beautiful for women), to whole dataset (how many female biographies contain that adjective)
- Explore the source of the bias and the parameters that affect it
- Implement the data story
