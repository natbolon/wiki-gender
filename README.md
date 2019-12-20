# Gender biases in Wikipedia
A study of linguistic and topic biases in the overview section of biographies about men and women in the English Wikipedia.

# Abstract
Wikipedia has become one of the greatest sources of information over the last decade. Among its articles, more than one million correspond to biographies. In this project, we analyze the linguistics used to portray men and women on this platform, and whether there exists a bias in the language used to describe them. 

We used the overview section of the biography articles in English Wikipedia to train several machine learning models to detect language asymmetries in the way men and women are depicted. 

By analyzing different parts of speech like adjectives and nouns in the overview, the models were able to predict a biography's gender, revealing that gender biases do exist on Wikipedia. 

Furthermore, we present the models' results in an easy, non-technical data story. 

# Research questions
- How subjective are the adjectives used to describe people on English Wikipedia?
- Is there evidence of linguistic and topic bias based on the gender of the person being described?
- Is there an asymmetry in the way men and women are portrayed in the wikipedia? If it does:
 - Can we find a robust statistical way to measure it?
 - Is the bias present in all fields of occupation?

# Dataset
We will use the overview of biographies about men and women in the English Wikipedia. The overview (also known as lead section) is the first section of an article. According to Wikipedia, it should stand on its own as a concise overview of the article’s topic. Wikipedia editors write on this section what they consider most important about the person, so biases are likely to play a role in this selection process.

We filtered the articles of the wikipedia and keep only the biographies (we expect it will be approximately 25% of the whole wikipedia).

## Project's Notebook available [here](gender_biases_in_wikipedia.ipynb): 
The complete notebook can be found on our Repository's main page, it is called:
`gender_biases_in_wikipedia.ipynb`

# Project's Data Story available [here](https://wiki-gender.github.io/)
Take a look to the Data story we created for this project. It can be found in the following URL:
https://wiki-gender.github.io/


