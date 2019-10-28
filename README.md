# wiki-gender
Project for Applied Data Analysis 2019: Study of linguistic biases in the overview of biographies about men and women in the English Wikipedia

# Title ## TODO: find a nice title

# Abstract
## TODO:
Wikipedia has become one of the greatest sources of information in the last decade. Among its articles, more than one million correspond to biographies. Our aim is to analyze how men and women are portrayed and if there exist any biases in the language used to describe them.  

By using the biography articles in English Wikipedia, the goal of this study is to look for possible asymmetries in the way a person is described based on its gender. Moreover, we want to study the relation between these possible biases and the context in which this person is described: year, topic to which the person is related, culture, etc. with the idea of better understanding gender biases in our language.

# Research questions
## TODO: add more questions
- Is there an asymmetry in the way men and women are portrayed in the wikipedia?
- Have we changed along time the choice of words use to describe people?
- Are there sectors/topics more likely to introduce language biases than others?
- How subjective are the adjectives used to describe the people?
- Are there any countries where there are more linguistic biases than others?


# Dataset
## TODO: complete information: size, local/cluster/, etc.
We will use the overview of biographies about men and women in the English Wikipedia. The overview (also known as lead section) is the first section of an article. According to Wikipedia, it should stand on its own as a concise overview of the article’s topic. Wikipedia editors need to focus on what they consider most important about the person, and biases are likely to play a role in this selection process.

The idea is to filter the articles of the wikipedia and keep only the biographies (we expect it will be approximately 25% of the whole wikipedia).

https://github.com/DavidGrangier/wikipedia-biography-dataset extra dataset for nlp analysis on biographies - already parsed (if needed)


# A list of internal milestones up until project milestone 2
**Week 1 (4th November):**

- Collect the dataset (select from the cluster the biographies, save them local)
- Take overview from men and women and the basic information
- Set up initial GitHub structure

**Week 2 (11th November):**

- Compute statistics about the biographies (by gender, year and country)

**Week 3 (18th November):**

- Explore the dataset and perform an initial NLP analysis
- Agree on the metrics

**Week 4 (25th November):**

- Try out different tools and metric
- Try to predict whether a given words (gender neutral) from a biography are from a man or a woman.


# Questions for TAs
* How can we access and use the wikipedia dataset in the cluster?
