### Instructions

**IMPORTANT:** Run the files with argument **--local** when running them locally, e.g. `file.py --local`

1. First, we extract the Q-ids from the [WHQI](#http://whgi.wmflabs.org/) dataset, which is updated every week, and it contains all the Q-ids for all the people in wikidata that exists in the English Wikipedia. The data file can be downloaded from [here](#http://whgi.wmflabs.org/snapshot_data/2019-11-04/gender-index-data-2019-11-04.csv) or use the command:  `wget  http://whgi.wmflabs.org/snapshot_data/2019-11-04/gender-index-data-2019-11-04.csv`. To do the filtering, we have to run the file: **`1_extract_qid_wikidata.py`** and the output file containing the Q-ids will be called: `qid_people_wikidata.csv`.

2. Then, we use the Q-ids to filter the whole wikidata xml file that can be found in the cluster (`hdfs:///datasets/wikidatawiki/wikidatawiki-20170301-pages-articles-multistream.xml`). To do that, we run the file: **`2_extract_people_wikidata.py`**. This returns a file called `people_wikidata.json`, which contains all the attributes of people (e.g. label, title, gender, year, etc.).

   -------------------

3. Filter the English Wikipedia entries (xml file in the cluster - `hdfs:///datasets/enwiki-20191001/enwiki-20191001-pages-articles-multistream.xml`) by the title extracted in the previous step. We saved the attributes and the whole biography into a file called: `biographies_wikipedia.json`. To do that, we run the file: **`3_filter_people_enwiki.py`.**

4. Finally, we extract the overview of those entries and create the **final** dataset called: `overview_wikipedia.json`. To do that, we run the file **`4_extract_overview_enwiki.py`**
