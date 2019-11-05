Idea: We want find the metadata from the wikidata and then select them from the enwiki dataset

NOTE! we run this locally with spark!

to do this we need:
1. parse the wikidata.xml
2. get the info from the <revision> ... <text> thing and save it to json
3. from the json file select the person type, the gender and whatever useful we think we can find (by code)
  - we can also check here: https://github.com/emilebourban/ADA-Project/blob/master/wididata_parser.py
4. connect the wikidata to the enwiki
