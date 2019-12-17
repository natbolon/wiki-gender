import json

# used to read gender and occupations dictionaries
def read_dictionary(path):
    with open(path) as json_file:
        line = json_file.readline()
        dictionary = json.loads(line)
    return dictionary
    
# read occupations dictionary
def read_dict_occupations(path = '../data/dict_occupations.json'):
    dict_occupations = {}
    with open(path) as json_file:
        content = json_file.readlines()
        for line in content:
            occ = json.loads(line)
            dict_occupations.update(occ)
    return dict_occupations
        
# read subjectivity lexicon
def read_dict_subjectivity(path):
    subjectivity_dictionary = {}
    with open(path, 'r') as json_file:
        for item in eval(json_file.readline()):
            subjectivity_dictionary.update({item['word']: (item['strength'], item['subj'])})

    return subjectivity_dictionary