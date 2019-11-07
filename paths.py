import os


def check_dir(dirname):
    if not os.path.exists(dirname):
        os.mkdir(dirname)
    return dirname


DIR_PATH = os.path.dirname(os.path.realpath(__file__))

DATA_PATH = os.path.join(DIR_PATH, 'data/')
