import logging

class Mylog():
    def __init__(self):
        logging.getLogger().setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s :%(levelname)s:%(name)s [%(thread)d]:  %(message)s')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(formatter)
        logging.getLogger().addHandler(console)
