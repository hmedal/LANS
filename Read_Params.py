import json


class Read_Params:
    Params = {}

    def __init__(self):
        with open('params.txt') as data_file:
            self.Params = json.load(data_file)
