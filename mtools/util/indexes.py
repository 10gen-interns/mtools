from pymongo import ASCENDING as asc
from pymongo import DESCENDING as desc

from itertools import permutations

class IndexBuilder(object):

    def __init__(self,combine_list=None, fields=None):
        if combine_list:
            self.combine_list = combine_list
        else:
            self.combine_list = ['operation', 'log2code', 'thread', 'namespace']
        if fields:
            self.fields= fields
        else:
            self.fields = {"line_no": ('line_no', asc),
                      "operation": ('operation', asc),
                      "log2code":('log2code.uid', asc),
                      "thread":('thread', asc),
                      "namespace": ('namespace', asc),
                      "duration":('duration', desc)}

        self.indexes = [[self.fields["line_no"]],
                        [self.fields["duration"]]
                        ]
        self.combine_indexes()


    def create_index(self,combination):
        ind = []
        for attribute in combination:
            ind.append(self.fields[attribute])
        ind.append(self.fields['duration'])
        return ind

    def combine_indexes(self,amt=2):
        perms= permutations(self.combine_list,amt)
        for combo in perms:
            self.indexes.append(self.create_index(combo))

    def __call__(self):
        return self.indexes

