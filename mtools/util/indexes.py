from pymongo import ASCENDING as asc
from pymongo import DESCENDING as desc

from itertools import permutations

class IndexBuilder(object):

    def __init__(self,combine_list=None, fields=None, index_list=None):
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

        self.create_distinct_indexes(index_list)

    def create_index(self,combination):
        ind = []
        for attribute in combination:
            ind.append(self.fields[attribute])
        return ind


    def permute_indexes(self,amt=2):
        """creates a permuation of all the indexes
            and adds it to the index list
        """
        perms= permutations(self.combine_list,amt)
        for combo in perms:
            self.indexes.append(self.create_index(combo))

    def create_distinct_indexes(self,index_list=None):
        """ given an index list, create the indexes and append them to all the indexes
        """
        if not index_list:
            index_list = [('thread','line_no'),('operation', 'line_no'),
                          ('log2code', 'duration'), ('log2code', 'line_no'),
                          ('namespace', 'duration'), ('namespace', 'line_no')
                          ]
        for index_tuple in index_list:
            self.indexes.append(self.create_index(index_tuple))

    def __call__(self):
        return self.indexes

i = IndexBuilder()
print i()
