import pymongo
from pymongo import MongoClient

from mtools.util.logline import LogLine
from mtools.util.log2code import Log2CodeConverter

import json 

class LogImporter(object):
    """ Constructor initializes file and connects to mongo
        In one document there is: 
            - _id which is the line number of the logline
            - json representation of logline which has:
                - line string
                - split tokens
                - duration 
                - thread
                - operation
                - namespace
                - counters (nscanned, ntoreturn, nupdated, nreturned, ninserted)
            - corresponding log2code message
            - corresponding variable parts of log2code message
    """

    def __init__(self, logfile, host='localhost', port=27017, coll_name=None):

        self.logfile = logfile
        self.log2code = Log2CodeConverter()
        # open an instance of mongo
        self.client = MongoClient(host, port)
        self.db = self.client.logfiles
        # string name of collection if it's not already decided
        if coll_name == None:
            name = self._collection_name(logfile.name)
        else:
            name = coll_name

        # drop that collection name - each log file is a collection
        self.db.drop_collection(name)
        self.collection = self.db[name]
        self._mongo_import()
        print "logs imported"

    def _collection_name(self,logname):
        """ takes the ending part of the filename """
        # take out directory, and the .log part
        return logname.split('/')[-1].split('.')[0]
    
    def line_to_dict(self,line, index):
        """ converts a line to a dictionary that can be imported into Mongo
            the object id is the index of the line
        """
        # convert the json representation into a dictionary 
        logline_dict = LogLine(line).to_dict()
        # get the variable parts and the log2code output of the line
        codeline, variable = self.log2code(line, variable=True)

        if codeline:
            # add in the other keys to the dictionary
            logline_dict['log2code'] = {'pattern': codeline.pattern, 'variables': variable}
        # make the _id the index of the line in the logfile
        logline_dict['_id'] = index
        return logline_dict

    def _mongo_import(self):
        """ imports every line into mongo with the collection name 
            being the name of the logfile and each document being one logline
        """
        batch = []

        for index, line in enumerate(self.logfile):
            # add to batch
            batch.append(self.line_to_dict(line,index))
            if index % 10000 == 0:
                print "imported %i lines so far..." % index
                self.collection.insert(batch, w=0)
                batch = []

        # insert the remaining docs in the last batch
        self.collection.insert(batch, w=0)

