import pymongo
from pymongo import MongoClient

from mtools.util.logline import LogLine
from mtools.util.log2code import Log2CodeConverter

import os

import json 

class LogImporter(object):
    """ Constructor initializes file and connects to mongo
        Document looks like this:
        {   _id: num
            thread: ""
            namespace: ""
            duration: num
            log2code:
                {   uid: num
                    pattern: [""]
                    variables: [""]
                }
            counters: nums
            line_str: ""
        }
    """
    class CollectionError(Exception):

        def __init__(self, value):
            self.value= value
        def __str__(self):
            return "Collection"  + repr(self.value) + "already exist"

    def __init__(self, logfile, host='localhost', port=27017, coll_name=None, drop=False):

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

        # raise an error if the collection name will be overwritten
        if name in self.db.collection_names():
            if drop:
                print "dropped collection"
                self.db.drop_collection(name)
            else:
                raise self.CollectionError(name)

        self.collection = self.db[name]
        # log2code database
        self.log2code_db = self.client.log2code
        self._mongo_import()
        print "logs imported"

    def _collection_name(self, logname):
        """ takes the ending part of the filename """
        basename = os.path.basename(logname)
        return os.path.splitext(basename)[0]

    def _getLog2code(self, codeline):
        # get the pattern
        pattern = codeline.pattern

        log_dict =  self.log2code_db["instances"].find_one({'pattern': pattern})
        # this will return None if there is no log_dict (this only happens in
        # two cases)
        return log_dict

    def line_to_dict(self, line, index):
        """ converts a line to a dictionary that can be imported into Mongo
            the object id is the index of the line
        """
        # convert the json representation into a dictionary 
        logline_dict = LogLine(line).to_dict()
        
        del logline_dict['split_tokens']
        # get the variable parts and the log2code output of the line
        codeline, variable = self.log2code(line, variable=True)

        if codeline:
            # add in the other keys to the dictionary
            log_dict = self._getLog2code(codeline)
            if not log_dict:
                # it's not in mongo, therefore doesn't have a uid
                logline_dict['log2code'] = {'uid': -1,
                                           'pattern': codeline.pattern,
                                           'variables:': variable}
            else:
                logline_dict['log2code'] = {'uid': log_dict['_id'],
                                            'pattern': log_dict['pattern'],
                                            'variables': variable
                                            }
        else:
            # there is not codeline, so there is a uid of -1 and pattern is none
            logline_dict['log2code'] = {'pattern': None,
                                        'variables': variable,
                                        'uid': -1}
        # make the _id the line of the logfile (unique)
        logline_dict['_id'] = index
        return logline_dict

    def _mongo_import(self):
        """ imports every line into mongo with the collection name 
            being the name of the logfile and each document being one logline
        """
        batch = []

        for index, line in enumerate(self.logfile):
            # add to batch
            line = unicode(line, errors='ignore')
            batch.append(self.line_to_dict(line,index))
            if index % 10000 == 0:
                print "imported %i lines so far..." % index
                self.collection.insert(batch, w=0)
                batch = []

        # insert the remaining docs in the last batch
        self.collection.insert(batch, w=0)

