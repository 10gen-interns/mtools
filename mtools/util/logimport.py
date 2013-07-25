from pymongo import MongoClient

from mtools.util.logline import LogLine
from mtools.util.log2code import Log2CodeConverter

import os

from indexes import IndexBuilder


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

    def __init__(self, logfile, host='localhost', port=27017, coll_name=None, drop=False, log_import=True, index=True):

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

        # raise an error if the collection name will be overwritten - must be in logimport stage
        if name in self.db.collection_names() and log_import:
            if drop:
                print "dropped collection"
                self.db.drop_collection(name)
            else:
                raise self.CollectionError(name)

        self.collection = self.db[name]
        # log2code database
        if log_import:
            self.log2code_db = self.client.log2code
            self._mongo_import()

        if index:
            self.indexes = IndexBuilder()
            print "logs imported..starting indexing"
            #make this into a single file
            self._ensure_indices()
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

    def _log2code_dict(self, uid, pattern, variables):
        return {'uid': uid,
                'pattern': pattern,
                'variables': variables}

    def _add_connection(self, logline_dict):
        """ add the connection to the threads
        """
        # get the logline variable part
        conn_num = logline_dict['log2code']['variables'][2]
        logline_dict['thread'].append("conn" + conn_num)



    def line_to_dict(self, line, index):
        """ converts a line to a dictionary that can be imported into Mongo
            the object id is the index of the line
        """
        # convert the json representation into a dictionary 
        logline_dict = LogLine(line).to_dict()
        
        del logline_dict['split_tokens']
        del logline_dict['line_str']

        try:
            thread = logline_dict['thread']
            logline_dict['thread']= [thread]
        except:
            pass

        # get the variable parts and the log2code output of the line
        codeline, variable = self.log2code(line, variable=True)

        if codeline:
            # add in the other keys to the dictionary
            log_dict = self._getLog2code(codeline)
            if not log_dict:
                # it's not in mongo, therefore doesn't have a uid
                logline_dict['log2code'] = self._log2code_dict(-1,
                                                               codeline.pattern, variable)

            else:
                logline_dict['log2code'] = self._log2code_dict(log_dict['_id'],
                                                               log_dict['pattern'],
                                                               variable)

        else:
            # there is not codeline, so there is a uid of -1 and pattern is none
            logline_dict['log2code'] = self._log2code_dict(-1, None, variable)

        if logline_dict['log2code']['uid'] == 891:
            self._add_connection(logline_dict)

        # make the _id the line of the logfile (unique)
        logline_dict['line_no'] = index
        return logline_dict

    def _ensure_indices(self):
        """ indexes the logfile based on the indexes in created in the index builder
        """
        for i in self.indexes():
            print "creating index on" + str(i)
            self.collection.ensure_index(i)



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

