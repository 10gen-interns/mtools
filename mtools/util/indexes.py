from pymongo import ASCENDING as asc
from pymongo import DESCENDING as desc
indexes = [ [('line_no', asc)],
            [('operation',asc)],
            [('duration', desc)],
            [('thread', asc)],
            ['log2code.uid',asc]
]