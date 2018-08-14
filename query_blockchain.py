import argparse
import json
import subprocess
import binascii
import time
import os
import sys
import bisect
from multiprocessing import Pool

from Savoir import Savoir
rpcuser = 'multichainrpc'
filename = '/home/ubuntu/.multichain/logchain/multichain.conf'
rpcpasswd = dict(map(lambda y: y.split('='), \
                     filter(lambda x: x, \
                            open(filename).read().split('\n'))))['rpcpassword']
rpchost = 'localhost'
rpcport = '4920'
chainname = 'logchain'
api = Savoir(rpcuser,rpcpasswd,rpchost,rpcport,chainname)

#wrapping calls to the timestamp stream in an interator lets us
#easily binary search in them using python's bisect 
#TODO: there are probably unhandled error cases here if the stream
#      gets corrupted, and we assume that e.g. json.loads
#      will have a zero'th element
class TimestampIterator:
    def __init__(self, nodenum):
        global api
        self.nodenum = nodenum
        self.api = api
        self.numrecords = self.api.liststreams("node%dtimestamps" % self.nodenum)[0]['keys']
        self.recordnum = 0

    def __iter__(self):
        return self

    def __len__(self):
        return self.numrecords

    def __getitem__(self,i):
        return int(self.api.liststreamkeys("node%dtimestamps" % self.nodenum, '*', False, 1, i, True)[0]['key'])
            
    def next(self):
        if self.recordnum == self.numrecords:
            raise StopIteration
        else:
            self.recordnum += 1
            return int(self.api.liststreamkeys( "node%dtimestamps" % self.nodenum, '*', False, 1, self.recordnum-1, True)[0]['key'])


#===== binary search for timestmap ranges =====
def interval_ids(nodenum, starttimestamp, endtimestamp):

        numrecords = api.liststreams("node%dtimestamps" % nodenum)[0]['keys']

        lower_bound = bisect.bisect_left(TimestampIterator(nodenum), int(starttimestamp))
        upper_bound = bisect.bisect_right(TimestampIterator(nodenum), int(endtimestamp), lo=lower_bound)

        if upper_bound - lower_bound == 0:
            return set([])
        else:
            resulti = binascii.unhexlify(api.liststreamkeyitems("node%dtimestamps" % nodenum,\
                                                                "*", False, 1, lower_bound, True)[0]['data'])

            resultj = binascii.unhexlify(api.liststreamkeyitems("node%dtimestamps" % nodenum,\
                                                                "*", False, 1, upper_bound-1, True)[0]['data'])
            
            return set(map(lambda x: 'UID_%d_%d' % (nodenum,x), \
                           range(int(resulti.split('_')[2]),int(resultj.split('_')[2])+1)))

#==== main query logic ====

def loaditem(itemid):
    global api
    return json.loads(binascii.unhexlify(api.liststreamkeyitems("logdata", '%s' % itemid)[0]['data']))

def print_result(d):
        print('%s\t%d\t%d\t%d\t%d\t%s\t%s' % (d['Timestamp'], \
                                              d['Node'], \
                                              d['ID'], \
                                              d['Ref-ID'], \
                                              d['User'], \
                                              d['Activity'], \
                                              d['Resource']))


def check_constraints(filters,d):
        for (cnt,kv) in filters:
                k = kv.split('_')[0]
                v = reduce(lambda x,y: x+'_'+y, kv.split('_')[1:])
                try:
                        v = int(v)
                except:
                        pass
                if not d[k] == v:
                        return False
        return d

#clauses is a dictionary 
#timestamp range is a tuple or None
#sortby is a string or None
#descendingp is a boolean
def query(clauses, timestamp_range, sortby, descendingp):

    global api

    start_time = time.time()

    #if there's a timestamp constraint, get the keys, this should be logn
    timestamp_matches = set([])
    if timestamp_range:
        if 'Node' in clauses:
            timestamp_matches = timestamp_matches.union(interval_ids(int(clauses['Node']), \
                                                                     timestamp_range[0], \
                                                                     timestamp_range[1]))
        else:
            for node in [1,2,3,4]:
                timestamp_matches = timestamp_matches.union(interval_ids(node,\
                                                                         timestamp_range[0],\
                                                                         timestamp_range[1]))

    #if there's clauses, use liststreamkeys to get counts
    if clauses:
            match_cts = sorted(map(lambda (k,v): 
                                   (api.liststreamkeys(k, k+'_'+v)[0]['items'], k+'_'+v), \
                                   clauses.iteritems()))


    #CASES
    # no clauses, no timestamps -> return everything (no need to filter) 
    #   - liststreakeyitems * -> actual
    # no clauses, timestamps -> return timestamp set (no need to filter) 
    #   - timestamp_matches -> liststreamkeyitems id -> actual
    # clauses, no timestamps -> return smallest clause set (need to filter) 
    #   - smallest match_cts -> potential -> check_constraints
    # clauses, timestamps -> return smallest of clauses and timestamp sets (need to filter) 
    #   - smallest match_cts/timestamp -> potential -> check

    matches = set([])
    if not clauses:
            if timestamp_range:
                    matches = timestamp_matches
    else:
            if timestamp_range and (len(timestamp_matches) <= match_cts[0][0]):
                    matches = timestamp_matches
            else:
                    matches = set(map(lambda x: binascii.unhexlify(x['data']), \
                                      api.liststreamkeyitems( match_cts[0][1].split('_')[0], \
                                                              '%s' % match_cts[0][1], False, \
                                                              pow(2,30))))
                    if timestamp_range:
                            matches = matches.intersection(timestamp_matches)

    elapsed_time = time.time() - start_time
    print 'took %s seconds to identify %s potential records' % (elapsed_time, len(matches))

    start_time = time.time()


    if not clauses and not timestamprange:
            actual_results = map(lambda x: 
                                 json.loads(binascii.unhexlify(x['data'])), \
                                 api.liststreamkeyitems( "logdata", \
                                                         '*', False, pow(2,30)))
    else:
            p = Pool(5)
            potential_results = p.map(loaditem, matches)

            if not clauses:
                    actual_results = potential_results
            else:
                    actual_results = filter(lambda x: check_constraints(match_cts,x), \
                                            potential_results)


    if sortby:
            actual_results = sorted(actual_results, key=lambda x:x[sortby], \
                                    reverse=descendingp)
    map(print_result, actual_results)

    elapsed_time = time.time() - start_time

    print 'took %s seconds to return %s records' % (elapsed_time, len(actual_results))


#argparse based cmdline
#query_blockchain --clause colname=value --clause colname2=value2 
#--timestamplow value --timestamphigh --sortby Colname3 --reverse-sort
if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--clause', \
                        action='append', \
                        help='a constraint in the form of COLNAME=VALUE, e.g. Node=3, can appear multiple times and will be ANDed')
    parser.add_argument('--timestamplow', \
                        action='store', \
                        type=int, \
                        help='do not return entries with a timesamp lower that this value')
    parser.add_argument('--timestamphigh', \
                        action='store', \
                        type=int, \
                        help='do not return entries with a timestamp high than this value')
    parser.add_argument('--sortby', \
                        action='store', \
                        type=str, \
                        help='sort entries by the value of the column spcified by this argument')
    parser.add_argument('--descendingp', \
                        action='store_const', \
                        const=True, \
                        default=False, \
                        help='sort in descending order (default is ascending)')
    args = parser.parse_args()

    clauses = {}
    if args.clause:
        for clause in args.clause:
            (k,v) = clause.split('=')
            #TODO: check to make sure well formed
            clauses[k] = v

    timestamprange = None
    if args.timestamplow or args.timestamphigh:
        timestamprange = (0,sys.maxint)
        if args.timestamplow:
            timestamprange = (int(args.timestamplow), timestamprange[1])
        if args.timestamphigh:
            timestamprange = (timestamprange[0], int(args.timestamphigh))

    query(clauses, timestamprange, args.sortby, args.descendingp)
    
