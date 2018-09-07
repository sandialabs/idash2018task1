#TODO: thread to balance optimize disk i/o vs network, try to saturate
#TODO: look into the rawtransaction way of doing multiple things at once
#TODO: look into optimizing/issuing network directly, instead of through RPC
#import subprocess
import binascii
import math
import json
import time
import random
import argparse
import os
import sys

from Savoir import Savoir

def insert(api,trps):
 start_time = time.time()
 delta = 1.0/float(trps)
 target_time = None
 behindct = ontimect = 0
 cmd_times = []
 addr = api.listaddresses()[0]['address']
 #addr = json.loads(subprocess.check_output(["multichain-cli",\
 #                                          sys.argv[1],"listaddresses"],\
 #                                         stderr=devnull))[0]['address']	
 numinsertions = 0
 d = {}
 for line in sys.stdin.readlines():
	(d['timestamp'],d['node'],d['id'],d['ref-id'],d['user'],\
         d['activity'],d['resource']) = line[:-1].split('\t')
	for int_key in ['node', 'id', 'timestamp', 'user', 'ref-id']:
		d[int_key] = int(d[int_key])
        sends = []

        sends.append({'for':'logdata', 'key':'UID_' + str(d['node']) + \
                      '_' + str(d['id']), 'data': binascii.hexlify(json.dumps(d))})
	numinsertions += 1

	for (k,v) in d.iteritems():

                sends.append({'for':str(k), 'key':str(k)+'_'+str(v), \
                              'data': binascii.hexlify('UID_' + str(d['node']) + \
                                                       '_' + str(d['id']))})
		if k == 'timestamp':
                        sends.append({'for':"node%dtimestamps" % int(d['node']), \
                                      'key':str(v), \
                                      'data':binascii.hexlify('UID_' \
                                                              + str(d['node'])\
                                                              + '_' + str(d['id']))})
        #cmd = ['multichain-cli', sys.argv[1], 'createrawsendfrom', \
        #       '%s' % addr, "{}", "%s" % json.dumps(sends), 'send']

	if target_time == None:
		target_time = time.time()
	if time.time() >= target_time:
		behindct += 1
		print 'fallen behind :-( (%d+%d=%d)' % (behindct,ontimect,behindct+ontimect)
	else:
		ontimect += 1
		print 'caught back up :-) (%d+%d=%d)' % (behindct,ontimect,behindct+ontimect)
	while time.time() < target_time:
		pass
	cmd_start = time.time()
	#subprocess.check_output(cmd,stderr=devnull)
	api.createrawsendfrom('%s' % addr, {}, sends, 'send')
	cmd_time = time.time()-cmd_start
	cmd_times.append(cmd_time)
	target_time += delta

 elapsed_time = time.time() - start_time

 insertionstring = 'insertion took %s seconds, %s secs/record (%s records)' % \
			(elapsed_time, elapsed_time/numinsertions, numinsertions)

 print insertionstring
 histogram = map(lambda x:0, range(40))
 bigger = 0
 maxval = 0
 for sample in cmd_times:
	if sample > maxval:
		maxval = sample
	bucket = int(math.floor(sample/.1))
	if bucket > 39:
		bigger += 1
	else:
		histogram[bucket] += 1
 print histogram
 print bigger
 print maxval

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rpcuser', \
                        action='store', \
                        help='multichain rpc user')
    parser.add_argument('--rpcport', \
                        action='store', \
                        help='multichain rpc port')
    parser.add_argument('--rpcpasswd', \
                        action='store', \
                        help='multichain rpc password')
    parser.add_argument('--chainname', \
                        action='store', \
                        help='multichain chain name')
    parser.add_argument('--targetrecordspersecond', \
                        action='store', \
                        type=int, \
                        help='attempt to insert at this rate')

    args = parser.parse_args()
    if args.rpcuser:
        rpcuser = args.rpcuser
    else:
        rpcuser = 'multichainrpc'
    if args.rpcpasswd:
        rpcpasswd = args.rpcpasswd
    else:
        filename = '/home/ubuntu/.multichain/logchain/multichain.conf'
        rpcpasswd = dict(map(lambda y: y.split('='), \
                     filter(lambda x: x, \
                            open(filename).read().split('\n'))))['rpcpassword']
    rpchost = 'localhost'

    if args.rpcport:
        rpcport = args.rpcport
    else:
        rpcport = '4920'

    if args.chainname:
        chainname = args.chainname
    else:
        chainname = 'logchain'

    #print 'connecting Savoir(%s,%s,%s,%s,%s)' % (rpcuser,rpcpasswd,rpchost,rpcport,chainname)
    savoirapi = Savoir(rpcuser,rpcpasswd,rpchost,rpcport,chainname)
    insert(savoirapi,args.targetrecordspersecond)

