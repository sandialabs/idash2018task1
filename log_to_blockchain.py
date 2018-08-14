#TODO: thread to balance optimize disk i/o vs network, try to saturate
#TODO: look into the rawtransaction way of doing multiple things at once
#TODO: look into optimizing/issuing network directly, instead of through RPC
import subprocess
import binascii
import json
import sys
import time
import random
import os

devnull = open(os.devnull, 'w')

start_time = time.time()

addr = json.loads(subprocess.check_output(["multichain-cli",\
                                           "logchain","listaddresses"],\
                                          stderr=devnull))[0]['address']	
numinsertions = 0
d = {}
for line in sys.stdin.readlines():
	(d['Timestamp'],d['Node'],d['ID'],d['Ref-ID'],d['User'],\
         d['Activity'],d['Resource']) = line[:-1].split('\t')
	for int_key in ['Node', 'ID', 'Timestamp', 'User', 'Ref-ID']:
		d[int_key] = int(d[int_key])
        sends = []

        sends.append({'for':'logdata', 'key':'UID_' + str(d['Node']) + \
                      '_' + str(d['ID']), 'data': binascii.hexlify(json.dumps(d))})
	numinsertions += 1

	for (k,v) in d.iteritems():

                sends.append({'for':str(k), 'key':str(k)+'_'+str(v), \
                              'data': binascii.hexlify('UID_' + str(d['Node']) + \
                                                       '_' + str(d['ID']))})
		if k == 'Timestamp':
                        sends.append({'for':"node%dtimestamps" % int(d['Node']), \
                                      'key':str(v), \
                                      'data':binascii.hexlify('UID_' \
                                                              + str(d['Node'])\
                                                              + '_' + str(d['ID']))})
        cmd = ['multichain-cli', 'logchain', 'createrawsendfrom', \
               '%s' % addr, "{}", "%s" % json.dumps(sends), 'send']

	subprocess.check_output(cmd,stderr=devnull)	

elapsed_time = time.time() - start_time

insertionstring = 'insertion took %s seconds, %s secs/record (%s records)' % \
			(elapsed_time, elapsed_time/numinsertions, numinsertions)

print insertionstring

