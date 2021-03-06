import pylibmc, time, csv
from datetime import datetime
import random, sys, os
import numpy as np
from multiprocessing import Queue, Pool, Process
import multiprocessing
import threading
import signal
import redis
import argparse
from ZipfGenerator import ZipfGenerator
#import logging

sizes=[str(i).zfill(5) for i in xrange(1,10001)]
indexes=[str(i).zfill(6) for i in xrange(0,1000000)]

def sendRequest_multi(db,mc,lf):
	p=0.08
	keys=[]
	n=50
	valsizes=[(i-1)%10000 for i in np.random.geometric(p,n)]
#	print valsizes
	for i in xrange(0,n):
		# both random variables are used as index so it is +1 usually
#		valsize=(np.random.geometric(p)-1)%10000
		valsize=valsizes[i]
		if valsize<100: # remember it is a index
#			indx=random.randint(0,1000-1)
			indx=random.randint(0,1000000-1)
		elif (valsize+1)<1000: # valsize is index, actual size is +1
			indx=random.randint(0,100000-1)
		else:
			indx=random.randint(0,500-1)

		key=''.join([sizes[valsize],indexes[indx]])
		keys.append(key)
#		print key
#		sys.stdout.flush()
#		break
	st=time.time()
	while True:
		try:
			vals=mc.get_multi(keys)
			break
		except (pylibmc.ConnectionError, pylibmc.ServerDead, pylibmc.ServerDown):
			exc_type, exc_obj, exc_tb = sys.exc_info()
#			lf.write("key: %s, excptn in MC get: %s\n"%( key, str(exc_type) )  )
#			lf.flush()
#			print "Error getting key %s exception type:%s"%(key,str(exc_type))
#			sys.stdout.flush()
			continue
		except:
			exc_type, exc_obj, exc_tb = sys.exc_info()
			lf.write("key: %s, excptn in MC get: %s\n"%( key, str(exc_type) )  )
			lf.flush()
			break
	hit_et=time.time()-st
	not_found=[i for i in keys if i not in vals]
	misses=len(not_found)
	nbr_hit=n-misses
	if misses>0:
#		return (0,False)
		try:
			val=db.mget(not_found) # ordered list of values identically ordered as not_found
			if val==None or len(val)<len(not_found):
				raise ValueError('Value returned for key was None from DB')
			miss_et=time.time()-st
		except Exception as e:
			lf.write("key: %s, excptn in DB get: %s\n"%( not_found[0],str(sys.exc_info()) ) )
			lf.flush()
			return None
		while True:
			try:
				mc.set_multi(dict(zip(not_found,val)))
				break
			except (pylibmc.ConnectionError, pylibmc.ServerDead, pylibmc.ServerDown):
#				exc_type, exc_obj, exc_tb = sys.exc_info()
#				print "Error setting key %s exception type:%s"%(key,str(exc_type))
#				sys.stdout.flush()
				continue
			except:
				exc_type, exc_obj, exc_tb = sys.exc_info()
				lf.write("key: %s, excptn in MC set: %s\n"%( not_found[0], str(exc_type) ) )
				lf.flush()
				break
		return (time.time()-st,hit_et,nbr_hit,miss_et,misses) # some miss in cache
	else:
		return (time.time()-st,hit_et,n,0,0) # all hit in cache


#def sendRequest(db,mc,lf,zg):
def sendRequest(db,mc,lf):
	p=0.08
	for i in xrange(0,1):
		# both random variables are used as index so it is +1 usually
		valsize=(np.random.geometric(p)-1)%100
#		valsize=random.randint(0,100-1)
		if valsize<100: # remember it is a index
			indx=random.randint(0,1000-1)
#			indx=random.randint(0,1000000-1)
		elif (valsize+1)<1000: # valsize is index, actual size is +1
			indx=random.randint(0,100000-1)
		else:
			indx=random.randint(0,500-1)

		key=''.join([sizes[valsize],indexes[indx]])
#		print key
#		sys.stdout.flush()
#		break
		st=time.time()
		while True:
			try:
				val=mc.get(key)
				break
			except (pylibmc.ConnectionError, pylibmc.ServerDead, pylibmc.ServerDown):
				exc_type, exc_obj, exc_tb = sys.exc_info()
#				lf.write("key: %s, excptn in MC get: %s\n"%( key, str(exc_type) )  )
#				lf.flush()
#				print "Error getting key %s exception type:%s"%(key,str(exc_type))
#				sys.stdout.flush()
				continue
			except:
				exc_type, exc_obj, exc_tb = sys.exc_info()
				lf.write("key: %s, excptn in MC get: %s\n"%( key, str(exc_type) )  )
				lf.flush()
				break
		
		if val is None:
			return (0,False)
			try:
				val=db.get(key)
				if val==None:
					raise ValueError('Value returned for key was None from DB')
			except Exception as e:
				lf.write("key: %s, excptn in DB get: %s\n"%( key,str(sys.exc_info()) ) )
				lf.flush()
				continue
			while True:
				try:
					mc.set(key,str(val))
					break
				except (pylibmc.ConnectionError, pylibmc.ServerDead, pylibmc.ServerDown):
#					exc_type, exc_obj, exc_tb = sys.exc_info()
#					print "Error setting key %s exception type:%s"%(key,str(exc_type))
#					sys.stdout.flush()
					continue
				except:
					exc_type, exc_obj, exc_tb = sys.exc_info()
					lf.write("key: %s, excptn in MC set: %s\n"%( key, str(exc_type) ) )
					lf.flush()
					break
			return (time.time()-st,False) # miss in cache
		else:
			return (time.time()-st,True) # hit in cache
		

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    
def init_memcached():
#	mc=pylibmc.Client(["mc1:11211","mc2:11211","mc3:11211","mc4:11211","mc5:11211","mc6:11211"])
#	mc=pylibmc.Client(["mc1:11211","mc2:11211","mc3:11211","mc4:11211"])
	mc=pylibmc.Client(["mc1:11211","mc2:11211","mc3:11211"])
#	mc=pylibmc.Client(["localhost:11212"])
	mc.behaviors['ketama'] = True
	mc.behaviors['remove_failed'] = 1
	mc.behaviors['retry_timeout'] = 1
	mc.behaviors['dead_timeout'] = 60
	return mc

def doWork(p_i,lambd,nbr_req,rates):
#	db=pylibmc.Client(["database:21201"]) # port for memcachedb
	db=redis.StrictRedis(host='database',port=16379,db=0)
	mc=init_memcached()
	lf=open("mplogs/log_%d.txt"%p_i,'w')
	if rates is None:
#		print "IAFILE not provided, using const-RR"
#		sys.stdout.flush()
		i=0
		while i<=nbr_req:
			i+=1
			st=time.time()
	#		sendRequest(db,mc,None)
			ret=sendRequest_multi(db,mc,lf) # contains (RT,hit)
#			ret=sendRequest(db,mc,lf) # contains (RT,hit)
	#		sendRequest(db,mc,lf,zipfgen)
			el=time.time()-st # total time spent in function
			line="%s %f %f %f %i %f %i\n"%(datetime.now().strftime('%H:%M:%S'),el*1000,ret[0]*1000,ret[1]*1000,ret[2],ret[3]*1000,ret[4])
			lf.write(line)
			wt=random.expovariate(lambd)
			time.sleep(wt)
	else:
		for tup in rates:
			i=0
			if p_i==1:
				print tup
				sys.stdout.flush()
				
			while i<=tup[1]:
				i+=1
				st=time.time()
#				ret=sendRequest(db,mc,lf) # contains (RT,hit)
				ret=sendRequest_multi(db,mc,lf) # (time.time()-st,hit_et,nbr_hit,miss_et,misses)
				el=time.time()-st # total time spent in function
				line="%s %f %f %f %i %f %i\n"%(datetime.now().strftime('%H:%M:%S'),el*1000,ret[0]*1000,ret[1]*1000,ret[2],ret[3]*1000,ret[4])
				lf.write(line)
				wt=random.expovariate(tup[0]) # (rr,nr) expovariate takes 1/mean of exponential which is RR
				time.sleep(wt)
			

	print "Process %d ended"%p_i
	sys.stdout.flush()
	lf.flush()
	lf.close()


#wait_pool=threading.Condition()

def main(concurrent,n,lambd,iafile):
	rates=None
	if iafile is not None:
		rates=[]
		for line in iafile:
			rr , nr=line.partition(" ")[::2]
			rr , nr=float(rr)/concurrent,int(nr)/float(concurrent)
			rates.append((rr,nr))
		iafile.close()
#	print rates
#	return
	try:
		pool=Pool(concurrent,init_worker)
		for i in xrange(concurrent):
			res=pool.apply_async(doWork,args=(i,lambd/concurrent,n/concurrent,rates,))
#			res.get()
		print "Started process pool"
		pool.close()
		strt=time.time()
		pool.join()
		el=time.time()-strt
		print "requests processed in time:%06f seconds, request rate approx: %f per sec"%(el,float(n)/el)
	except KeyboardInterrupt:
		print "received interrupt"
		pool.terminate()
		pool.join()
		return
	except:
		print "received exception"
		pool.terminate()
		pool.join()
		return

#	print "Completed, average latency:%0.6f hit_rate:%0.3f, hits:%i"%(rts/(n*10),float(nbr_hits)/(n*10),nbr_hits)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Load generation script for memcached, ardb setup')
	parser.add_argument('-c',help='Concurrency, number of threads, default=120',type=int,required=False,default=120)
	parser.add_argument('-n',help='Total requests to send, default=100000000',type=int,required=False,default=100000000)
	parser.add_argument('-l',help='When specified, used as max request rate, default=200000',type=int,required=False,default=200000)
	parser.add_argument('-iafile',help='When specified, uses file for request rates -l option is ignored',required=False, type=argparse.FileType('r'))
	
	args = parser.parse_args()
#	print args
	main(args.c,args.n,args.l,args.iafile)
