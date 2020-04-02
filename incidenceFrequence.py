#!/usr/bin/python

from mrjob.job  import MRJob

class incFreq(MRJob):

	def mapper(self,_,lines):
		lines = lines.split(',')
		key = lines[3]
		yield key,1

	def reducer(self,key,values):
		yield key, sum(values)

if __name__ == "__main__":
	incFreq.run()

