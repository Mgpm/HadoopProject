#!/usr/bin/python

from mrjob.job import MRJob

class TestMR(MRJob):
	def mapper(self,key,lines):
	      lines = lines.split(',')
	      key = lines[0]
	      yield key,1

	def combiner(self,key,values):
		yield None,(sum(values),key)

	def reducer(self,_,values):
		yield max(values)


if __name__=="__main__":
	TestMR.run()

