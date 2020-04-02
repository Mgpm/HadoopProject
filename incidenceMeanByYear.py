from mrjob.job import MRJob
import numpy as np 

class incMeanByYear(MRJob):

	def mapper(self,_,lines):
		lines = lines.split(',')
		yield  lines[3],1
	def combiner(self,incidence,values):
			yield incidence,sum(values)

	def reducer(self,incidence,values):
			 for i in values:
			 	yield incidence,i/4
if __name__ == "__main__":
        incMeanByYear.run()



