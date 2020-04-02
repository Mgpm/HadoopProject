from mrjob.job import MRJob
import numpy as np

class incMean(MRJob):

	def mapper(self,_,lines):
		lines = lines.split('\t')
		yield None,(int(lines[1]),)
	def reducer(self,_, values):
		p = []
		for i in values:
		       p.append(i[0])
		p = np.array(p)
		yield "MEAN-INCIDENCE", p.mean()

if __name__ == "__main__":
	incMean.run()
