from mrjob.job import MRJob
import csv
import chardet
#_*_coding : utf-8_*_

class Histo_incidences(MRJob):


	def mapper(self,_,line):

		for row in csv.reader(line.splitlines(),delimiter=','):
			key = dict(zip(['Date','Localisation','ESR','Type'], row))
			yield key['Type'], 1

	def reducer(self,key,value):
		yield key, sum(value)


if __name__=='__main__':
        Histo_incidences.run()


