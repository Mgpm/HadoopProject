from mrjob.job import MRJob

class incMax(MRJob):

        def mapper(self,_,lines):
                lines = lines.split('\t')
                yield None, (int(lines[1]),lines[0])

        def reducer(self,_, values):
                yield "MAX-INCIDENCE", max(values)
if __name__ == "__main__":
        incMax.run()



