from mrjob.job import MRJob

class incMin(MRJob):

        def mapper(self,_,lines):
                lines = lines.split('\t')
                yield None, (int(lines[1]),lines[0])

        def reducer(self,_, values):
                yield "MIN-INCIDENCE",min(values)
if __name__ == "__main__":
        incMin.run()

