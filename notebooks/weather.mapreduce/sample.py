# sample hdfs:/weather/weather.csv to make a test set.
#!/usr/bin/python
"""
sample a small set for testing
"""
import random
class sample(MRJob):
    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            if random.random()<0.001: ## sample ratio 1/1000 here, I did 1/100 as well
                yield (None,line)
        except Exception, e:
            stderr.write(str(e))
            self.increment_counter('MrJob Counters','mapper-error',1)

if __name__ == '__main__':
    sample.run()