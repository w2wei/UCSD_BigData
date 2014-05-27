## count number of valid station/year. >50% TMAX and TMIN 
#!/usr/bin/python
"""
count the number of valid measurements. A valide record has at least 50% days with TMAX and TMIN.
"""

class countMeas(MRJob):
    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            rec = line.split(",")
            stn,meas,year = rec[:3]
            stderr.write(stn+"\t"+meas+"\t"+year)
            if meas=='TMAX' or meas=='TMIN': # this is measurement is about tmax of tmin
                if len(filter(None,rec[3:]))/365.0>0.5: # measurements during >50% days of the year 
                    stderr.write("mapper: "+",".join(rec[:3]))
                    yield (stn,1)
        except Exception, e:
            stderr.write('Error in line:\n'+str(line[:5]))
            stderr.write(str(e))
            self.increment_counter('MrJob Counters','mapper-error',1)

    def reducer(self, key, value):
        self.increment_counter('MrJob Counters','reducer',1)
        try:
            l_counts=[v for v in value]
            s=sum(l_counts)
            yield (key,s)

        except Exception as e:
            stderr.write('Error:')
            stderr.write(str(e))

if __name__ == '__main__':
    countMeas.run()