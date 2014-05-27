#!/usr/bin/python
"""
run PCA on all nodes, all data from one station
"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
from sys import stderr
import pandas as pd
import re,pickle,base64,zlib
import numpy as np

def decode(eVal):
    """ Decode a string into a value """
    return pickle.loads(zlib.decompress(base64.b64decode(eVal)))

def encode(Value):
    """ Encode a value as a string """
    return base64.b64encode(zlib.compress(pickle.dumps(Value),9))

def checkThreshold(x):
    x=list(x)
    stderr.write(str(x[:10]))
    for i in x:
        if i > 0.99:
            return x.index(i)

class weatherPCA(MRJob):
    def NN(self,vec):
        """nearest neighbor method to substitute missing values in record the nearest left neighbor"""
        # if there is None
        if not None in vec:
            return vec
        else:
#             stderr.write("type of vector "+"\t"+str(type(vec)))
            for i in xrange(len(vec)):
                if vec[i]==None:
                    if i>0:
                        vec[i]=vec[i-1]
                    else:
                        vec[i]=filter(None,vec)[0]

            return vec
            
    def str2flt(self,vec):
        """convert string elements to float; missing values are replaced with None"""
        newvec=[]
        for v in vec:
            try:
                newv=float(v)
            except:
                newv=None
            finally:
                newvec.append(newv)
        return newvec
                
    def pca_dat_prep(self, _, line):
        '''
        Input is a station and the records of tmax and tmin of some year.\
        Output is station and encoded 365*2 numpy arrays
        '''
        try:
            stn,trecEn = line.split("\t")
            stn=stn.strip('\"')
            trec=decode(trecEn.strip('\"'))
            # array element type str->float, drop NaN and substitude the value with nearest neighbor
            tmax = self.NN(self.str2flt(trec["TMAX"]))
            tmin = self.NN(self.str2flt(trec["TMIN"]))
            # for double check
            if None in tmax or None in tmin:
                stderr.write("None exists\n")
#           stderr.write("tmax: "+"\t"+str(tmax)+"\n") # 
            datArr = np.array([tmax,tmin]).T
#           stderr.write("dataArr "+str(datArr.shape)+"\n")
            yield stn, encode(datArr)
        except Exception, e:
            stderr.write('Error in line:\n'+str(line[:5]))
            stderr.write(str(e))

    def pca_indi_sum(self,key,value):
        '''get the sum of measurement 2-D numpy arrays'''
        try:
            stn=key
            vals=list(value)
            arrays = [decode(x) for x in vals]#x = x[~numpy.isnan(x)] # remove nan
            measSum=np.zeros(shape=arrays[0].shape) # neasSum type: numpy array
#             stderr.write("measSum initialized\n")
            for arr in arrays:
                measSum+=arr
            yield stn,(encode(measSum),encode(len(arrays)),encode(arrays)) # sum of arrays
        except Exception as e:
            stderr.write('Error:')
            stderr.write(str(e)+"\t")
            
    def pca_indi_mean(self,key,value):
        '''calculate the mean vector of this node/station'''
        try:
            stn=key
            vals=[v for v in value]
            measSumEn,countEn,arraysEn = vals[0]
            measSum = decode(measSumEn)
            count=decode(countEn)
            mean = measSum/float(count)
#             stderr.write(stn+"\t"+str(mean))
            yield stn,(encode(mean),arraysEn)
        except Exception as e:
            stderr.write('Error:')
            stderr.write(str(e)+"\t")    

    
    def pca_indi_cov(self,key,value):
        "calculate variance and covariance matrix"
        try:
            stn=key
            vals=list(value)
            meanEn,arraysEn = vals[0]
            mean=decode(meanEn)
            arrays=decode(arraysEn) # shape of each array (365,2)
            cov=[]
            for arr in arrays: # remove [:2], this is just for testing
                v=arr-mean# variance array
                cov.append(np.dot(v,v.T)) ## Here should use np.dot rather than np.outer. The np definition is confusing. 
                                          ## if np.outer, the dimension is incorrect. shuld be 365*365
#             stderr.write("shape of cov[0]: "+"\t"+str(cov[0].shape))
            yield stn,encode(cov)
        except Exception as e:
            stderr.write('Error:')
            stderr.write(str(e)+"\n")
            
    def pca_indi_svd(self,key,value):
        """get eigen vectors explaining 99% of variance"""
        try:
            stn=key
            vals=list(value)
            covArrEn = vals[0] # encoded covariance arrays
#             stderr.write("cov: "+"\t"+str(len(decode(covArrEn)))+"\n"+str(decode(covArrEn)[:5])+"\n")
            covArr = decode(covArrEn)
            covSum = np.zeros(covArr[0].shape)
            for cov in covArr:
                covSum+=cov
            meanCov = covSum/float(len(covArr))
            ## SVD below
            U,D,V=np.linalg.svd(cov)
            varXpln = np.cumsum(D[:])/np.sum(D) # cummulative density of explained variance
#             stderr.write("var explained: \n"+str(varXpln))
#             stderr.write("shape of U: "+"\t"+str(U.shape))
#             stderr.write("shape of D: "+"\t"+str(D.shape))
#             stderr.write("shape of V: "+"\t"+str(V.shape))
            ## eigen vector selection, need explain 99% variance
            cut=checkThreshold(varXpln)
#             stderr.write("cut: "+'\n'+str(cut))
            ev = U[:,:cut] ## selected eigen vectors
#             stderr.write("Results: \n"+str(ev.shape)+"\n"+str(ev)+'\n')
            yield stn,encode(ev)
        except Exception as e:
            stderr.write('Error:')
            stderr.write(str(e)+"\n")
            
    def steps(self):
        return [
            self.mr(mapper=self.pca_dat_prep,
                    reducer=self.pca_indi_sum),
            self.mr(reducer=self.pca_indi_mean),
            self.mr(reducer=self.pca_indi_cov),
            self.mr(reducer=self.pca_indi_svd)
            
        ]


if __name__ == '__main__':
    weatherPCA.run()