##############################################################################
#                              Alexandre A. S. Lopes                         #
#                             Prithvi Lakshminarayanan                       #
#                       Master of Computer Science - Big Data                #
#                                  alopes@sfu.ca                             #
#                                  plakshmi@sfu.ca                           #
#                                    03/10/2017                              #
##############################################################################

from Benchmark import Benchmark
import socket


class HWBench(Benchmark):



    def __init__(self, sc, Name, Num_Exec=3):

        super(HWBench,self).__init__(Name, Num_Exec)
        self.sc = sc




class HWNetwork(HWBench):



    def __init__(self, sc, Num_Exec=3):
        super(HWNetwork, self).__init__(sc, "Network", Num_Exec)
        print "Unique hosts:", self.GetUniqueHosts()

    def GetUniqueHosts(self):

        rdd=self.sc.parallelize(range(1,10000))
        rdd=rdd.map(lambda v: (socket.gethostname(),v))\
               .reduceByKey(lambda a,b: a if a<b else b)
        return rdd.collect()

    def process(self):
        print "Process"




