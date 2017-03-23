##############################################################################
#                              Alexandre A. S. Lopes                         #
#                             Prithvi Lakshminarayanan                       #
#                       Master of Computer Science - Big Data                #
#                                  alopes@sfu.ca                             #
#                                  plakshmi@sfu.ca                           #
#                                    03/10/2017                              #
##############################################################################

import socket
import time
from abc import ABCMeta, abstractmethod

class Benchmark(object):
    __metaclass__ = ABCMeta

    Results={}
    Name=""
    Hostname=""


    def __init__(self, Name, Num_Exec=3):
        self.Name=Name
        self.Hostname=socket.gethostname()
        self.Num_Exec=Num_Exec

    def GetResults(self):
        return self.Results

    def Measure(self):
        times=[]

        for i in range(self.Num_Exec):
            self.pre_process()
            start = time.time()
            self.process()
            end = time.time()
            times.append(end - start)

        self.Results["time"] = float(sum(times))/float(len(times))


        #self.Results["time"] = 0

    def pre_process(self):
        return


    @abstractmethod
    def process(self):
        pass










