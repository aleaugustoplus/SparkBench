##############################################################################
#                              Alexandre A. S. Lopes                         #
#                             Prithvi Lakshminarayanan                       #
#                       Master of Computer Science - Big Data                #
#                                  alopes@sfu.ca                             #
#                                  plakshmi@sfu.ca                           #
#                                    03/10/2017                              #
##############################################################################

import socket
import timeit
from abc import ABCMeta, abstractmethod

class Benchmark(object):
    __metaclass__ = ABCMeta

    def __init__(self, Name, Num_Exec=3):
        self.Name=Name
        self.Hostname=socket.gethostname()
        self.Num_Exec=Num_Exec

    def GetResults(self):
        return self.Results

    def Measure(self):
        times=[]
        for i in range(self.Num_Exec):
            start = timeit.timeit()
            self.process()
            end = timeit.timeit()
            times.append(end - start)



        self.Results["time"] = sum(times)/len(times)

    @abstractmethod
    def process(self):
        pass










