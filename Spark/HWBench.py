##############################################################################
#                              Alexandre A. S. Lopes                         #
#                             Prithvi Lakshminarayanan                       #
#                       Master of Computer Science - Big Data                #
#                                  alopes@sfu.ca                             #
#                                  plakshmi@sfu.ca                           #
#                                    03/10/2017                              #
##############################################################################

from Benchmark import Benchmark
from TPCH2Data import TPCH2Data

hosts=['nml-cloud-215.cs.sfu.ca', 'nml-cloud-235.cs.sfu.ca',
       'nml-cloud-233.cs.sfu.ca','nml-cloud-193.cs.sfu.ca',
       'nml-cloud-210.cs.sfu.ca', 'nml-cloud-152.cs.sfu.ca']
#hosts=['ale-ultra']




class HWBench(Benchmark):


    def __init__(self, SQLContext, Name, Num_Exec=1):
        super(HWBench,self).__init__(Name, Num_Exec)
        self.Context = SQLContext


class HWNetwork(HWBench):

    def __init__(self, SQLContext, Num_Exec=3):
        super(HWNetwork, self).__init__(SQLContext, "Network", Num_Exec)


    def process(self):
        dfResult=self.Context.sql("""SELECT o1.*,o2.*
                                     FROM orders o1
                                     JOIN orders o2
                                     ON o1.orderkey=o2.orderkey""")
        dfResult.count()

class HWDiskWrite(HWBench):

    def __init__(self, SQLContext, Num_Exec=3):
        super(HWDiskWrite, self).__init__(SQLContext, "DiskWrite", Num_Exec)


    def pre_process(self):
        
        self.Context.sql("""SELECT * FROM orders""").cache().count()

    def process(self):
        dfResult=self.Context.sql("""SELECT concat(*) FROM orders""")
        dfResult.write.format('text').mode("overwrite").save('benchmark_tmp.txt')

class HWDiskRead(HWBench):

    Path=""

    def __init__(self, SQLContext, Num_Exec=3):
        super(HWDiskRead, self).__init__(SQLContext, "DiskRead", Num_Exec)


    def pre_process(self):
        self.Context.clearCache()

    def process(self):

        self.Context.sql("""SELECT * FROM orders""").count()

#-----------Naive Primality test---------------------
class HWCPU(HWBench):

    SizeOfPrime=0
    SizeOfCluster=0
    Rdd=None
    sc=None
    def __init__(self, SizeOfPrime, SizeOfCluster, sc, SQLContext, Num_Exec=3):
        super(HWCPU, self).__init__(SQLContext, "CPU", Num_Exec)
        self.SizeOfPrime=SizeOfPrime
        self.SizeOfCluster=SizeOfCluster
        self.sc=sc


    def pre_process(self):
        self.Rdd=self.sc.parallelize([self.SizeOfPrime for i in range(self.SizeOfCluster)])


    def process(self):

        def naive_prime_test(Number):

            for i in range(2, Number-1):
                if Number % i == 0:
                    return False

            return True

        self.Rdd.map(naive_prime_test).count()





