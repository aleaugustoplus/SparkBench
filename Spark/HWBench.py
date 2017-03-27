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
import socket

hosts=['nml-cloud-215.cs.sfu.ca', 'nml-cloud-235.cs.sfu.ca',
       'nml-cloud-233.cs.sfu.ca','nml-cloud-193.cs.sfu.ca',
       'nml-cloud-210.cs.sfu.ca', 'nml-cloud-152.cs.sfu.ca']
#hosts=['ale-ultra']
"""Project [clerk#0,comment#1,custkey#2L,order_priority#3,orderdate#4,orderkey#5L,orderstatus#6,ship_priority#7L,totalprice#8,clerk#128,comment#129,custkey#130L,order_priority#131,orderdate#132,
orderkey#133L,orderstatus#134,ship_priority#135L,totalprice#136]
+- SortMergeJoin [orderkey#5L], [orderkey#133L]
   :- Sort [orderkey#5L ASC], false, 0
   :  +- TungstenExchange hashpartitioning(orderkey#5L,200), None
   :     +- ConvertToUnsafe
   :        +- Scan ExistingRDD[clerk#0,comment#1,custkey#2L,order_priority#3,orderdate#4,orderkey#5L,orderstatus#6,ship_priority#7L,totalprice#8] 
   +- Sort [orderkey#133L ASC], false, 0
      +- TungstenExchange hashpartitioning(orderkey#133L,200), None
         +- ConvertToUnsafe
            +- Scan ExistingRDD[clerk#128,comment#129,custkey#130L,order_priority#131,orderdate#132,orderkey#133L,orderstatus#134,ship_priority#135L,totalprice#136]"""

"""select cpu_system_rate + cpu_user_rate where category= HOST and 
(hostname = 'nml-cloud-235.cs.sfu.ca' or hostname = 'nml-cloud-210.cs.sfu.ca' or 
hostname = 'nml-cloud-233.cs.sfu.ca' or hostname = 'nml-cloud-152.cs.sfu.ca' or 
hostname = 'nml-cloud-193.cs.sfu.ca')"""

class HWBench(Benchmark):


    def __init__(self, SQLContext, Name, Num_Exec=1):
        super(HWBench,self).__init__(Name, Num_Exec)
        self.Context = SQLContext


class HWNetwork(HWBench):

    def __init__(self, SQLContext, Num_Exec=3):
        super(HWNetwork, self).__init__(SQLContext, "Network", Num_Exec)

    def pre_process(self):

	self.Context.clearCache()
        self.Context.sql("""SELECT * FROM orders""").cache().count()


    def process(self):
        dfResult=self.Context.sql("""SELECT o1.*,o2.*
                                     FROM orders o1
                                     JOIN orders o2
                                     ON o1.orderkey=o2.orderkey""")
#	print dfResult.explain()
        dfResult.count()

class HWDiskWrite(HWBench):

    def __init__(self, SQLContext, Num_Exec=3):
        super(HWDiskWrite, self).__init__(SQLContext, "DiskWrite", Num_Exec)


    def pre_process(self):
        
        self.Context.sql("""SELECT * FROM orders""").cache().count()

    def process(self):
        dfResult=self.Context.sql("""SELECT comment FROM orders""")
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
           i=2
           while i < Number:
                if Number % i == 0:
                    return (socket.gethostname(), i)
                i=i+1

           return (socket.gethostname(), i)


        print "Hosts", self.Rdd.map(naive_prime_test).reduceByKey(lambda v, v2: v).collect()
	



