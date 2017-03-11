##############################################################################
#                              Alexandre A. S. Lopes                         #
#                             Prithvi Lakshminarayanan                       #
#                       Master of Computer Science - Big Data                #
#                                  alopes@sfu.ca                             #
#                                  plakshmi@sfu.ca                           #
#                                    03/10/2017                              #
##############################################################################

from Benchmark import Benchmark
from TPCH2DataFrame import TPCH2DataFrame


class DFBench(Benchmark):

    @staticmethod
    def LoadData(sc, input_dir):
        TPCH2DataFrame(sc, input_dir)

    def __init__(self, Context, Name, Num_Exec=3):

        super(DFBench,self).__init__(Name, Num_Exec)
        self.Context = Context




class DFJoin(DFBench):

    def __init__(self, SQLContext, Num_Exec=3):
        super(DFJoin, self).__init__(SQLContext, "DataFrameJoin", Num_Exec)


    def process(self):
        dfResult=self.Context.sql("""SELECT o.orderkey, o.totalprice, p.name
                                     FROM orders o
        				             JOIN lineitem l ON (o.orderkey = l.orderkey)
                                     JOIN part p ON (l.partkey = p.partkey)""")
        dfResult.count()







