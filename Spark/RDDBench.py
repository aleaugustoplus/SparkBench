##############################################################################
#                              Alexandre A. S. Lopes                         #
#                             Prithvi Lakshminarayanan                       #
#                       Master of Computer Science - Big Data                #
#                                  alopes@sfu.ca                             #
#                                  plakshmi@sfu.ca                           #
#                                    03/10/2017                              #
##############################################################################

from Benchmark import Benchmark



class RDDBench(Benchmark):



    def __init__(self, RDDs, Name, Num_Exec=3):

        super(RDDBench,self).__init__(Name, Num_Exec)
        self.RDDs = RDDs




class RDDJoin(RDDBench):

    def __init__(self, RDDs, Num_Exec=3):
        super(RDDJoin, self).__init__(RDDs, "RDDJoin", Num_Exec)


    def process(self):
        #dfResult=self.Context.sql("""SELECT o.orderkey, o.totalprice, p.name
        #                             FROM orders o
        #				             JOIN lineitem l ON (o.orderkey = l.orderkey)
        #                             JOIN part p ON (l.partkey = p.partkey)""")


        RDDResult=self.RDDs["orders"]\
                      .map(lambda row: (row['orderkey'],row))\
                      .join(self.RDDs["lineitem"].map(lambda row: (row['orderkey'],row)))\
                      .map(lambda (k,v): (v[1]['partkey'], v))\
                      .join(self.RDDs["part"].map(lambda row: (row['partkey'],row)))

        RDDResult.count()
        #print "Take end:",RDDResult.take(1)

class RDDSort(RDDBench):

    def __init__(self, RDDs, Num_Exec=3):
        super(RDDSort, self).__init__(RDDs, "RDDSort", Num_Exec)


    def process(self):
        RDDResult = self.RDDs["orders"].sortBy(lambda row: row['orderkey'])
        RDDResult.first()
        RDDResult.count()


class RDDReduceByKey(RDDBench):

    def __init__(self, RDDs, Num_Exec=3):
        super(RDDReduceByKey, self).__init__(RDDs, "RDDReduceByKey", Num_Exec)


    def process(self):

        RDDResult = self.RDDs["orders"] \
                        .map(lambda row: (row['orderkey'], row))\
                        .reduceByKey(lambda v,u:  v)


        RDDResult.count()




