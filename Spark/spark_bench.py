##############################################################################
#                              Alexandre A. S. Lopes                         #
#                             Prithvi Lakshminarayanan                       #
#                       Master of Computer Science - Big Data                #
#                                  alopes@sfu.ca                             #
#                                  plakshmi@sfu.ca                           #
#                                    03/10/2017                              #
##############################################################################

# anomaly_detection.py

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from DFBench import DFJoin,DFOrderBy,DFGroupBy
from RDDBench import RDDJoin,RDDSort, RDDReduceByKey
from HWBench import HWNetwork
from HWBench import HWDiskWrite
from HWBench import HWDiskRead
from HWBench import HWCPU
from TPCH2Data import TPCH2Data

TPCH_DATASET_PATH="Data/tpch-small"
NUMBER_OF_TRIES=1
SIZE_OF_CLUSTER=3
SIZE_OF_PRIME=10000




if __name__ == "__main__":
    conf = SparkConf().setAppName('Spark benchmark')
    sc = SparkContext(conf=conf)
    sqlCt = SQLContext(sc)
    log4j = sc._jvm.org.apache.log4j
    log = log4j.LogManager.getLogger(__name__)
    log4j.PropertyConfigurator.configure("log4j.properties")
    log4j.LogManager.getLogger(__name__).setLevel(log4j.Level.DEBUG)

    data = TPCH2Data(sc, TPCH_DATASET_PATH)
    benchs = []
    # benchs.append(DFJoin(sqlCt,NUMBER_OF_TRIES))
    # benchs.append(DFOrderBy(sqlCt,NUMBER_OF_TRIES))
    # benchs.append(DFGroupBy(sqlCt,NUMBER_OF_TRIES))
    #benchs.append(RDDJoin(data.RDDs, NUMBER_OF_TRIES))
    # benchs.append(RDDSort(data.RDDs, NUMBER_OF_TRIES))
    # benchs.append(RDDReduceByKey(data.RDDs, NUMBER_OF_TRIES))
    benchs.append(HWNetwork(sqlCt, NUMBER_OF_TRIES))
    benchs.append(HWDiskWrite(sqlCt, NUMBER_OF_TRIES))
    benchs.append(HWDiskRead(sqlCt, NUMBER_OF_TRIES))
    benchs.append(HWCPU(SIZE_OF_PRIME, SIZE_OF_CLUSTER*2, sc, sqlCt, NUMBER_OF_TRIES))

    for b in benchs:
        b.Measure()
        print "Name of test:", b.Name
       # print "Hostname:", b.Hostname
        print "Results:", b.GetResults(), " seconds"


    log4j.LogManager.getLogger(__name__).setLevel(log4j.Level.INFO)





















