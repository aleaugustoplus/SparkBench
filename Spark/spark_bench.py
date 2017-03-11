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
from DFBench import DFBench,DFJoin

TPCH_DATASET_PATH="Data/a4-tpch-0"

#global log
def RunDataFrame(sc,sqlCt):
    DFBench.LoadData(sc, TPCH_DATASET_PATH)
    Bench=DFJoin(sqlCt)
    Bench.Measure()
    print "Results:", Bench.GetResults()



if __name__ == "__main__":
    conf = SparkConf().setAppName('Spark benchmark')
    sc = SparkContext(conf=conf)
    sqlCt = SQLContext(sc)
    log4j = sc._jvm.org.apache.log4j
    log = log4j.LogManager.getLogger(__name__)
    log4j.PropertyConfigurator.configure("log4j.properties")
    log4j.LogManager.getLogger(__name__).setLevel(log4j.Level.DEBUG)

    RunDataFrame(sc,sqlCt)

    log4j.LogManager.getLogger(__name__).setLevel(log4j.Level.INFO)






















