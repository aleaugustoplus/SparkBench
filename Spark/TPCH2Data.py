##############################################################################
#                              Alexandre A. S. Lopes                         #
#                             Prithvi Lakshminarayanan                       #
#                       Master of Computer Science - Big Data                #
#                                  alopes@sfu.ca                             #
#                                  plakshmi@sfu.ca                           #
#                                    03/10/2017                              #
##############################################################################

import sys, os
import decimal
import datetime
from pyspark.sql import Row



class TPCH2Data():

    lineitem_types = [('orderkey', 'int'), ('partkey', 'int'), ('suppkey', 'int'), ('linenumber', 'int'),
                      ('quantity', 'int'), ('extendedprice', 'decimal'), ('discount', 'decimal'), ('tax', 'decimal'),
                      ('returnflag', 'text'), ('linestatus', 'text'), ('shipdate', 'date'), ('commitdate', 'date'),
                      ('receiptdate', 'date'), ('shipinstruct', 'text'), ('shipmode', 'text'), ('comment', 'text')]

    orders_types = [('orderkey', 'int'), ('custkey', 'int'), ('orderstatus', 'text'), ('totalprice', 'decimal'),
                    ('orderdate', 'date'), ('order_priority', 'text'), ('clerk', 'text'), ('ship_priority', 'int'),
                    ('comment', 'text')]
    part_types = [('partkey', 'int'), ('name', 'text'), ('mfgr', 'text'), ('brand', 'text'), ('type', 'text'),
                  ('size', 'int'), ('container', 'text'), ('retailprice', 'decimal'), ('comment', 'text')]


    tables = [  # table name, field/type list, primary key
        ('orders', orders_types, 'orderkey'),
        ('part', part_types, 'partkey'),
        ('lineitem', lineitem_types, 'linenumber, orderkey')
    ]

    RDDs={}

    def __init__(self, sc, input_dir):
        for tbl, types, primarykey in self.tables:
            self.RDDs[tbl]=self.read_table(sc, input_dir, tbl, types)



    def to_dataframe_data(self,line, types):



        def fix_type(d):
            v, (_, t) = d
            if t == 'int':
                return int(v)
            elif t == 'decimal':
                return decimal.Decimal(v)
            elif t == 'text':
                return v
            elif t == 'date':
                # storing dates as strings: pyspark-cassandra doesn't seem to support the date type
                return datetime.datetime.strptime(v, '%Y-%m-%d').date().isoformat()
            else:
                raise ValueError, t

        fdata = zip(line.split('|'), types)
        data = map(fix_type, fdata)
        cdata = dict((f, v) for v, (f, t) in zip(data, types))
        return Row(**cdata)

    def read_table(self,sc, input_dir, tbl, types):
        infile = os.path.join(input_dir, tbl + '.tbl.gz')

        cdata = sc.textFile(infile).map(lambda line: self.to_dataframe_data(line, types)).setName(tbl)
        dfData=cdata.toDF()
        print "Registering temporary table:",  tbl
        dfData.registerTempTable(tbl)
        return cdata













