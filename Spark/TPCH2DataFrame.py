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



class TPCH2DataFrame():

    customer_types = [('custkey', 'int'), ('name', 'text'), ('address', 'text'), ('nationkey', 'int'), ('phone', 'text'),
                      ('acctbal', 'decimal'), ('mktsegment', 'text'), ('comment', 'text')]
    lineitem_types = [('orderkey', 'int'), ('partkey', 'int'), ('suppkey', 'int'), ('linenumber', 'int'),
                      ('quantity', 'int'), ('extendedprice', 'decimal'), ('discount', 'decimal'), ('tax', 'decimal'),
                      ('returnflag', 'text'), ('linestatus', 'text'), ('shipdate', 'date'), ('commitdate', 'date'),
                      ('receiptdate', 'date'), ('shipinstruct', 'text'), ('shipmode', 'text'), ('comment', 'text')]
    nation_types = [('nationkey', 'int'), ('name', 'text'), ('regionkey', 'int'), ('comment', 'text')]
    orders_types = [('orderkey', 'int'), ('custkey', 'int'), ('orderstatus', 'text'), ('totalprice', 'decimal'),
                    ('orderdate', 'date'), ('order_priority', 'text'), ('clerk', 'text'), ('ship_priority', 'int'),
                    ('comment', 'text')]
    partsupp_types = [('partkey', 'int'), ('suppkey', 'int'), ('availqty', 'int'), ('supplycost', 'decimal'),
                      ('comment', 'text')]
    part_types = [('partkey', 'int'), ('name', 'text'), ('mfgr', 'text'), ('brand', 'text'), ('type', 'text'),
                  ('size', 'int'), ('container', 'text'), ('retailprice', 'decimal'), ('comment', 'text')]
    region_types = [('regionkey', 'int'), ('name', 'text'), ('comment', 'text')]
    supplier_types = [('suppkey', 'int'), ('name', 'text'), ('address', 'text'), ('nationkey', 'int'), ('phone', 'text'),
                      ('acctbal', 'decimal'), ('comment', 'text')]

    tables = [  # table name, field/type list, primary key
      #  ('customer', customer_types, 'custkey'),
      #  ('nation', nation_types, 'nationkey'),
        ('orders', orders_types, 'orderkey'),
      #  ('partsupp', partsupp_types, 'partkey, suppkey'),
        ('part', part_types, 'partkey'),
      #  ('region', region_types, 'regionkey'),
      #  ('supplier', supplier_types, 'suppkey'),
        ('lineitem', lineitem_types, 'linenumber, orderkey'),
    ]

    def __init__(self, sc, input_dir):
        for tbl, types, primarykey in self.tables:
            self.read_table(sc, input_dir, tbl, types)


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
        return cdata

    def read_table(self,sc, input_dir, tbl, types):
        infile = os.path.join(input_dir, tbl + '.tbl.gz')
        cdata = sc.textFile(infile, minPartitions=100).map(lambda line: self.to_dataframe_data(line, types)).setName(tbl)
        dfData=cdata.toDF()
        print "Registering temporary table:",  tbl
        dfData.registerTempTable(tbl)













