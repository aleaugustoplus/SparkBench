##############################################################################
#                              Alexandre A. S. Lopes                         #
#                             Prithvi Lakshminarayanan                       #
#                       Master of Computer Science - Big Data                #
#                                  alopes@sfu.ca                             #
#                                  plakshmi@sfu.ca                           #
#                                    03/10/2017                              #
##############################################################################

from Benchmark import Benchmark
from pyspark import SparkConf
from pyspark import SparkContext
import socket
import sys

#hosts=['nml-cloud-215.cs.sfu.ca', 'nml-cloud-235.cs.sfu.ca',
#       'nml-cloud-233.cs.sfu.ca','nml-cloud-193.cs.sfu.ca',
#       'nml-cloud-210.cs.sfu.ca', 'nml-cloud-152.cs.sfu.ca']
hosts=['ale-ultra']

def Server(v):
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Bind the socket to the port
    server_address = (socket.gethostname(), 10000)
    print  'starting up on %s port %s' % server_address
    try:
        sock.bind(server_address)
        # Listen for incoming connections
        sock.listen(1)
    except BaseException:
        return

    while True:
        # Wait for a connection
        print 'waiting for a connection'
        connection, client_address = sock.accept()

        try:
            print 'connection from', client_address

            # Receive the data in small chunks and retransmit it
            while True:
                data = connection.recv(16)
                print 'received "%s"' % data
                if data:
                    print 'sending data back to the client'
                    connection.sendall(data)
                else:
                    print 'no more data from', client_address
                    break

        finally:
            # Clean up the connection
            connection.close()

def Client(v):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect the socket to the port where the server is listening
    server_address = (v, 10000)
    print >> sys.stderr, 'connecting to %s port %s' % server_address
    sock.connect(server_address)

    try:

        # Send data
        message = 'This is the message.  It will be repeated.'
        print >> sys.stderr, 'sending "%s"' % message
        sock.sendall(message)

        # Look for the response
        amount_received = 0
        amount_expected = len(message)

        while amount_received < amount_expected:
            data = sock.recv(16)
            amount_received += len(data)
            print >> sys.stderr, 'received "%s"' % data

    finally:
        print >> sys.stderr, 'closing socket'
        sock.close()


class HWBench(Benchmark):



    def __init__(self, sc, Name, Num_Exec=1):
        super(HWBench,self).__init__(Name, Num_Exec)
        self.sc = sc




class HWNetwork(HWBench):



    def __init__(self, sc, Num_Exec=1):
        super(HWNetwork, self).__init__(sc, "Network", Num_Exec)
        print "Unique hosts:", self.GetUniqueHosts()

        rdd = self.sc.parallelize(hosts)
        rdd = rdd.map(Client)
        print "Collect", rdd.collect()

    def GetUniqueHosts(self):

        rdd=self.sc.parallelize(range(1,10000))
        rdd=rdd.map(lambda v: (socket.gethostname(),v))\
               .reduceByKey(lambda a,b: a if a<b else b)
        return rdd.collect()

    def process(self):
        print "Process"


if __name__ == "__main__":
    conf = SparkConf().setAppName('Spark benchmark server')
    sc = SparkContext(conf=conf)


    rdd =  sc.parallelize(range(1, 10000))
    rdd =  rdd.map(Server)
    print "Collect", rdd.collect()



