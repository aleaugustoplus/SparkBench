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
import threading
import time
import random


SERVER_PORT=20005
SERVER_BUFFER=5000
SIZE_OF_PARALLELIZE=5
CLIENTS_WAITING=15



hosts=['nml-cloud-215.cs.sfu.ca', 'nml-cloud-235.cs.sfu.ca',
       'nml-cloud-233.cs.sfu.ca','nml-cloud-193.cs.sfu.ca',
       'nml-cloud-210.cs.sfu.ca', 'nml-cloud-152.cs.sfu.ca']
#hosts=['ale-ultra']




class HWBench(Benchmark):



    def __init__(self, sc, Name, Num_Exec=1):
        super(HWBench,self).__init__(Name, Num_Exec)
        self.sc = sc


def EchoServer(v):


    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (socket.gethostbyname(socket.gethostname()), SERVER_PORT)
    try:
        sock.bind(server_address)
        sock.listen(1)
    except :
        print 'Closing server'
        return "A. O."

    while True:
        # Wait for a connection
        print  'starting up on %s port %s' % server_address
        print 'waiting for a connection'
        connection, client_address = sock.accept()

        try:
            while True:
                data = connection.recv(SERVER_BUFFER)
                #print 'received "%s"' % data
                if data:
                    #print 'sending data back to the client'
                    connection.sendall(data)
                    if 'exit' in data:
                        print "Server Leaving..."
                        return "OutSuccess"
                else:
                    print 'no more data from', client_address
                    break

        finally:
            # Clean up the connection
            connection.close()

def Client(origin, dest, Filesize, Exit=False):

    message="""        recv_into(buffer, [nbytes[, flags]]) -> nbytes_read              A version of recv() that stores its data into a buffer rather than creating         a new string.  Receive up to buffersize bytes from the socket.  If buffersize         is not specified (or 0), receive up to the size available in the given buffer.              recv() for documentation about the flags.                recv_into(buffer, [nbytes[, flags]]) -> nbytes_read              A version of recv() that stores its data into a buffer rather than creating         a new string.  Receive up to buffersize bytes from the socket.  If buffersize         is not specified (or 0), receive up to the size available in the given buffer.              recv() for documentation about the flags.                recv_into(buffer, [nbytes[, flags]]) -> nbytes_read              A version of recv() that stores its data into a buffer rather than creating         a new string.  Receive up to buffersize bytes from the socket.  If buffersize         is not specified (or 0), receive up to the size available in the given buffer.              recv() for documentation about the flags.                recv_into(buffer, [nbytes[, flags]]) -> nbytes_read              A version of recv() that stores its data into a buffer rather than creating         a new string.  Receive up to buffersize bytes from the socket.  If buffersize         is not specified (or 0), receive up to the size available in the given buffer.              recv() for documentation about the flags.                recv_into(buffer, [nbytes[, flags]]) -> nbytes_read              A version of recv() that stores its data into a buffer rather than creating         a new string.  Receive up to buffersize bytes from the socket.  If buffersize         is not specified (or 0), receive up to the size available in the given buffer.              recv() for documentation about the flags.                recv_into(buffer, [nbytes[, flags]]) -> nbytes_read              A version of recv() that stores its data into a buffer"""
    if origin!=dest or origin=='ale-ultra':
        server_address = (dest, SERVER_PORT)
        print 'connecting to %s port %s' % server_address

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address)
        print 'Client Connected'

        try:
            times=[]
            Results={}
            start = time.time()

            for i in range(Filesize):
                # Send data

                sock.sendall(message)

                # Look for the response
                amount_received = 0
                amount_expected = len(message)

                while amount_received < amount_expected:
                    data = sock.recv(SERVER_BUFFER)
                    amount_received += len(data)


            end = time.time()
            times.append(end - start)
            Results["time"] = (float(sum(times)) / float(len(times)))/2.0

            if Exit:
                sock.sendall("exit")
                sock.recv(SERVER_BUFFER)

            return Results

        finally:
            print >> sys.stderr, 'closing socket'
            sock.close()


class HWNetwork(HWBench):

    Hosts=[]

    def __init__(self, sc, Num_Exec=1):
        super(HWNetwork, self).__init__(sc, "Network", Num_Exec)
        self.Hosts = self.GetUniqueHosts()

    def GetUniqueHosts(self):

        rdd=self.sc.parallelize(range(1,SIZE_OF_PARALLELIZE))
        rdd=rdd.map(lambda v: (socket.gethostname(),v))\
               .reduceByKey(lambda a,b: a if a<b else b)
        return rdd.collect()

    def StartServers(self):
        # Starts
        rdd = self.sc.parallelize(range(1, SIZE_OF_PARALLELIZE))
        rdd = rdd.map(EchoServer)
        def run():
            print "Servers Collect", rdd.collect()
        threading.Thread(target=run).start()

    def process(self):
        Hosts = self.Hosts
        random.shuffle(Hosts)

        self.StartServers()
        CrossHosts = []

        if len(Hosts) > 1:
            TmpHosts = Hosts[len(Hosts) / 2:len(Hosts)]
            for i in range(0, len(Hosts) / 2):
                CrossHosts.append((Hosts[i], TmpHosts[0]))
                TmpHosts.remove(TmpHosts[0])
        else:
            CrossHosts.append((Hosts[0][0], Hosts[0][0]))

        print "Waiting to start"
        time.sleep(CLIENTS_WAITING)
        print "Starting clients...: ", CrossHosts

        rddHosts = self.sc.parallelize(CrossHosts)
        rdd1 = rddHosts.map(lambda (o, d): Client(o,d,  1))
        print "Client Collect 1:", rdd1.collect()
        rdd1 = rddHosts.map(lambda (o, d): Client(o, d, 1024/2, True))
        print "Client Collect MB:", rdd1.collect()
        rdd1 = rddHosts.map(lambda (o, d): Client(o, d, 1024*1024/2))
        print "Client Collect GB", rdd1.collect()
        rdd1 = rddHosts.map(lambda (o, d): Client(o, d, 1024*1024*5/2, True))
        print "Client Collect 5GB", rdd1.collect()



if __name__ == "__main__":
    conf = SparkConf().setAppName('Spark benchmark server')
    sc = SparkContext(conf=conf)


    rdd =  sc.parallelize(range(1, 10000))
    rdd =  rdd.map(Server)
    print "Collect", rdd.collect()



