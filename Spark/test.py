


#1000001137
#1000004123
#1000004207
#1000004233
#1000004249
#10000001251



def naive_prime_test(Number):
	   i=2	
           while i < Number:
                if Number % i == 0:
                    return i
		i=i+1

           return i

print "Starting...."
print "i:", naive_prime_test(1000004233)
print "Finished"


