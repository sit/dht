#!/usr/bin/env python
import random, sys

if len(sys.argv) < 5:
  print "Usage:\n\tTHIS <N> <end> <mean of alive time> <query interval>"

n = int(sys.argv[1])
end = int(sys.argv[2])
livemu = float(sys.argv[3])
qt = int(sys.argv[4])

gen = random.Random()

print "node 1 1 join wellknown=1"
print "observe %d numnodes=%d reschedule=0 initnodes=%d" % (n, n, n)

for node in range(2,n+1):
  joincrash = 1;
  time = gen.uniform(0,n)
  while (time < end):
    nexttime = time + int(gen.expovariate(1.0/livemu)) 
    if (joincrash):
      joincrash = 0
      print "node %d %d join wellknown=1" % (time, node)
      while ((time+qt) < nexttime):
        time += qt;
	key1 = long(gen.random() * 4294967295L)
	key2 = long(gen.random() * 4294967295L)
	print "node %d %d lookup key=0x%X%X" % (time, node, key1, key2)
    else:
      print "node %d %d crash" % (time, node)
      joincrash = 1

    time = nexttime

print "simulator %d exit" % (end)
