protocol: Kademlia, param=1 anotherparam=5
     net: 1024, Euclidean, random 100 100
   event: 1, join, wellknown=1
  events: 1023, 100, 50, linear, join, wellknown=1
 observe: 1000, KademliaObserver, reschedule=500, type=Kademlia
 events: 100, 450000, 2000, constant, lookup

simulator 800000 exit
