net: 50,Euclidean,random 100 100,Node,Koorde
event: 1,0,wellknown=1
events: 49,100,50,linear,0,wellknown=1
observe 1000 ChordObserver reschedule=500 type=Koorde numnodes=50
events: 49,50000,100,linear,4,
simulator 100000 exit