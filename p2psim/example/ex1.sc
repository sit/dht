net: 1024,Euclidean,random 100 100,Node,Koorde
event: 1,join,wellknown=1
events: 1023,100,50,linear,join,wellknown=1
observe: 10000,ChordObserver,reschedule=10000,type=Koorde
events: 100,2000000,2000,constant,lookup,
simulator 2300000 exit