net: 1024,Euclidean,random 100 100,Node,Koorde
event: 1,join,wellknown=1
events: 1023,100,50,linear,join,wellknown=1
observe: 100000,ChordObserver,reschedule=10000,type=Koorde,initnodes=1023
events: 100,110000,2000,constant,lookup,
simulator 200000 exit