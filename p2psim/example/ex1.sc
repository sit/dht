net: 1024,Euclidean,random 100 100,Node,Koorde
event: 1,join,wellknown=1
events: 1023,100,50,linear,join,wellknown=1
observe: 1000,ChordObserver,reschedule=500,type=Koorde
events: 100,450000,2000,constant,lookup,
simulator 800000 exit