net: 256,Euclidean,random 100 100,Node,Koorde
event: 1,join,wellknown=1
events: 255,100,50,linear,join,wellknown=1
observe: 1000,ChordObserver,reschedule=500,type=Koorde
events: 49,50000,2000,constant,lookup,
simulator 200000 exit