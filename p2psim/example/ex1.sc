net: 1024,Euclidean,random 100 100,Node,ChordFinger
event: 1,join,wellknown=1
events: 1023,100,50,linear,join,wellknown=1
observe: 5000,ChordObserver,reschedule=5000,type=ChordFinger
events: 100,450000,2000,constant,lookup,
simulator 800000 exit
