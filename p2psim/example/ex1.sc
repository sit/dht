# OBSOLETE.  IGNORE THIS.
protocol: ChordFinger, base=2 successors=5 stabtimer=1000 timeout=3000
     net: 512, Euclidean, random 100 100
   event: 1, join, wellknown=1
  events: 511, 100, 50, linear, join, wellknown=1
 observe: 1000, reschedule=50000, numnodes=512, initnodes=0
 events: 1000, 450000, 2000, constant, lookup

simulator 800000 exit
