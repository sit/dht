# Kelips test file

protocol: Kelips, k=10
net: 100, Euclidean, random 100 100
event: 1, join, wellknown=1
events: 99, 100, 50, linear, join, wellknown=1
eventat: 30000, 23, lookup, key=17 # 23
eventat: 32000, 24, lookup, key=22 # 34
eventat: 34000, 25, lookup, key=25 # 37
eventat: 36000, 26, lookup, key=6A # 106
eventat: 38000, 27, lookup, key=96 # 150
simulator 100000 exit
