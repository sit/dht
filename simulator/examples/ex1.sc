# NOTE: the simulation may take up to 30 min depending on the machine

# create a network with 1,000 nodes; a new node joins every 
# second on average ...
events 1000 1000 100 0 0 0 0 

# ... wait 1 min for network to stabilize ...
wait 60000

# ... insert 1,000 documents; one document is inserted every 
# 100 ms on average ...
events 1000 100 0 0 0 100 0 

# ... wait another minute for network to stabilize ...
wait 60000

# ... generate 10,000 events; one event every 714 ms on average
# - a join event is generated with prob 10/(10+10+20+100) = 1/14
# - a leave event is generated with prob 1/14
# - a document insertion is generated with prob 1/7
# - a document query is generated with prob 5/7
# Note: constants are choosen such that a node joins (leaves) 
# every second on average
events 10000 714 10 10 0 20 100

# ... wait for all pending requests to finish ...
wait 60000

# ... terminate
exit

