# NOTE: the simulation may take up to 30 min depending on the machine

# create 1,000 nodes (a new node joins every second) ...
events 1000 1000 100 0 0 0 0

# ... wait 1 min for network to stabilize ... 
wait 60000

# ... insert 10,000 documents...
events 10000 1000 0 0 0 100 0 

# ... wait 1 minute for network to stabilize ...
wait 60000

# ... fail 500 nodes at the same time ...
events 500 0 0 0 100 0 0

# ... initiate 10,000 queries ...
events 10000 1000 0 0 0 0 100

# ... wait 1 min for outstanding queries to complete ...
wait 60000

# ... terminate
exit

    