# Usage example:
# >../traffic_gen simple.sc 11 >! simple.ev
# >../sim simple.ev 10
#

# create a network consisting of three nodes
# a new node joins the system each 1 sec on the average
events 3 1000 1 0 0 0 0

# wait 10 sec for the network to stabilize
wait 10000

# insert 5 documents in the network
events 5 1000 0 0 0 1 0

# generate a total of 10 events; each event is a document insertion
# with probability 0.5 and a document lookup with probability 0.5 
events 10 1000 0 0 0 10 10

# wait 1 sec to complete all operations 
wait 1000

# end the simulation
exit