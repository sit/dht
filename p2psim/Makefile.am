bin_PROGRAMS = p2psim
p2psim_SOURCES = main.C node.C network.C chord.C packet.C topology.C \
                 euclidean.C threaded.C protocol.C event.C eventqueue.C \
                 kademlia.C protocolfactory.C topologyfactory.C \
                 p2pevent.C netevent.C p2psim.C parse.C \
                 eventfactory.C simevent.C pastry.C chordfinger.C \
                 koorde.C anyready.C observeevent.C \
                 observerfactory.C chordobserver.C observer.C \
                 threadmanager.C vivalditest.C nodefactory.C \
                 vivaldi.C randomgraph.C dvgraph.C euclideangraph.C
p2psim_LDADD = -L/usr/local/lib -lthread -l9 -lfmt -lutf -lgmp -lcrypto
INCLUDES = -I/usr/local/include
CXXFLAGS = -g -Wall -fno-exceptions 

