if GTK_AVAILABLE
VIS = vis
endif

bin_PROGRAMS = p2psim $(VIS)
p2psim_SOURCES = main.C node.C network.C chord.C packet.C topology.C \
                 euclidean.C threaded.C protocol.C event.C eventqueue.C \
                 kademlia.C protocolfactory.C topologyfactory.C \
                 p2pevent.C netevent.C p2psim.C parse.C \
                 eventfactory.C simevent.C pastry.C chordfinger.C chordfingerpns.C\
                 koorde.C anyready.C observeevent.C \
                 observerfactory.C chordobserver.C observer.C \
		 kademliaobserver.C chordtoe.C \
                 threadmanager.C vivalditest.C \
                 vivaldi.C randomgraph.C dvgraph.C euclideangraph.C \
                 e2egraph.C g2graph.C threadspeedup.C dhtprotocol.C rpchandle.C
p2psim_LDADD = -L/usr/local/lib -lthread -l9 -lfmt -lutf -lgmp -lcrypto
CXXFLAGS = -g -Wall -fno-exceptions -fno-default-inline -fno-inline -O2
# CXXFLAGS = -g -Wall -fno-exceptions -O2

if GTK_AVAILABLE
vis_SOURCES = vis.C
GTKINCLUDES = $(GSTUFF_CFLAGS) 
vis_LDADD = $(GSTUFF_LIBS)
endif
INCLUDES = -I/usr/local/include $(GTKINCLUDES)
