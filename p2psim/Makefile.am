bin_PROGRAMS = p2psim
p2psim_SOURCES = main.C node.C network.C chord.C packet.C topology.C \
                 euclidean.C threaded.C protocol.C event.C eventqueue.C \
                 kademlia.C protocolfactory.C topologyfactory.C \
                 p2pevent.C netevent.C cbevent.C p2psim.C parse.C \
                 eventfactory.C simevent.C pastry.C chordsucclist.C \
                 chordsucclistfinger.C koorde.C 
p2psim_LDADD = -L/usr/local/lib -lthread -l9 -lfmt -lutf -lcrypto -lssl 
INCLUDES = -I/usr/local/include
