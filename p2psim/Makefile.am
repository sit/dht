bin_PROGRAMS = p2psim
p2psim_SOURCES = main.C node.C network.C chord.C packet.C topology.C \
                 euclidian.C threaded.C protocol.C event.C eventqueue.C \
                 kademlia.C protocolfactory.C topologyfactory.C \
                 p2pevent.C netevent.C
p2psim_LDADD = -L/usr/local/lib -lthread -l9 -lfmt -lutf

INCLUDES = -I/usr/local/include
