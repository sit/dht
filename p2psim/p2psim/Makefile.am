bin_PROGRAMS = p2psim
p2psim_SOURCES = main.C
p2psim_LDADD = -L. -L.. \
	       -L../events -levents \
	       -L../topologies -ltopologies \
	       -L../eventgenerators -leventgenerators \
	       -L../misc -lmisc \
	       -L/usr/local/lib -lthread -l9 -lfmt -lutf -lgmp -lcrypto \
	       -L../observers -lobservers \
	       -L../protocols -lprotocols \
	       -L../p2psim -lp2psim \
	       -L../protocols -lprotocols \
	       -L../p2psim -lp2psim \
	       -L../events -levents \
	       -L../misc -lmisc \
	       -L../eventgenerators -leventgenerators \
	       -L../topologies -ltopologies \
	       -L../observers -lobservers \
	       -L../p2psim -lp2psim \
	       -L../events -levents \
               -L../failuremodels -lfailuremodels

noinst_LIBRARIES = libp2psim.a
libp2psim_a_SOURCES = anyready.C condvar.C dhtprotocol.C event.C \
		      eventqueue.C \
		      eventgenerator.C \
		      eventqueueobserver.C \
		      network.C node.C observed.C \
		      p2psim.C packet.C parse.C protocol.C \
		      rpchandle.C threaded.C threadmanager.C \
		      threadspeedup.C tmgdmalloc.C topology.C \
		      oldobserver.C

include $(top_srcdir)/Makefile.incl
