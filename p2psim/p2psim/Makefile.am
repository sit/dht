bin_PROGRAMS = p2psim
p2psim_SOURCES = main.C
p2psim_LDADD = p2psim_core.o \
               ../events/events.o \
	       ../eventgenerators/eventgenerators.o \
	       ../failuremodels/failuremodels.o \
	       ../misc/misc.o \
	       ../observers/observers.o \
	       ../protocols/protocols.o \
	       ../topologies/topologies.o \
               -L/usr/local/lib -lthread -l9 -lfmt -lutf -lgmp -lcrypto

noinst_PROGRAMS = p2psim_core.o
p2psim_core_o_SOURCES = anyready.C condvar.C dhtprotocol.C event.C \
		        eventqueue.C \
		        eventgenerator.C \
		        eventqueueobserver.C \
		        network.C node.C observed.C \
		        p2psim.C packet.C parse.C protocol.C \
		        rpchandle.C threaded.C threadmanager.C \
		        threadspeedup.C tmgdmalloc.C topology.C \
		        oldobserver.C
p2psim_core_o_LIBS =
p2psim_core_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
