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
	       ../libtask/libtask.o \
               -lcrypto $(GBLIBS)

EXTRA_DIST = args.h condvar.h event.h eventgenerator.h eventqueue.h \
             eventqueueobserver.h failuremodel.h network.h node.h observed.h \
             observer.h observerinfo.h p2protocol.h p2psim.h p2psim_hashmap.h \
             packet.h parse.h rpchandle.h threaded.h threadmanager.h tmgdmalloc.h \
             topology.h skiplist.h keyfunc.h bighashmap_arena.hh \
	     bighashmap.cc bighashmap.hh

noinst_PROGRAMS = p2psim_core.o
p2psim_core_o_SOURCES = condvar.C p2protocol.C event.C \
		        eventqueue.C \
		        eventgenerator.C \
		        eventqueueobserver.C \
		        network.C node.C observed.C \
		        p2psim.C packet.C parse.C \
		        rpchandle.C threaded.C threadmanager.C \
		        tmgdmalloc.C topology.C \
			bighashmap_arena.cc

p2psim_core_o_LIBS = 
p2psim_core_o_LINK = $(LD) -r -o $@

include $(top_srcdir)/Makefile.incl
