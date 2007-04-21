## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.21 2005/08/24 01:07:59 sit Exp $

$(PROGRAMS): $(LDEPS)

RPCBUILT = dhblock_storage.h dhblock_storage.C 
 
SUFFIXES = .x .C .h
.x.h:
	-$(RPCC) -h $< || rm -f $@
.x.C:
	-$(RPCC) -c $< || rm -f $@
$(RPCC):
	@:

dhblock_storage.h: dhblock_storage.x
dhblock_storage.C: dhblock_storage.x
dhblock_storage.o: dhblock_storage.h

$(DEP_FILES): dhblock_storage.h

noinst_LIBRARIES = libdhash.a

chordlib_LIBRARIES = libdhashclient.a
chordinclude_HEADERS = \
		       dhashclient.h \
		       dhash.h \
		       dhash_common.h \
		       dhblock.h \
		       dhblock_chash.h \
		       dhblock_keyhash.h \
		       dhblock_replicated.h \
		       dhblock_noauth.h

libdhashclient_a_SOURCES = \
			   dhashclient.C \
			   dhblock.C \
			   dhblock_storage.C \
			   dhblock_chash.C \
			   dhblock_keyhash.C \
			   dhblock_replicated.C \
			   dhblock_noauth.C 

noinst_HEADERS = \
		 dhashcli.h \
		 dhashgateway.h \
		 dhash_impl.h \
		 dhash_store.h \
		 download.h \
		 dhblock.h \
		 dhblock_chash.h \
		 dhblock_keyhash.h \
		 dhblock_replicated.h \
		 dhblock_srv.h \
		 dhblock_chash_srv.h \
		 dhblock_keyhash_srv.h \
		 dhblock_replicated_srv.h \
		 dhblock_noauth_srv.h \
		 dhblock_storage.x


libdhash_a_SOURCES = \
		     client.C \
		     gateway.C \
		     download.C \
		     dhash_store.C \
		     server.C \
		     dhblock.C \
		     dhblock_storage.C \
		     dhblock_chash.C \
		     dhblock_keyhash.C \
		     dhblock_replicated.C \
		     dhblock_noauth.C \
		     dhblock_srv.C \
		     dhblock_chash_srv.C \
		     dhblock_replicated_srv.C \
		     dhblock_keyhash_srv.C \
		     dhblock_noauth_srv.C

dist-hook:
	cd $(distdir) && rm -f $(RPCBUILT)

.PHONY: rpcclean
rpcclean:
	rm -f $(RPCBUILT)

CLEANFILES = core *.core *~ *.rpo $(RPCBUILT)
MAINTAINERCLEANFILES = Makefile.in

