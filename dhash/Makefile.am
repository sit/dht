## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.18 2005/05/24 20:58:55 sit Exp $

$(PROGRAMS): $(LDEPS)

noinst_LIBRARIES = libdhash.a

chordlib_LIBRARIES = libdhashclient.a
chordinclude_HEADERS = \
		       dhashclient.h \
		       dhash.h \
		       dhash_common.h \
		       dhblock.h \
		       dhblock_chash.h \
		       dhblock_keyhash.h \
		       dhblock_replicated.h

libdhashclient_a_SOURCES = \
			   dhashclient.C \
			   dhblock.C \
			   dhblock_chash.C \
			   dhblock_keyhash.C \
			   dhblock_replicated.C

noinst_HEADERS = \
		 dhashcli.h \
		 dhashgateway.h \
		 dhash_impl.h \
		 dhash_store.h \
		 download.h \
		 pmaint.h \
		 succopt.h \
		 vsc.h \
		 dhblock.h \
		 dhblock_chash.h \
		 dhblock_keyhash.h \
		 dhblock_replicated.h \
		 dhblock_srv.h \
		 dhblock_chash_srv.h \
		 dhblock_keyhash_srv.h \
		 dhblock_replicated_srv.h \
		 dhblock_noauth_srv.h


libdhash_a_SOURCES = \
		     client.C \
		     gateway.C \
		     download.C \
		     dhash_store.C \
		     dhash_fetchrec.C \
		     pmaint.C \
		     server.C \
		     dhblock.C \
		     dhblock_chash.C \
		     dhblock_keyhash.C \
		     dhblock_replicated.C \
		     dhblock_srv.C \
		     dhblock_chash_srv.C \
		     dhblock_replicated_srv.C \
		     dhblock_keyhash_srv.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

