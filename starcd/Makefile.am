## Process this file with automake to produce Makefile.in

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = starcd starfs_new

noinst_HEADERS = \
starcd.h quorum.h

starcd_SOURCES = \
server.C starcd.C quorum_read.C quorum_write.C

starfs_new_SOURCES = \
starfs_new.C quorum_read.C quorum_write.C

# XXX - this should not be required!
MYLDADD = ../dhash/libdhash.a ../merkle/libmerkle.a ../chord/libchord.a \
         ../svc/libsvc.la ../sfsrodb/libsfsrodb.a $(DBLIB) $(LDADD)
starcd_LDADD = $(MYLDADD)
starfs_new_LDADD = $(MYLDADD)

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in


