## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.13 2002/04/18 17:56:17 fdabek Exp $

$(PROGRAMS): $(LDEPS) $(DBLIB)

bin_PROGRAMS = lsd

# noinst_HEADERS =

lsd_SOURCES = lsd.C

# lsd_LDADD = ../dhash/libdhash.a ../svc/libsvc.la  $(DBLIB) ${LDADD} 
lsd_LDADD = ../chord/libchord.a  ../dhash/libdhash.a ../svc/libsvc.la \
	 $(DBLIB) ${LDADD} 


EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

