## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.11 2001/07/05 15:35:21 kaashoek Exp $

$(PROGRAMS): $(LDEPS)

bin_PROGRAMS = lsd

# noinst_HEADERS =

lsd_SOURCES = lsd.C

# lsd_LDADD = ../dhash/libdhash.a ../svc/libsvc.la  $(DBLIB) ${LDADD} 
lsd_LDADD = ../chord/libchord.a ../svc/libsvc.la  $(DBLIB) ${LDADD} 


EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

