## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.1 2003/08/07 05:12:28 benjie Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = proxy
proxy_SOURCES = proxy.C gateway.C
proxy_LDADD = ../svc/libsvc.la ${LDADD} 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

