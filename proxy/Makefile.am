## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.2 2003/08/07 21:33:00 benjie Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = proxy
proxy_SOURCES = proxy.C gateway.C sync.C
proxy_LDADD = ../svc/libsvc.la $(DBLIB) ${LDADD} 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

