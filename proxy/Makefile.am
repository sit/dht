## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.4 2003/08/14 18:10:08 fdabek Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

bin_PROGRAMS = proxy
proxy_SOURCES = proxy.C gateway.C sync.C
proxy_LDADD = ../svc/libsvc.la $(DBLIB) ${LDADD} 
noinst_headers = proxy.h

noinst_HEADERS = proxy.h

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

