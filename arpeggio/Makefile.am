## Process this file with automake to produce Makefile.in
# $Id: Makefile.am,v 1.2 2004/08/20 22:51:50 amdragon Exp $

$(PROGRAMS): $(LDEPS) $(DBDEPS)

SUFFIXES = .x .C .h .py
.x.h:
	-$(RPCC) -h $< || rm -f $@
.x.C:
	-$(RPCC) -c $< || rm -f $@
.x.py:
	-$(RPCC) -python $< || rm -f $@
$(RPCC):
	@:

bin_PROGRAMS = cd testcd

cd_prot.h: cd_prot.x
cd_prot.C: cd_prot.x
cd_prot.o: cd_prot.h
cd_prot.py: cd_prot.x

$(DEP_FILES): cd_prot.h
noinst_HEADERS = cd_prot.x

cd_SOURCES = cd_prot.C cd.C

cd_LDADD = ../chord/libchord.a ../utils/libutil.a ../svc/libsvc.la \
	   $(DBLIB) ${LDADD} 

testcd: testcd.py cd_prot.py ${top_srcdir}/svc/chord_types.py
	cp $< $@

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in

