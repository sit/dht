## Process this file with automake to produce Makefile.in

$(PROGRAMS): $(LDEPS) $(DBDEPS)

SUFFIXES = .T .C .h
.T.C:
	-$(TAME) -o $@ $< || rm -f $@

bin_PROGRAMS = maintd
noinst_HEADERS = maint_policy.h 

# libchord is only needed to link libmerkle code
# that refers to user_args.
maintd_SOURCES = maint_sync.T maint_global.T maint_policy.T maintd.C 
maintd_LDADD = ../dhash/libdhash.a \
	../merkle/libmerkle.a \
	../chord/libchord.a \
	../utils/libutil.a \
	../svc/libsvc.la  \
	$(DBLIB) ${LDADD} 

TAMEOUT = maint_sync.C maint_global.C maint_policy.C

CLEANFILES = core *.core *~ *.rpo $(TAMEOUT)
MAINTAINERCLEANFILES = Makefile.in
