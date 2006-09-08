## Process this file with automake to produce Makefile.in
## $Id: Makefile.am,v 1.21 2006/09/08 19:40:37 sit Exp $

chordlib_LIBRARIES = libutil.a libadb.a
chordinclude_HEADERS = id_utils.h configurator.h skiplist.h modlogger.h

noinst_HEADERS = \
	coord.h \
	ida.h \
	id_utils.h \
	keyauxdb.h \
	location.h \
	locationtable.h \
	misc_utils.h \
	modlogger.h \
	ida-field.C \
	libadb.h \
	dbfe.h

libutil_a_SOURCES = \
	configurator.C \
	coord.C \
	ida.C \
	id_utils.C \
	keyauxdb.C \
	location.C \
	locationtable.C \
	misc_utils.C \
	modlogger.C \
	libadb.C \
	dbfe_sleepycat.C \
	dbfe.C

libadb_a_SOURCES = libadb.C

bin_PROGRAMS = adbd
noinst_PROGRAMS = adb_benchmark

adbd_SOURCES = adbd.C 
adbd_LDADD =  libutil.a ../svc/libsvc.la $(LIBARPC)  $(LIBSFSCRYPT) $(LIBGMP) $(DBLIB) $(LIBASYNC) 

adb_benchmark_SOURCES = adb_benchmark.C
adb_benchmark_LDADD =  libutil.a ../svc/libsvc.la $(LIBARPC)  $(LIBSFSCRYPT) $(LIBGMP) $(DBLIB) $(LIBASYNC) 

# should really figure out how to encode explict rule so that make
# will run ida-genfield.py to update ida-field.C if ida-genfield.py changes.


TESTS = test_ida test_skiplist test_locationtable test_adb test_keyauxdb
check_PROGRAMS = $(TESTS)

test_keyauxdb_SOURCES = test_keyauxdb.C
test_keyauxdb_LDADD = keyauxdb.o $(LIBSFSCRYPT) $(LIBASYNC) $(LIBGMP)

test_ida_SOURCES = test_ida.C
test_ida_LDADD = ./libutil.a $(LIBSFSCRYPT) $(LIBARPC) $(LIBASYNC) $(LIBGMP)

test_locationtable_SOURCES = test_locationtable.C
test_locationtable_LDADD = ./libutil.a $(LIBSFSCRYPT) $(LIBARPC) $(LIBASYNC) $(LIBGMP)

test_skiplist_SOURCES = test_skiplist.C

test_adb_SOURCES = test_adb.C
test_adb_LDADD = libadb.a libutil.a ../svc/libsvc.la $(LIBARPC) $(LIBSFSCRYPT) $(LIBGMP) $(DBLIB) $(LIBASYNC) 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
