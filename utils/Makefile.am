## Process this file with automake to produce Makefile.in
## $Id: Makefile.am,v 1.15 2005/08/19 20:04:33 fdabek Exp $

chordlib_LIBRARIES = libutil.a libadb.a
chordinclude_HEADERS = id_utils.h configurator.h skiplist.h modlogger.h

noinst_HEADERS = \
	block_status.h \
	coord.h \
	ida.h \
	id_utils.h \
	location.h \
	locationtable.h \
	misc_utils.h \
	modlogger.h \
	ida-field.C \
	libadb.h \
	dbfe.h

libutil_a_SOURCES = \
	block_status.C \
	configurator.C \
	coord.C \
	ida.C \
	id_utils.C \
	location.C \
	locationtable.C \
	misc_utils.C \
	modlogger.C \
	libadb.C \
	dbfe.C

libadb_a_SOURCES = libadb.C

bin_PROGRAMS = adbd

adbd_SOURCES = adb.C 
adbd_LDADD =  libutil.a ../svc/libsvc.la $(LIBARPC)  $(LIBSFSCRYPT) $(LIBGMP) $(DBLIB) $(LIBASYNC) 

# should really figure out how to encode explict rule so that make
# will run ida-genfield.py to update ida-field.C if ida-genfield.py changes.


TESTS = test_ida test_skiplist test_locationtable test_adb
check_PROGRAMS = $(TESTS)

test_ida_SOURCES = test_ida.C
test_ida_LDADD = ./libutil.a $(LIBSFSCRYPT) $(LIBARPC) $(LIBASYNC) $(LIBGMP)

test_locationtable_SOURCES = test_locationtable.C
test_locationtable_LDADD = ./libutil.a $(LIBSFSCRYPT) $(LIBARPC) $(LIBASYNC) $(LIBGMP)

test_skiplist_SOURCES = test_skiplist.C

test_adb_SOURCES = test_adb.C
test_adb_LDADD = libadb.a ../svc/libsvc.la $(LIBARPC)  $(LIBSFSCRYPT) $(LIBGMP)  $(DBLIB) $(LIBASYNC) 

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
