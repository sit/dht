## Process this file with automake to produce Makefile.in
## $Id: Makefile.am,v 1.12 2005/05/24 00:38:15 strib Exp $

chordlib_LIBRARIES = libutil.a
chordinclude_HEADERS = id_utils.h configurator.h skiplist.h

noinst_HEADERS = \
	block_status.h \
	coord.h \
	ida.h \
	id_utils.h \
	location.h \
	locationtable.h \
	misc_utils.h \
	modlogger.h \
	ida-field.C

libutil_a_SOURCES = \
	block_status.C \
	configurator.C \
	coord.C \
	ida.C \
	id_utils.C \
	location.C \
	locationtable.C \
	misc_utils.C \
	modlogger.C 


# should really figure out how to encode explict rule so that make
# will run ida-genfield.py to update ida-field.C if ida-genfield.py changes.


TESTS = test_ida test_skiplist test_locationtable
check_PROGRAMS = $(TESTS)

test_ida_SOURCES = test_ida.C
test_ida_LDADD = ./libutil.a $(LIBSFSCRYPT) $(LIBARPC) $(LIBASYNC) $(LIBGMP)

test_locationtable_SOURCES = test_locationtable.C
test_locationtable_LDADD = ./libutil.a $(LIBSFSCRYPT) $(LIBARPC) $(LIBASYNC) $(LIBGMP)

test_skiplist_SOURCES = test_skiplist.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
