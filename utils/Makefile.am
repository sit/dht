## Process this file with automake to produce Makefile.in
## $Id: Makefile.am,v 1.7 2003/05/06 17:39:14 fdabek Exp $

noinst_LIBRARIES = libutil.a

noinst_HEADERS = \
	configurator.h \
	coord.h \
	ida.h \
	id_utils.h \
	location.h \
	locationtable.h \
	misc_utils.h \
	modlogger.h \
	skiplist.h \
	ida-field.C

libutil_a_SOURCES = \
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


TESTS = test_ida test_skiplist
check_PROGRAMS = $(TESTS)

test_ida_SOURCES = test_ida.C
test_ida_LDADD = ./libutil.a $(LIBSFSCRYPT) $(LIBARPC) $(LIBASYNC) $(LIBGMP)

test_skiplist_SOURCES = test_skiplist.C

EXTRA_DIST = .cvsignore
CLEANFILES = core *.core *~ *.rpo
MAINTAINERCLEANFILES = Makefile.in
