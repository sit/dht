## Process this file with automake to produce Makefile.in
## $Id: Makefile.am,v 1.5 2003/04/10 20:28:08 sit Exp $

noinst_LIBRARIES = libutil.a

noinst_HEADERS = \
	coord.h \
	ida.h \
	id_utils.h \
	location.h \
	locationtable.C \
	misc_utils.C \
	modlogger.h \
	skiplist.h

libutil_a_SOURCES = \
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
