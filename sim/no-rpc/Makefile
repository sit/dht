CFLAGS =  -g -Wall
CC   = gcc
GET  = 
REL  = 
SRCS = event.c misc.c util.c node.c finger.c request.c sim.c in.c \
join.c stabilize.c leave.c doc.c
OBJS = event.o misc.o util.o node.o finger.o request.o sim.o in.o \
join.o stabilize.o leave.o doc.o

INCLUDE_FILES = def.h node.h event.h fun.h doc.h finger.h \
request.h

all: $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o sim -lm -ll
	$(CC) $(CFLAGS) traffic_gen.c util.c  -o traffic_gen -lm -ll
depend: 
	makedepend $(CFLAGS) $(SRCS)
clean: 
	rm *.o
sources: $(SRCS)

# DO NOT DELETE THIS LINE -- make depend depends on it.


doc.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
doc.o: /usr/include/alloca.h /usr/include/bits/endian.h
doc.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
doc.o: /usr/include/bits/select.h /usr/include/bits/sigset.h
doc.o: /usr/include/bits/stdio_lim.h /usr/include/bits/time.h
doc.o: /usr/include/bits/types.h /usr/include/bits/waitflags.h
doc.o: /usr/include/bits/waitstatus.h /usr/include/bits/wchar.h
doc.o: /usr/include/endian.h /usr/include/features.h /usr/include/_G_config.h
doc.o: /usr/include/gconv.h /usr/include/gnu/stubs.h /usr/include/libio.h
doc.o: /usr/include/stdio.h /usr/include/stdlib.h /usr/include/sys/cdefs.h
doc.o: /usr/include/sys/select.h /usr/include/sys/sysmacros.h
doc.o: /usr/include/sys/types.h /usr/include/time.h /usr/include/wchar.h
doc.o: /usr/include/xlocale.h
doc.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
doc.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
event.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
event.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
event.o: /usr/include/bits/stdio_lim.h /usr/include/bits/types.h
event.o: /usr/include/bits/wchar.h /usr/include/features.h
event.o: /usr/include/_G_config.h /usr/include/gconv.h
event.o: /usr/include/gnu/stubs.h /usr/include/libio.h /usr/include/malloc.h
event.o: /usr/include/stdio.h /usr/include/sys/cdefs.h /usr/include/wchar.h
event.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
event.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
finger.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
finger.o: /usr/include/bits/endian.h /usr/include/bits/huge_val.h
finger.o: /usr/include/bits/mathcalls.h /usr/include/bits/mathdef.h
finger.o: /usr/include/bits/nan.h /usr/include/bits/pthreadtypes.h
finger.o: /usr/include/bits/sched.h /usr/include/bits/stdio_lim.h
finger.o: /usr/include/bits/types.h /usr/include/bits/wchar.h
finger.o: /usr/include/endian.h /usr/include/features.h
finger.o: /usr/include/_G_config.h /usr/include/gconv.h
finger.o: /usr/include/gnu/stubs.h /usr/include/libio.h /usr/include/malloc.h
finger.o: /usr/include/math.h /usr/include/stdio.h /usr/include/sys/cdefs.h
finger.o: /usr/include/wchar.h
finger.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
finger.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
join.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
join.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
join.o: /usr/include/bits/stdio_lim.h /usr/include/bits/types.h
join.o: /usr/include/bits/wchar.h /usr/include/features.h
join.o: /usr/include/_G_config.h /usr/include/gconv.h /usr/include/gnu/stubs.h
join.o: /usr/include/libio.h /usr/include/stdio.h /usr/include/sys/cdefs.h
join.o: /usr/include/wchar.h
join.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
join.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
leave.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
leave.o: /usr/include/alloca.h /usr/include/bits/endian.h
leave.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
leave.o: /usr/include/bits/select.h /usr/include/bits/sigset.h
leave.o: /usr/include/bits/stdio_lim.h /usr/include/bits/time.h
leave.o: /usr/include/bits/types.h /usr/include/bits/waitflags.h
leave.o: /usr/include/bits/waitstatus.h /usr/include/bits/wchar.h
leave.o: /usr/include/endian.h /usr/include/features.h
leave.o: /usr/include/_G_config.h /usr/include/gconv.h
leave.o: /usr/include/gnu/stubs.h /usr/include/libio.h /usr/include/stdio.h
leave.o: /usr/include/stdlib.h /usr/include/sys/cdefs.h
leave.o: /usr/include/sys/select.h /usr/include/sys/sysmacros.h
leave.o: /usr/include/sys/types.h /usr/include/time.h /usr/include/wchar.h
leave.o: /usr/include/xlocale.h
leave.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
leave.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
misc.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
misc.o: /usr/include/alloca.h /usr/include/bits/endian.h
misc.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
misc.o: /usr/include/bits/select.h /usr/include/bits/sigset.h
misc.o: /usr/include/bits/time.h /usr/include/bits/types.h
misc.o: /usr/include/bits/waitflags.h /usr/include/bits/waitstatus.h
misc.o: /usr/include/endian.h /usr/include/features.h /usr/include/gnu/stubs.h
misc.o: /usr/include/stdlib.h /usr/include/sys/cdefs.h
misc.o: /usr/include/sys/select.h /usr/include/sys/sysmacros.h
misc.o: /usr/include/sys/types.h /usr/include/time.h /usr/include/xlocale.h
misc.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
node.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
node.o: /usr/include/bits/endian.h /usr/include/bits/huge_val.h
node.o: /usr/include/bits/mathcalls.h /usr/include/bits/mathdef.h
node.o: /usr/include/bits/nan.h /usr/include/bits/pthreadtypes.h
node.o: /usr/include/bits/sched.h /usr/include/bits/stdio_lim.h
node.o: /usr/include/bits/types.h /usr/include/bits/wchar.h
node.o: /usr/include/endian.h /usr/include/features.h /usr/include/_G_config.h
node.o: /usr/include/gconv.h /usr/include/gnu/stubs.h /usr/include/libio.h
node.o: /usr/include/malloc.h /usr/include/math.h /usr/include/stdio.h
node.o: /usr/include/sys/cdefs.h /usr/include/wchar.h
node.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
node.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
request.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
request.o: /usr/include/alloca.h /usr/include/bits/endian.h
request.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
request.o: /usr/include/bits/select.h /usr/include/bits/sigset.h
request.o: /usr/include/bits/stdio_lim.h /usr/include/bits/time.h
request.o: /usr/include/bits/types.h /usr/include/bits/waitflags.h
request.o: /usr/include/bits/waitstatus.h /usr/include/bits/wchar.h
request.o: /usr/include/endian.h /usr/include/features.h
request.o: /usr/include/_G_config.h /usr/include/gconv.h
request.o: /usr/include/gnu/stubs.h /usr/include/libio.h /usr/include/stdio.h
request.o: /usr/include/stdlib.h /usr/include/sys/cdefs.h
request.o: /usr/include/sys/select.h /usr/include/sys/sysmacros.h
request.o: /usr/include/sys/types.h /usr/include/time.h /usr/include/wchar.h
request.o: /usr/include/xlocale.h
request.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
request.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
sim.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
sim.o: /usr/include/alloca.h /usr/include/bits/endian.h
sim.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
sim.o: /usr/include/bits/select.h /usr/include/bits/sigset.h
sim.o: /usr/include/bits/stdio_lim.h /usr/include/bits/time.h
sim.o: /usr/include/bits/types.h /usr/include/bits/waitflags.h
sim.o: /usr/include/bits/waitstatus.h /usr/include/bits/wchar.h
sim.o: /usr/include/ctype.h /usr/include/endian.h /usr/include/features.h
sim.o: /usr/include/_G_config.h /usr/include/gconv.h /usr/include/gnu/stubs.h
sim.o: /usr/include/libio.h /usr/include/malloc.h /usr/include/stdio.h
sim.o: /usr/include/stdlib.h /usr/include/sys/cdefs.h
sim.o: /usr/include/sys/select.h /usr/include/sys/sysmacros.h
sim.o: /usr/include/sys/types.h /usr/include/time.h /usr/include/wchar.h
sim.o: /usr/include/xlocale.h
sim.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
sim.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
stabilize.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
stabilize.o: /usr/include/alloca.h /usr/include/bits/endian.h
stabilize.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
stabilize.o: /usr/include/bits/select.h /usr/include/bits/sigset.h
stabilize.o: /usr/include/bits/stdio_lim.h /usr/include/bits/time.h
stabilize.o: /usr/include/bits/types.h /usr/include/bits/waitflags.h
stabilize.o: /usr/include/bits/waitstatus.h /usr/include/bits/wchar.h
stabilize.o: /usr/include/endian.h /usr/include/features.h
stabilize.o: /usr/include/_G_config.h /usr/include/gconv.h
stabilize.o: /usr/include/gnu/stubs.h /usr/include/libio.h
stabilize.o: /usr/include/stdio.h /usr/include/stdlib.h
stabilize.o: /usr/include/sys/cdefs.h /usr/include/sys/select.h
stabilize.o: /usr/include/sys/sysmacros.h /usr/include/sys/types.h
stabilize.o: /usr/include/time.h /usr/include/wchar.h /usr/include/xlocale.h
stabilize.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
stabilize.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
util.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
util.o: /usr/include/alloca.h /usr/include/bits/endian.h
util.o: /usr/include/bits/huge_val.h /usr/include/bits/mathcalls.h
util.o: /usr/include/bits/mathdef.h /usr/include/bits/nan.h
util.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
util.o: /usr/include/bits/select.h /usr/include/bits/sigset.h
util.o: /usr/include/bits/stdio_lim.h /usr/include/bits/time.h
util.o: /usr/include/bits/types.h /usr/include/bits/waitflags.h
util.o: /usr/include/bits/waitstatus.h /usr/include/bits/wchar.h
util.o: /usr/include/endian.h /usr/include/features.h /usr/include/_G_config.h
util.o: /usr/include/gconv.h /usr/include/gnu/stubs.h /usr/include/libio.h
util.o: /usr/include/math.h /usr/include/stdio.h /usr/include/stdlib.h
util.o: /usr/include/sys/cdefs.h /usr/include/sys/select.h
util.o: /usr/include/sys/sysmacros.h /usr/include/sys/types.h
util.o: /usr/include/time.h /usr/include/wchar.h /usr/include/xlocale.h
util.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
util.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
