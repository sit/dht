CFLAGS =  -g
CC   = gcc
GET  = 
REL  = 
SRCS = event.c misc.c util.c node.c finger.c request.c sim.c in.c \
join.c stabilize.c leave.c doc.c
OBJS = event.o misc.o util.o node.o finger.o request.o sim.o in.o \
join.o stabilize.o leave.o doc.o

INCLUDE_FILES = def.h node.h event.h event_types.h fun.h doc.h finger.h \
request.h

sim: $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o sim -lm -ll
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
doc.o: /usr/include/bits/types.h /usr/include/bits/waitflags.h
doc.o: /usr/include/bits/waitstatus.h /usr/include/endian.h
doc.o: /usr/include/features.h /usr/include/gnu/stubs.h /usr/include/stdlib.h
doc.o: /usr/include/sys/cdefs.h /usr/include/sys/select.h
doc.o: /usr/include/sys/sysmacros.h /usr/include/sys/types.h
doc.o: /usr/include/time.h /usr/include/xlocale.h
doc.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
doc.o: var.h
event.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
event.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
event.o: /usr/include/bits/stdio_lim.h /usr/include/bits/types.h
event.o: /usr/include/bits/wchar.h /usr/include/features.h
event.o: /usr/include/_G_config.h /usr/include/gconv.h
event.o: /usr/include/gnu/stubs.h /usr/include/libio.h /usr/include/malloc.h
event.o: /usr/include/stdio.h /usr/include/sys/cdefs.h /usr/include/wchar.h
event.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
event.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
event.o: var.h
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
finger.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/float.h
finger.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
finger.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
finger.o: var.h
in.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
in.o: /usr/include/bits/endian.h /usr/include/bits/pthreadtypes.h
in.o: /usr/include/bits/sched.h /usr/include/bits/stdio_lim.h
in.o: /usr/include/bits/types.h /usr/include/bits/wchar.h /usr/include/ctype.h
in.o: /usr/include/endian.h /usr/include/features.h /usr/include/_G_config.h
in.o: /usr/include/gconv.h /usr/include/gnu/stubs.h /usr/include/libio.h
in.o: /usr/include/malloc.h /usr/include/stdio.h /usr/include/sys/cdefs.h
in.o: /usr/include/wchar.h /usr/include/xlocale.h
in.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
in.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
in.o: var.h
join.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h var.h
leave.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
leave.o: /usr/include/alloca.h /usr/include/bits/endian.h
leave.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
leave.o: /usr/include/bits/select.h /usr/include/bits/sigset.h
leave.o: /usr/include/bits/types.h /usr/include/bits/waitflags.h
leave.o: /usr/include/bits/waitstatus.h /usr/include/endian.h
leave.o: /usr/include/features.h /usr/include/gnu/stubs.h
leave.o: /usr/include/stdlib.h /usr/include/sys/cdefs.h
leave.o: /usr/include/sys/select.h /usr/include/sys/sysmacros.h
leave.o: /usr/include/sys/types.h /usr/include/time.h /usr/include/xlocale.h
leave.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
leave.o: var.h
misc.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h var.h
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
node.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/float.h
node.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
node.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
node.o: var.h
request.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
request.o: /usr/include/alloca.h /usr/include/bits/endian.h
request.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
request.o: /usr/include/bits/select.h /usr/include/bits/sigset.h
request.o: /usr/include/bits/stdio_lim.h /usr/include/bits/types.h
request.o: /usr/include/bits/waitflags.h /usr/include/bits/waitstatus.h
request.o: /usr/include/bits/wchar.h /usr/include/endian.h
request.o: /usr/include/features.h /usr/include/_G_config.h
request.o: /usr/include/gconv.h /usr/include/gnu/stubs.h /usr/include/libio.h
request.o: /usr/include/stdio.h /usr/include/stdlib.h /usr/include/sys/cdefs.h
request.o: /usr/include/sys/select.h /usr/include/sys/sysmacros.h
request.o: /usr/include/sys/types.h /usr/include/time.h /usr/include/wchar.h
request.o: /usr/include/xlocale.h
request.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
request.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
request.o: var.h
sim.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
sim.o: /usr/include/alloca.h /usr/include/bits/endian.h
sim.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
sim.o: /usr/include/bits/select.h /usr/include/bits/sigset.h
sim.o: /usr/include/bits/stdio_lim.h /usr/include/bits/types.h
sim.o: /usr/include/bits/waitflags.h /usr/include/bits/waitstatus.h
sim.o: /usr/include/bits/wchar.h /usr/include/ctype.h /usr/include/endian.h
sim.o: /usr/include/features.h /usr/include/_G_config.h /usr/include/gconv.h
sim.o: /usr/include/gnu/stubs.h /usr/include/libio.h /usr/include/malloc.h
sim.o: /usr/include/stdio.h /usr/include/stdlib.h /usr/include/sys/cdefs.h
sim.o: /usr/include/sys/select.h /usr/include/sys/sysmacros.h
sim.o: /usr/include/sys/types.h /usr/include/time.h /usr/include/wchar.h
sim.o: /usr/include/xlocale.h
sim.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
sim.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
sim.o: var.h
stabilize.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
stabilize.o: /usr/include/alloca.h /usr/include/bits/endian.h
stabilize.o: /usr/include/bits/pthreadtypes.h /usr/include/bits/sched.h
stabilize.o: /usr/include/bits/select.h /usr/include/bits/sigset.h
stabilize.o: /usr/include/bits/types.h /usr/include/bits/waitflags.h
stabilize.o: /usr/include/bits/waitstatus.h /usr/include/endian.h
stabilize.o: /usr/include/features.h /usr/include/gnu/stubs.h
stabilize.o: /usr/include/stdlib.h /usr/include/sys/cdefs.h
stabilize.o: /usr/include/sys/select.h /usr/include/sys/sysmacros.h
stabilize.o: /usr/include/sys/types.h /usr/include/time.h
stabilize.o: /usr/include/xlocale.h
stabilize.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
stabilize.o: var.h
util.o: def.h doc.h event.h finger.h fun.h incl.h node.h request.h
util.o: /usr/include/bits/endian.h /usr/include/bits/huge_val.h
util.o: /usr/include/bits/mathcalls.h /usr/include/bits/mathdef.h
util.o: /usr/include/bits/nan.h /usr/include/bits/pthreadtypes.h
util.o: /usr/include/bits/sched.h /usr/include/bits/stdio_lim.h
util.o: /usr/include/bits/types.h /usr/include/bits/wchar.h
util.o: /usr/include/endian.h /usr/include/features.h /usr/include/_G_config.h
util.o: /usr/include/gconv.h /usr/include/gnu/stubs.h /usr/include/libio.h
util.o: /usr/include/malloc.h /usr/include/math.h /usr/include/stdio.h
util.o: /usr/include/sys/cdefs.h /usr/include/wchar.h
util.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/float.h
util.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stdarg.h
util.o: /usr/sww/pkg/gcc-2.95.2/lib/gcc-lib/i686-pc-linux-gnu/2.95.2/include/stddef.h
util.o: var.h
