%.o: %.c
	gcc -I. -Wall -c $*.c

%.o: %.s
	gcc -Wall -c $*.s

OFILES=\
	386.o\
	386label.o\
	channel.o\
	emalloc.o\
	idle.o\
	print.o\
	q.o\
	sched.o\

all libtask.a: $(OFILES)
	ar rvu libtask.a $(OFILES)
	ranlib libtask.a

clean:
	rm -f *.o

nuke:
	rm -f *.o libtask.a

tprimes: tprimes.o libtask.a
	$(CC) -o tprimes tprimes.o libtask.a
