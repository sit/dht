CFLAGS =  -g 
CC   = gcc
OBJS = successor.o successor_misc.o docs.o util.o

successor:	$(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o successor -lm
depend: 
	makedepend $(CFLAGS) $(SRCS)
clean: 
	rm *.o

# DO NOT DELETE


