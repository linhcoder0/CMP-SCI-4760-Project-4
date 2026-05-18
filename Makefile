CC = gcc
CFLAGS = -g -std=c99

TARGETS = oss worker

all: $(TARGETS)

oss: oss.o
	$(CC) $(CFLAGS) -o oss oss.o

worker: worker.o
	$(CC) $(CFLAGS) -o worker worker.o

.c.o:
	$(CC) $(CFLAGS) -c $<

clean:
	rm -f oss worker *.o