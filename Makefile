all:
	gcc -g -std=c99 -o oss oss.c
	gcc -g -std=c99 -o worker worker.c

clean:
	rm -f oss worker *.o