CC := gcc --std=c99 -c -Wall -Wextra
LD := gcc --std=c99

all: zenodotus

zenodotus: main.o
	$(LD) -o zenodotus main.o -lsqlite3

main.o: main.c
	$(CC) main.c

clean:
	rm -f main.o zenodotus
