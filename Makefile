CC := gcc --std=c99 -c -Wall -Wextra -O2
LD := gcc --std=c99

all: zenodotus

zenodotus: main.o
	$(LD) -o zenodotus main.o -lsqlite3

main.o: main.c
	$(CC) main.c

.PHONY: clean
clean:
	rm -f main.o zenodotus
