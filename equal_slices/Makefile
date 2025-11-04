tzip: main.c serial.c serial.h
	gcc main.c serial.c -lz -o tzip

test:
	rm -f text.tzip
	./tzip books
	./check.sh reference-books.tzip

test2:
	rm -f text.tzip
	./tzip books2
	./check.sh reference-books2.tzip

clean:
	rm -f tzip text.tzip
