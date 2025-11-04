#include <stdio.h>
#include <time.h>
#include <assert.h>

#include "serial.h"

// main function: do not modify!
int main(int argc, char **argv) {
	// time computation header
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);
	// end of time computation header

	assert(argc == 2);
	compress_directory(argv[1]);

	// time computation footer
	clock_gettime(CLOCK_MONOTONIC, &end);
	printf("Time: %.2f seconds\n", ((double)end.tv_sec+1.0e-9*end.tv_nsec)-((double)start.tv_sec+1.0e-9*start.tv_nsec));
	// end of time computation footer
    return 0;
}