#include <pthread.h>
#include <dirent.h> 
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h> 
#include <zlib.h>
 
#define BUFFER_SIZE 1048576 // 1MB
#define NUM_THREADS 2

int cmp(const void *a, const void *b) {
	return strcmp(*(char **) a, *(char **) b);
}

// struct to represent a compressed file in memory
typedef struct {
	int 	size; // size of data
	char* 	data; // ptr to char array of data
} compressed_file;

// struct to hold arguments for worker threads
typedef struct {
	int 	num_files;		// total number of files to compress
	int 	start_index;	// first file to compress
	int 	end_index;		// last file to compress
	char* 	directory_name; // paths that prepends all the filenames
	char** 	files;			// array of files to compress
	compressed_file* finished_files;	// array for the compressed data (all workers write to this)
} worker_args;

/// @brief A worker function that compresses a slice of files array and writes to the finished_files array.
/// @param thread_args must unpack to a worker_args struct
/// @return 
void* compress_slice(void* thread_args){
	// unpack thread arguments
	worker_args* args = (worker_args*)thread_args;
	int 	num_files 		= args->num_files;
	int 	start_index		= args->start_index;
	int 	end_index		= args->end_index;
	char* 	directory_name 	= args->directory_name;
	char** 	files 			= args->files;
	compressed_file* finished_files = args->finished_files;

	// for each file in the slice
	for(int index = start_index; index < end_index; index++){
		// break when no more files to compress
		if(index >= num_files){
			break;
		}
		printf("%i\n", index);

		// build the file path
		int len = strlen(directory_name)+strlen(files[index])+2;
		char *full_path = malloc(len*sizeof(char));
		assert(full_path != NULL);
		strcpy(full_path, directory_name);
		strcat(full_path, "/");
		strcat(full_path, files[index]);

		// load file
		unsigned char buffer_in[BUFFER_SIZE];
		FILE *f_in = fopen(full_path, "r");
		assert(f_in != NULL);
		int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
		fclose(f_in);
		
		// zip file
		unsigned char buffer_out[BUFFER_SIZE];
		z_stream strm;
		int ret = deflateInit(&strm, 9);
		assert(ret == Z_OK);
		strm.avail_in = nbytes;
		strm.next_in = buffer_in;
		strm.avail_out = BUFFER_SIZE;
		strm.next_out = buffer_out;
		
		ret = deflate(&strm, Z_FINISH);
		assert(ret == Z_STREAM_END);
		
		// write zipped data to results
		// no critical section needed as each thread only writes to the index it already got.
		int nbytes_zipped = BUFFER_SIZE-strm.avail_out;
		finished_files[index].size = nbytes_zipped;
		finished_files[index].data = malloc(nbytes_zipped);
		memcpy(finished_files[index].data, buffer_out, nbytes_zipped);	

		free(full_path);
	}
	return NULL;
}

int compress_directory(char *directory_name) {
	DIR *d;
	struct dirent *dir;
	char **files = NULL;
	int nfiles = 0;

	d = opendir(directory_name);
	if(d == NULL) {
		printf("An error has occurred\n");
		return 0;
	}

	// create sorted list of text files
	while ((dir = readdir(d)) != NULL) {
		files = realloc(files, (nfiles+1)*sizeof(char *));
		assert(files != NULL);

		int len = strlen(dir->d_name);
		if(dir->d_name[len-4] == '.' && dir->d_name[len-3] == 't' && dir->d_name[len-2] == 'x' && dir->d_name[len-1] == 't') {
			files[nfiles] = strdup(dir->d_name);
			assert(files[nfiles] != NULL);

			nfiles++;
		}
	}
	closedir(d);
	qsort(files, nfiles, sizeof(char *), cmp);

	// create a single zipped package with all text files in lexicographical order
	int total_in = 0, total_out = 0;
	FILE *f_out = fopen("text.tzip", "w");
	assert(f_out != NULL);

	// worker threads
	pthread_t workers[NUM_THREADS];
	worker_args thread_args[NUM_THREADS];
	pthread_mutex_t index_mutex = PTHREAD_MUTEX_INITIALIZER;

	// index all the threads will access for the next file to compress
	int shared_index = 0;

	// shared array of finished files for all the threads to write to
	compressed_file* finished_files = calloc(nfiles, sizeof(compressed_file));

	int slice_size = (nfiles) / NUM_THREADS;
	for(int i = 0; i < NUM_THREADS; i++){

		// pack thread arguments
		thread_args[i].num_files 		= nfiles;
		thread_args[i].start_index		= i*slice_size;
		thread_args[i].end_index		= (i+1)*slice_size;
		thread_args[i].directory_name 	= directory_name;
		thread_args[i].files 			= files;
		thread_args[i].finished_files	= finished_files;

		// spawn thread
		pthread_create(&workers[i], NULL, compress_slice, &thread_args[i]);
	}

	for(int i = 0; i < NUM_THREADS; i++){
		// wait for thread to complete
		pthread_join(workers[i], NULL);
	}

	// write all the data from finished_files to the outfile
	for(int i = 0; i < nfiles; i++){
		fwrite(&finished_files[i].size, sizeof(int), 1, f_out);
		fwrite(finished_files[i].data, sizeof(char), finished_files[i].size, f_out);
		free(finished_files[i].data);
	}
	fclose(f_out);
	free(finished_files);

	printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/total_in);

	// release list of files
	for(int i = 0; i < nfiles; i++)
		free(files[i]);
	free(files);

	// do not modify the main function after this point!
	return 0;
}
