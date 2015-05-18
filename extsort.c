//------------------------------------------------------------------------- 
// Copyright (c) 2015 Parker Harris Emerson, using supporting software for
// CS415/515 Parallel Programming, copyright (c) Portland State University
//------------------------------------------------------------------------- 

// A ring program to explore MPI.
//
// - Process 0 sends an integer to process 1.
// - Process 1 decreases the integer's value by one, and sends it to the 
//   next Process in the ring.
// - The last Process in the ring passes the final number to Process 0.
//
// Usage: 
//   linux> mpirun -hostflie <hostfile> -n <#processes> simple [<N>]
// 
// 
#define _BSD_SOURCE
#include <unistd.h>	// for gethostname()
#include <stdlib.h>
#include <stdio.h>
#include <mpi.h>

#define TAG 1001
#define MINSIZE   10

// Swap two array elements 
//
void swap(int *array, int i, int j) {
  if (i == j) return;
  int tmp = array[i];
  array[i] = array[j];
  array[j] = tmp;
}

// Bubble sort for the base cases
//
void bubblesort(int *array, int low, int high) {
  if (low >= high) 
    return;
  for (int i = low; i <= high; i++)
    for (int j = i+1; j <= high; j++) 
      if (array[i] > array[j])
	swap(array, i, j);
}

// Pick an arbitrary element as pivot. Rearrange array 
// elements into [smaller one, pivot, larger ones].
// Return pivot's index.
//
int partition(int *array, int low, int high) {
  int pivot = array[high]; 	// use highest element as pivot 
  int middle = low;
  for(int i = low; i < high; i++)
    if(array[i] < pivot) {
      swap(array, i, middle);
      middle++;
    }
  swap(array, high, middle);
  return middle;
}
 
// QuickSort an array range
// 
void quicksort(int *array, int low, int high) {
  if (high - low < MINSIZE) {
    bubblesort(array, low, high);
    return;
  }
  int middle = partition(array, low, high);
  if (low < middle)
    quicksort(array, low, middle-1);
  if (middle < high)
    quicksort(array, middle+1, high);
}

// Main
//
int main(int argc, char *argv[])
{
  double start_time = MPI_Wtime();
  int index;
  int nprocs, rank;
  int *pivots, *bucket_cap;
  int *buffer;
  int buffer_size;
  int BUCKET_TAG = 0, BUCKET_SIZE_TAG = 1, WRITE_QUEUE_TAG = 2;
  char host[20];
  MPI_Status status;
  int N = 10;		// default integer value
  char* file_input = "./sorting_array";
  char* file_output = "./output_array";
  
  MPI_File fh_input;
  MPI_File fh_output;
  MPI_Status file_status;
  MPI_Offset file_size, offset;
  
  if (argc == 2) {
    file_input = argv[1];	// overwrite the value
  }

  gethostname(host, 20);

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  if (nprocs < 2) {
    printf("Need at least 2 processes.\n");
    MPI_Finalize();
    return(1);
  }
  printf("P%d/%d started on %s ...\n", rank, nprocs, host);

  //for (int index = 0; index < nprocs; index++ ) {
  if (rank == 0) {
    // Open and read file.
    printf("Opening file: %s\n", file_input);
    MPI_File_open(MPI_COMM_SELF, file_input, MPI_MODE_RDONLY,
                  MPI_INFO_NULL, &fh_input);

    MPI_File_get_size(fh_input, &file_size);
    printf("File size: %d\n", file_size);

    buffer_size = file_size / sizeof(int);
    buffer = (int *) malloc(buffer_size * sizeof(int));
    printf("Reading file.\n");
    MPI_File_read(fh_input, buffer, buffer_size, MPI_INTEGER, &file_status);
    MPI_File_close(&fh_input);
    
    // Establish buckets.
    printf("Sorting beginning of file.\n");
    quicksort(buffer, 0, nprocs * 10 - 1);
    pivots = (int *) malloc((nprocs - 1) * sizeof(int));
    bucket_cap = (int *) malloc(nprocs * sizeof(int));
    for (index = 0; index < nprocs - 1; index++) {
      pivots[index] = buffer[(index + 1) * 10];
    }
    int *buckets[nprocs];
    for (index = 0; index < nprocs; index++) {
      buckets[index] = (int *) malloc(buffer_size * sizeof(int));
    }
    int bucket_index;

    for (index = 0; index < nprocs; index++) {
      bucket_cap[index] = 0;
    }
    for (int buffer_index = 0; buffer_index < buffer_size; buffer_index++) {
      for (int pivot_index = 0; pivot_index < nprocs; pivot_index++) {
        if (pivot_index < nprocs - 1) {
          if (buffer[buffer_index] < pivots[pivot_index]) {
            buckets[pivot_index][bucket_cap[pivot_index]++] = buffer[buffer_index];
            break;
          }
        } else {
          if (buffer[buffer_index] < pivots[nprocs - 1]) {
            buckets[nprocs - 1][bucket_cap[nprocs - 1]++] = buffer[buffer_index];
          }
        }
      }
    }

    for (int i = 0; i < nprocs; i++) {
      printf("Bucket Cap: %d\n", bucket_cap[i]);
    }

    // Send Buckets
    for (int send_index = 1; send_index < nprocs; send_index++) {
      printf("P0 sending bucket count to P%d.\n", send_index);
      MPI_Send(&bucket_cap[send_index], 1, MPI_INT, send_index, BUCKET_SIZE_TAG, MPI_COMM_WORLD);
      printf("P0 awaiting response from P%d.\n", send_index);
      printf("P0 sending bucket of size %d to P%d.\n", bucket_cap[send_index], send_index);
      MPI_Send(buckets[send_index], bucket_cap[send_index], MPI_INT, send_index, BUCKET_TAG, MPI_COMM_WORLD);
    }

    // Quicksort own business.
    quicksort(buckets[0], 0, bucket_cap[0] - 1);
    printf("P%d completed quicksort.\n", rank);
    

    // Write
    printf("P%d opening and writing file.\n", rank);
    MPI_File_open(MPI_COMM_SELF, file_output, MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh_output);
    MPI_File_write(fh_output, buckets[0], bucket_cap[0], MPI_INT, &status);
    MPI_File_close(&fh_output);
    printf("File closed in P%d.\n", rank);
    MPI_Send(&N, 1, MPI_INT, 1, WRITE_QUEUE_TAG, MPI_COMM_WORLD);
    
  }
  else {
    printf("P%d awaiting bucket size.\n", rank);
    int bucket_size;
    MPI_Recv(&bucket_size, 1, MPI_INT, 0, BUCKET_SIZE_TAG, MPI_COMM_WORLD, &status);
    printf("P%d received, confirming, and mallocing bucket size.\n", rank);
    //MPI_Send(&N, 1, MPI_INT, 0, TAG, MPI_COMM_WORLD);
    int *bucket = (int *)malloc(bucket_size * sizeof(int));
    MPI_Recv(bucket, bucket_size, MPI_INT, 0, BUCKET_TAG, MPI_COMM_WORLD, &status);
    printf("P%d received bucket.\n", rank);
    quicksort(bucket, 0, bucket_size - 1);
    printf("P%d completed quicksort.\n", rank);

    // Write
    MPI_Recv(&N, 1, MPI_INT, rank - 1, WRITE_QUEUE_TAG, MPI_COMM_WORLD, &status);
    printf("P%d opening and writing file.\n", rank);
    MPI_File_open(MPI_COMM_SELF, file_output, MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &fh_output);
    MPI_File_seek(fh_output, 0, MPI_SEEK_END);
    MPI_File_write(fh_output, bucket, bucket_size, MPI_INT, &status);
    MPI_File_close(&fh_output);
    printf("File closed in P%d.\n", rank);
    if (rank < nprocs - 1) {
      MPI_Send(&N, 1, MPI_INT, rank + 1, WRITE_QUEUE_TAG, MPI_COMM_WORLD);
    } else { // Last process in loop.
      double end_time = MPI_Wtime();
      printf("Start time: %f\n", start_time);
      printf("End time: %f\n", end_time);
      printf("Total time: %f\n", end_time - start_time);
    }
  }


  MPI_Finalize();
}
