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
  int nprocs, rank;
  int *buffer;
  int buffer_size;
  char host[20];
  MPI_Status status;
  int N = 10;		// default integer value
  char* file_input = "./sorting_array";
  
  MPI_File file_handle;
  MPI_Status file_status;
  MPI_Offset file_size;
  
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
    printf("Opening file: '%s\n'", file_input);
    MPI_File_open(MPI_COMM_SELF, file_input, MPI_MODE_RDWR,
                  MPI_INFO_NULL, &file_handle);

    MPI_File_get_size(file_handle, &file_size);
    printf("File size: %d\n", file_size);

    buffer_size = file_size / sizeof(int);
    buffer = (int *) malloc(buffer_size * sizeof(int));
    printf("Reading file.\n");
    MPI_File_read(file_handle, buffer, buffer_size, MPI_INTEGER, &file_status);
    
    printf("Sorting file.\n");
    quicksort(buffer, 0, buffer_size - 1);
    for (int i = 0; i < buffer_size; i++) {
      printf("Num: %d\n", buffer[i]);
    }

    printf("Writing file.\n");
    MPI_File_seek(file_handle, 0, MPI_SEEK_SET); 
    MPI_File_write(file_handle, buffer, buffer_size, MPI_INTEGER, &file_status);

    MPI_Send(&N, 1, MPI_INT, 1, TAG, MPI_COMM_WORLD);
    printf("P0 sent %d to P1\n", N);
    MPI_Recv(&N, 1, MPI_INT, MPI_ANY_SOURCE, TAG, MPI_COMM_WORLD, &status);
    printf("P0 received %d\n", N);
    MPI_File_close(&file_handle);
  }
  else if (rank == (nprocs - 1)) {
    MPI_Recv(&N, 1, MPI_INT, rank-1, TAG, MPI_COMM_WORLD, &status);
    printf("P%d received %d\n", rank, N);
    N--;
    MPI_Send(&N, 1, MPI_INT, 0, TAG, MPI_COMM_WORLD);
    printf("P%d sent %d to P0\n", rank, N);
  }
  else {
    MPI_Recv(&N, 1, MPI_INT, rank-1, TAG, MPI_COMM_WORLD, &status);
    printf("P%d received %d\n", rank, N);
    N--;
    MPI_Send(&N, 1, MPI_INT, rank+1, TAG, MPI_COMM_WORLD);
    printf("P%d sent %d to P0\n", rank, N);
  }

  MPI_Finalize();
}
