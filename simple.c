//------------------------------------------------------------------------- 
// This is supporting software for CS415/515 Parallel Programming.
// Copyright (c) Portland State University
//------------------------------------------------------------------------- 

// A simple demo program of MPI send-receive routines.
//
// - Process 0 sends an integer to process 1.
// - Process 1 decreases the integer's value by one, and sends it back
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

int main(int argc, char *argv[])
{
  int nprocs, rank;
  char host[20];
  MPI_Status status;
  int N = 10;		// default integer value
  
  if (argc == 2) {
    N = atoi(argv[1]);	// overwirte the value
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

  if (rank == 0) {
    MPI_Send(&N, 1, MPI_INT, 1, TAG, MPI_COMM_WORLD);
    printf("P0 sent %d to P1\n", N);
    MPI_Recv(&N, 1, MPI_INT, MPI_ANY_SOURCE, TAG, MPI_COMM_WORLD, &status);
    printf("P0 recieved %d\n", N);
  } 
  else if (rank == 1) {
    MPI_Recv(&N, 1, MPI_INT, rank-1, TAG, MPI_COMM_WORLD, &status);
    printf("P1 recieved %d\n", N);
    N--;
    MPI_Send(&N, 1, MPI_INT, 0, TAG, MPI_COMM_WORLD);
    printf("P1 sent %d to P0\n", N);
  } else {
    // if there are more than 2 processes, others don't do anything
  }

  MPI_Finalize();
}
