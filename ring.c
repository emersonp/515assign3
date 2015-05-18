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

  //for (int index = 0; index < nprocs; index++ ) {
  if (rank == 0) {
    MPI_Send(&N, 1, MPI_INT, 1, TAG, MPI_COMM_WORLD);
    printf("P0 sent %d to P1\n", N);
    MPI_Recv(&N, 1, MPI_INT, MPI_ANY_SOURCE, TAG, MPI_COMM_WORLD, &status);
    printf("P0 received %d\n", N);
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
