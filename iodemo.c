//------------------------------------------------------------------------- 
// This is supporting software for CS415/515 Parallel Programming.
// Copyright (c) Portland State University
//------------------------------------------------------------------------- 

// A demo program of MPI concurrent I/O.
//
// - All processes open the same file.
// - Each computes an offset within the file where its output will go.
// - Each writes its rank into the file.
// - All processes close the file.
//
// Usage:
//   linux> mpirun  -hostflie <hostfile> -n <nprocs> iodemo <outfile>
//   linux> od -i <outfile>     -- to display the content of a binary file as integers
//
#define _BSD_SOURCE
#include <unistd.h>	// for gethostname()
#include <stdio.h>
#include <mpi.h>

int main(int argc, char *argv[])
{
  int nprocs, rank, offset;
  char host[20];
  MPI_Status status;
  MPI_File out = NULL;

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  gethostname(host, 20);
  printf("P%d/%d started on %s ...\n", rank, nprocs, host);

  MPI_File_open(MPI_COMM_WORLD, argv[1], MPI_MODE_CREATE|MPI_MODE_WRONLY, MPI_INFO_NULL, &out);
  offset = rank * 4;
  MPI_File_set_view(out, offset, MPI_INT, MPI_INT, "native", MPI_INFO_NULL);
  MPI_File_write(out, &rank, 1, MPI_INT, &status);
  printf("P%d wrote %d at offset %d in the output file\n", rank, rank, offset);
  MPI_File_close(&out);
  MPI_Finalize();
}
