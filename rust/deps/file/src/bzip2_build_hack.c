#include <stdio.h>
#include <stdlib.h>

// this symbol is missing otherwise. not sure it matters. implement it here.
void bz_internal_error ( int errcode ) {
  fprintf(stderr, "bz_internal_error %d\n", errcode);
  exit(1);
}
