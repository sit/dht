#include <stdio.h>
#include <stdlib.h>
#include <math.h>

/* expected input data format                 */
/*                                            */
/* one columns file                           */             
/*                                            */

/* output mean and standard deviation         */
/*                                            */

int main(int argc, char **argv) {
  FILE *df;
  int   num = 0, i;
  float val;
  double std = 0., mean = 0., min = 1.e+300, max = 1.e-300;
  
  if (argc != 2) {
    printf("Ussage: %s src\n", argv[0]);
    exit(-1);
  }

  if (!(df = fopen (argv[1], "r"))) {
    printf ("fopen() failed\n");
    exit (1);
  }

  /* count the number of elements and compute the mean */
  while (!feof(df)) {

    fscanf(df, "%f", &val);
    num++;
    mean += val;
  }

  num--;
  mean -= val;
  mean /= num;

  fclose(df);

  /* open the file again for computing standard deviation */
  df = fopen (argv[1], "r");

  for (i = 0; i < num; i++) {

    fscanf(df, "%f", &val);
    if (max < val)
      max = val;
    if (min > val)
      min = val;
    std += (val - mean)*(val - mean)/(float)(num - 1);
  }

  std = sqrt(std); /* /(num-1)); */

  printf("%d %f %f %f %f\n", num, mean, std, min, max);
  printf("%f, %f\n", mean - 2.2622*std/sqrt(num),
	 mean + 2.2622*std/sqrt(num));

  fclose(df);
}

  

