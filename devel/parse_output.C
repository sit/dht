#include <assert.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PDF 1
#define CDF 2
#define XY 3

float data[16000];
long num_elements;
int *pdf;
float *cdf;
int num_bins;
int col;

int
compar(const void *a, const void *b) 
{
  float x = *(float *)a;
  float y = *(float *)b;
  
  return ((x < y) ? -1 : 1);
}

void
get_data (FILE *in) 
{
  num_elements = 0;
  int parsed;
  float dummy1, dummy2;
  do {
    if (col == 0) 
      parsed = fscanf(in, "%f %f %f\n", 
		      &(data[num_elements++]), &dummy1, &dummy2);
    else if (col == 1)
      parsed = fscanf(in, "%f %f %f\n", 
		      &dummy1, &(data[num_elements++]), &dummy2);
    else
      parsed = fscanf(in, "%f %f %f\n", 
		      &dummy1, &dummy2, &(data[num_elements++]));
  } while (parsed == 3);
  num_elements--;
  
  qsort(data, num_elements, sizeof(float), compar);
}

long
array_max (float *d, int num) 
{
  float max = -RAND_MAX;
  for (int i = 0; i < num; i++) {
    if (d[i] > max) max = d[i];
  }
  return (long)max;
}

void
make_PDF (int bin_width) 
{
  long max = (long)array_max(data, num_elements);
  num_bins = max/bin_width + 1;
  pdf = (int *)malloc(sizeof(int)*num_bins);
  assert(pdf);
  bzero(pdf, sizeof(int)*num_bins);

  for (int i=0; i < num_elements; i++) {
    int bin = (int)(data[i]/bin_width);
    pdf[bin]++;
  }
}

void
make_CDF (int *pdf, int num_bins)
{
  cdf = (float *)malloc(sizeof(float)*num_bins);
  assert(cdf);
  bzero(cdf, sizeof(float)*num_bins);

  int running = 0;
  for (int i=0; i < num_bins; i++) {
    running += pdf[i];
    cdf[i] = running;
  }
  for (int i =0; i < num_bins; i++)
    cdf[i] /= running;
}

void
make_epts (int *pdf, float *cdf, int num_bins, int bin_width,
	   float *median, float *low_con, float *high_con) 
{
#define LOW_CON 0.05
#define HIGH_CON 0.95
  
  *median = data[num_elements/2];
#if 0
  int i=0;
  while (cdf[i] < LOW_CON) i++;
  *low_con = i*bin_width;
  i=num_bins - 1;
  while ((i > 0) && (cdf[i] > HIGH_CON)) i--;
  *high_con = i*bin_width;
#endif
  *low_con = data[(long)(num_elements*LOW_CON)];
  *high_con = data[(long)(num_elements*HIGH_CON)];
}


void
output_pdf (FILE *out, int *pdf, int num_bins, int bin_width) 
{
  fprintf(out, "newgraph\n");
  fprintf(out, "newcurve pts ");
  for (int i = 0; i < num_bins; i++) {
    fprintf(out, "%d %d ", i*bin_width, pdf[i]);
  }
  fprintf(out, " linetype solid\n");
}

void
output_cdf (FILE *out, float *cdf, int num_bins, int bin_width) 
{
  fprintf(out, "newgraph\n");
  fprintf(out, "newcurve pts ");
  for (int i = 0; i < num_bins; i++) {
    fprintf(out, "%d %f ", i*bin_width, cdf[i]);
  }
  fprintf(out, " linetype solid\n");
}

void
output_xy (FILE *out, int x, float med, float low, float high)
{
  fprintf(out, "%d %f %f %f ", x, med, low, high);
}

void
make_xy (FILE *input, FILE *output, int bin_width) 
{
  int parsed;
  int out_x[128];
  float out_med[128];
  float out_low[128];
  float out_high[128];
  int i = 0;
  
  do {
    int x_val;
    char filename[32];
    parsed = fscanf(input, "%d %s\n", &x_val, filename);
    printf("opening %s\n", filename);
    FILE *data_in = fopen(filename, "r");
    assert(data_in);
    get_data(data_in);
    make_PDF(bin_width);
    make_CDF(pdf, num_bins);

    make_epts (pdf, cdf, num_bins, bin_width,
	       &out_med[i], &out_low[i], &out_high[i]);
    out_x[i] = x_val;
    i++;
    delete cdf;
    delete pdf;
  } while (parsed == 2);

  fprintf(output, "newgraph\n");
  fprintf(output, "newcurve y_epts ");
  for (int j=0; j<i;j++) 
    output_xy(output, out_x[j], out_med[j], out_low[j], out_high[j]);
  fprintf(output, "linetype solid\n");

}

void
usage (char *prog) 
{
  printf("%s -[cpx] -i input [-o output]\n", prog);
}


int
main(int argc, char **argv) 
{

  FILE* input = NULL;
  FILE* output = stdout;
  int ch;
  int mode = PDF;
  int BIN_WIDTH = 10;

  col = 0;
  while ((ch = getopt (argc, argv, "cpxi:o:C:b:")) != -1)
    switch (ch) {
    case 'c':
      mode = CDF;
      break;
    case 'p':
      mode = PDF;
      break;
    case 'x':
      mode = XY;
      break;
    case 'b':
      BIN_WIDTH = atoi(optarg);
      break;
    case 'C':
      col = atoi(optarg);
      fprintf(stderr, "warning: using column %d\n", col);
      break;
    case 'i':
      input = fopen(optarg, "r");
      break;
    case 'o':
      output = fopen(optarg, "w");
      break;
    case '?':
    default:
      usage (argv[0]);
    }
  
  if (input == NULL) {
    fprintf(stderr, "%s: must specify input file\n", argv[0]);
    exit(1);
  }


  if (mode == PDF) {
    get_data(input);
    make_PDF(BIN_WIDTH);
    output_pdf(output, pdf, num_bins, BIN_WIDTH);
  } else if (mode == CDF) {
    get_data(input);
    make_PDF(BIN_WIDTH);
    make_CDF(pdf, num_bins);
    output_cdf(output, cdf, num_bins, BIN_WIDTH);
  } else {
    make_xy(input, output, BIN_WIDTH);
  }
}

