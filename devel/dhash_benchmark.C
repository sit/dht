/*
 *
 * Copyright (C) 2001  Frank Dabek (fdabek@lcs.mit.edu), 
 *   		       Massachusetts Institute of Technology
 * 
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

#include <sfsmisc.h>
#include <dhash_common.h>
#include <dhash_prot.h>
#include <dhashclient.h>
#include <verify.h>
#include <dbfe.h>
#include <crypt.h>
#include <sys/time.h>

static bigint *IDs;
static void **data;
str control_socket;
static FILE *outfile;
unsigned int datasize;

int out = 0;
int MAX_OPS_OUT = 1024;



chordID
make_block (void *data, int g) 
{
  // size must be word sized
  //  assert (datasize % sizeof (long) == 0);
  
  char *rd = (char *)data;
  for (unsigned int i = 0; i < datasize; i++) 
        rd[i] = random();
    //rd[i] = (char)('a' + g);
  rd[datasize - 1] = 0;

  return compute_hash (rd, datasize);
}

void
prepare_test_data (int num) 
{
  IDs = New chordID[num];
  data = (void **)malloc(sizeof(void *)*num);
  for (int i = 0; i < num; i++) {
    data[i] = malloc(datasize);
    IDs[i] = make_block(data[i], i);
  }
}


void
store_cb (dhash_stat status, ptr<insert_info> i)
{
  out--;

  if (status != DHASH_OK) {
    warn << "store_cb: " << i->key << " " << dhasherr2str(status) << "\n";
    fprintf (outfile, "store error\n");
  } else {
    str buf = strbuf () << "stored " << i->key << " at " << i->destID << "\n";
    fprintf (outfile, "%s", buf.cstr ());
  }
}


int
store (dhashclient &dhash, int num) 
{
  for (int i = 0; i < num; i++) {
    out++;
    dhash.insert ((char *)data[i], datasize, wrap (store_cb));
    while (out > MAX_OPS_OUT) 
      acheck ();
  }

  while (out > 0) 
    acheck ();

  return 0;
}



void
fetch_cb (int i, struct timeval start, dhash_stat stat, ptr<dhash_block> blk, route path)
{
  out--;

  if (!blk) {
    strbuf buf;
    buf << "Error: " << IDs[i] << "\n";
    fprintf (outfile, str (buf).cstr ());
  }
  else if (datasize != blk->len || memcmp (data[i], blk->data, datasize) != 0)
    fatal << "verification failed";
  else {
    struct timeval end;
    gettimeofday(&end, NULL);
    float elapsed = (end.tv_sec - start.tv_sec)*1000.0 + (end.tv_usec - start.tv_usec)/1000.0;
    char estr[128];
    sprintf (estr, "%f", elapsed);
    warnx << IDs[i] << " " << estr << " " << blk->hops << " " <<  blk->errors << " " << blk->retries << " ";
    for (u_int i=0; i < path.size (); i++) {
      warnx << path[i] << " ";
    }
    warnx << "\n";
  } 
}


void
fetch (dhashclient &dhash, int num)
{
  for (int i = 0; i < num; i++) {
    out++;
    struct timeval start;
    gettimeofday (&start, NULL);

    dhash.retrieve (IDs[i], wrap (fetch_cb, i, start));
    while (out > MAX_OPS_OUT)
      acheck ();
  }

  while (out > 0) 
    acheck();
}

void
usage (char *progname) 
{
  warn << "vnode_num control_socket num_trials data_size file <f or s> nops seed\n";
  exit(0);
}



int
main (int argc, char **argv)
{
  setprogname (argv[0]);

  if (argc < 9) {
    usage (argv[0]);
    exit (1);
  }

  control_socket = argv[2];
  dhashclient dhash (control_socket);

  int num = atoi(argv[3]);
  datasize = atoi(argv[4]);

  char *output = argv[5];
  if (strcmp(output, "-") == 0)
    outfile = stdout;
  else
    outfile = fopen(output, "w");

  if (!outfile) {
    printf ("could not open %s\n", output);
    exit(1);
  }

  //  int i = atoi(argv[1]);
  //  bool err = dhash.sync_setactive (i);
  //  assert (!err);

  unsigned int seed = strtoul (argv[8], NULL, 10);
  srandom (seed);
  prepare_test_data (num);

  MAX_OPS_OUT = atoi(argv[7]);

  struct timeval start;
  gettimeofday (&start, NULL);

  if (argv[6][0] == 's')
    store (dhash, num);
  else
    fetch (dhash, num);
  
  struct timeval end;
  gettimeofday (&end, NULL);
  float elapsed = (end.tv_sec - start.tv_sec)*1000.0 + (end.tv_usec - start.tv_usec)/1000.0;
  fprintf(outfile, "Total Elapsed: %f\n", elapsed);
}
