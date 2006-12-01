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

#include <async.h>
#include <dhash_common.h>
#include <dhashclient.h>
#include <dhblock.h>
#include <sys/time.h>

static bigint *IDs;
static bool *done;
static void **data;
str control_socket;
static FILE *outfile;
static FILE *bwfile;
unsigned int datasize;

ptr<axprt_stream> xprt;
int fconnected = 0;
int out = 0;
int totalnum = 0;
int MAX_OPS_OUT = 1024;

int bps = 0;
int sec = 0;
timecb_t *measurer = NULL;

u_int64_t
getusec ()
{
  timeval tv;
  gettimeofday (&tv, NULL);
  return tv.tv_sec * INT64(1000000) + tv.tv_usec;
}

void
measure_bw (void)
{
  float bw = datasize * bps; // we get called every second
  bw /= 1024; // convert to K.
  sec++;
  unsigned long long usecs = (getusec ()/1000);
  fprintf (bwfile, "%llu\t%6.2f KB/s\n", usecs, bw);
  bps = 0;
  measurer = delaycb (1, wrap (&measure_bw));
}


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
  done = (bool *)malloc (sizeof (bool) * num);
  for (int i = 0; i < num; i++) {
    data[i] = malloc(datasize);
    IDs[i] = make_block(data[i], i);
    done[i] = false;
  }
}


void
store_cb (u_int64_t start, dhash_stat status, ptr<insert_info> i)
{
  out--;

  strbuf s;
  if (status != DHASH_OK) {
    s << "store_cb: " << i->key << " " << status << "\n";
  } else {
    bps++;
    s << (i->key>>144) << " / " << (getusec () - start)/1000 << " /";
    for (size_t j = 0; j < i->path.size (); j++)
      s << " " << (i->path[j]>>144);
    s << "\n";
  }
  str buf (s);
  fprintf (outfile, "%s", buf.cstr ());
  if (outfile != stdout)
    warnx << buf;
}


int
store (dhashclient *dhash, int num) 
{
  for (int i = 0; i < num; i++) {
    out++;
    dhash->insert ((char *)data[i], datasize, wrap (store_cb, getusec ()));
    while (out > MAX_OPS_OUT) 
      acheck ();
  }

  while (out > 0) 
    acheck ();

  return 0;
}



void
fetch_cb (int i, struct timeval start, dhash_stat stat, ptr<dhash_block> blk, vec<chordID> path)
{
  out--;
  done[i] = true;

  if (!blk) {
    strbuf buf;
    buf << "Error: " << IDs[i] << "\n";
    fprintf (outfile, str (buf).cstr ());
  }
  else if (datasize != blk->data.len () || memcmp (data[i], blk->data.cstr (), datasize) != 0)
    fatal << "verification failed for block " << IDs[i];
  else {
    struct timeval end;
    gettimeofday(&end, NULL);
    strbuf buf;
    float elapsed = (end.tv_sec - start.tv_sec)*1000.0 + (end.tv_usec - start.tv_usec)/1000.0;
    char estr[128];
    sprintf (estr, "%f", elapsed);

    bps++;
    buf << (IDs[i]>>144) << " " << estr;
    buf << " /";
    for (u_int i = 0; i < blk->times.size (); i++)
      buf << " " << blk->times[i];
    buf << " /";

    buf << " " << blk->hops << " " <<  blk->errors
	<< " " << blk->retries << " ";
    for (u_int i=0; i < path.size (); i++) {
      buf << (path[i]>>144) << " ";
    }
    
    buf << "\n";
    fprintf (outfile, str (buf).cstr ());
    if (outfile != stdout)
      warnx << buf;
  } 
}


void
fetch (dhashclient &dhash, int num)
{
  for (int i = 0; i < num; i++) {
    out++;
    struct timeval start;
    gettimeofday (&start, NULL);

    dhash.retrieve (IDs[i], DHASH_CONTENTHASH, wrap (fetch_cb, i, start));
    while (out > MAX_OPS_OUT)
      acheck ();
  }

  while (out > 0) 
    acheck();
}

void
usage (char *progname) 
{
  warn << "vnode_num control_socket num_trials data_size file <f or s> nops seed [bw-file]\n";
  exit(0);
}

void
cleanup (void)
{
  for (int i = 0; i < totalnum; i++) {
    if (!done[i]) 
      warn << (IDs[i]>>144) << " not done " << "\n";
  }
  if (outfile) {
    fclose (outfile);
  }
  if (bwfile) {
    fclose (bwfile);
  }
  exit (1);
}

void
eofhandler () 
{
  warn << "Unexpected EOF: block too large?\n";
  cleanup ();
}

void
connected (dhashclient *dhash, int argc, char **argv) 
{
  dhash->seteofcb (wrap (eofhandler));

  fconnected = 1;
  int num = atoi(argv[3]);
  totalnum = num;
  datasize = atoi(argv[4]);

  char *output = argv[5];
  if (strcmp(output, "-") == 0)
    outfile = stdout;
  else
    outfile = fopen(output, "w");

  char *bwoutput;
  if (argc > 9) {
    bwoutput = argv[9];
    if (strcmp (bwoutput, "-") == 0)
      bwfile = stdout;
    else
      bwfile = fopen (bwoutput, "w");

    fflush (bwfile);
    measurer = delaycb (1, wrap (&measure_bw));
  }

  if (!outfile) {
    printf ("could not open %s\n", output);
    exit(1);
  }

  unsigned int seed = strtoul (argv[8], NULL, 10);
  srandom (seed);
  prepare_test_data (num);

  MAX_OPS_OUT = atoi(argv[7]);

  struct timeval start;
  gettimeofday (&start, NULL);

  if (argv[6][0] == 's')
    store (dhash, num);
  else
    fetch (*dhash, num);
  
  struct timeval end;
  gettimeofday (&end, NULL);
  float elapsed = (end.tv_sec - start.tv_sec)*1000.0 + (end.tv_usec - start.tv_usec)/1000.0;
  fprintf(outfile, "Total Elapsed: %f\n", elapsed);
  
  if (bwfile && measurer) {
    timecb_remove (measurer);
    measurer = NULL;
    measure_bw ();
    fclose (bwfile);
  }

  fclose (outfile);

  delete dhash;
}

void
tcp_connect_cb (int argc, char **argv, int fd)
{
  if (fd < 0) 
    fatal << "connect failed\n";
  warnx << "... connected!\n";
  xprt = axprt_stream::alloc (fd);    
  dhashclient *dhash = New dhashclient (xprt);
  connected (dhash, argc, argv);
}

int
main (int argc, char **argv)
{
  setprogname (argv[0]);

  if (argc < 9) {
    usage (argv[0]);
    exit (1);
  }
  sigcb (SIGTERM, wrap (&cleanup));
  sigcb (SIGINT, wrap (&cleanup));

  control_socket = argv[2];
  char *cstr = (char *)control_socket.cstr ();
  if (strchr (cstr, ':')) {
    char *port = strchr (cstr, ':');
    *port = 0; //isolate host
    port++; // point at port
    char *host = cstr;
    short i_port = atoi (port);
    warn << "Connecting to " << host << ":" << i_port << " via TCP...";
    tcpconnect (host, i_port, wrap (&tcp_connect_cb, argc, argv));
    while (!fconnected) acheck ();
  } else {
    dhashclient *dhash = New dhashclient (control_socket);
    connected (dhash, argc, argv);
  }
}


