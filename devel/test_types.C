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

#include "sfsmisc.h"
#include "dhash_prot.h"
#include "dhash.h"
#include "crypt.h"
#include <sys/time.h>

void store_cb_pk (dhashclient dhash, bool error, chordID key);
void fetch_cb (ptr<dhash_block> blk);

char *data = "This is some test data";
str control_socket;
unsigned int datasize;

int out = 0;
int MAX_OPS_OUT = 1024;



void
store_cb_pk (dhashclient dhash, bool error, chordID key)
{
  if (error)
    warn << "pk store error\n";
  else
    warn << "pk store successful\n";

  dhash.retrieve (key, DHASH_KEYHASH, wrap (fetch_cb));
}

void
store_cb_noauth (dhashclient dhash, bool error, chordID key)
{
  if (error)
    warn << "noauth store error " << key << "\n";
  else
    warn << "noauth store successful " << key << "\n";

  dhash.retrieve (key, DHASH_NOAUTH, wrap (fetch_cb));
}


void
fetch_cb (ptr<dhash_block> blk)
{
  out--;

  if (!blk) {
    warn << "Error\n";
  }
  else if (datasize != blk->len || memcmp (data, blk->data, datasize) != 0)
    fatal << "verification failed";
  else 
    warn << "success\n";
  
  exit (0);
}


void
usage (char *progname) 
{
  warn << progname << " some args\n";
  exit(0);
}



int
main (int argc, char **argv)
{
  setprogname (argv[0]);

  sfsconst_init ();

  control_socket = argv[1];
  dhashclient dhash (control_socket);

  str key = file2wstr (argv[2]);
  ptr<rabin_priv> sk =  import_rabin_priv (key, NULL);

  unsigned int seed = strtoul (argv[3], NULL, 10);
  srandom (seed);

  datasize = strlen(data);
  //  warn << "Testing content hash:\n";
  //dhash.insert (data[i], 100, wrap(&store_cb));
  warn << "Testing pub key\n";
  //  dhash.insert (data, strlen(data), *sk, wrap (&store_cb_pk, dhash));
  dhash.insert (bigint (10), 
		data, 
		strlen (data), 
		wrap (&store_cb_noauth, dhash),
		DHASH_NOAUTH);
  amain ();
}
