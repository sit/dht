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

void store_cb_pk (dhashclient dhash, bool error, ptr<insert_info> i);
void store_cb_ch (dhashclient dhash, bool error, ptr<insert_info> i);
void store_cb_noauth (dhashclient dhash, bool error, ptr<insert_info> i);
void store_cb_append (dhashclient dhash, bool error, ptr<insert_info> i);
void fetch_cb_append (dhashclient dhash, ptr<dhash_block> blk);
void store_cb_append_second (dhashclient dhash, bool error, 
			     ptr<insert_info> i);
void fetch_cb_append_second (dhashclient dhash, ptr<dhash_block> blk);

void fetch_cb (ptr<dhash_block> blk);

char *data_one = "This is some test data";
char *data_too = " so is this.";
char data[8192];

str control_socket;
unsigned int datasize;

#define CONTENT_HASH 1
#define PUB_KEY 2
#define APPEND 3
#define NOAUTH 4

void 
store_cb_ch (dhashclient dhash, bool error, ptr<insert_info> i)
{
  if (error)
    warn << "contenthash store error\n";
  else
    warn << "contenthash store success\n";

  dhash.retrieve (i->key, wrap (&fetch_cb));
}

void
store_cb_pk (dhashclient dhash, bool error, ptr<insert_info> i)
{
  if (error)
    warn << "pk store error\n";
  else
    warn << "pk store successful\n";

  dhash.retrieve (i->key, wrap (&fetch_cb));
}

void
store_cb_noauth (dhashclient dhash, bool error, ptr<insert_info> i)
{
  if (error)
    warn << "noauth store error " << i->key << "\n";
  else
    warn << "noauth store successful " << i->key << "\n";

  dhash.retrieve (i->key, wrap (&fetch_cb));
}

void
store_cb_append (dhashclient dhash, bool error, ptr<insert_info> i)
{
  if (error)
    warn << "store db error " << i->key << "\n";
  else
    warn << "store db successful " << i->key << "\n";
  
  dhash.retrieve (i->key, wrap (&fetch_cb_append, dhash));
}

void
fetch_cb_append (dhashclient dhash, ptr<dhash_block> blk) 
{
  if (!blk)
    fatal << "append: error\n";

  dhash.append (bigint (20), data_too, strlen (data_too), 
		wrap (&store_cb_append_second, dhash));
}

void
store_cb_append_second (dhashclient dhash, bool error, ptr<insert_info> i)
{
  if (error)
    warn << "append error " << i->key << "\n";

  dhash.retrieve (i->key, wrap (&fetch_cb_append_second, dhash));
}

void
fetch_cb_append_second (dhashclient dhash, ptr<dhash_block> blk) 
{
  if (!blk)
    fatal << "append (second): error\n";
  
  warn << "data (" << blk->len << " bytes) was " << blk->data << "\n";
  exit (0);
}

void
fetch_cb (ptr<dhash_block> blk)
{

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
  warn << progname << " sock randseed mode [mode options]\n";
  exit(0);
}

int
main (int argc, char **argv)
{
  setprogname (argv[0]);

  sfsconst_init ();
  
  if (argc < 3) usage (argv[0]);

  control_socket = argv[1];
  dhashclient dhash (control_socket);

  unsigned int seed = strtoul (argv[2], NULL, 10);
  srandom (seed);

  datasize = 8192;

  switch (atoi (argv[3])) {
  case CONTENT_HASH:
    {
    dhash.insert (data, datasize, wrap(&store_cb_ch, dhash));
    break;
    }
  case PUB_KEY:
    {
    assert (argc == 5 && "last argument is pubkey file");
    str key = file2wstr (argv[4]);
    ptr<rabin_priv> sk =  import_rabin_priv (key, NULL);
    dhash.insert (data, datasize, *sk, wrap (&store_cb_pk, dhash));
    break;
    }
  case NOAUTH:
    {
      /*    dhash.insert (bigint (10), 
		  data, 
		  strlen (data), 
		  wrap (&store_cb_noauth, dhash),
		  DHASH_NOAUTH); */
    }
    break;
  case APPEND:
    {
    dhash.append (bigint (20), data_one, strlen (data_one), 
		  wrap (&store_cb_append, dhash));
    }
    break;
  default:
    usage (argv[0]);
    break;
  }
	  
  amain ();
}
