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
#include "dhash_common.h"
#include "dhash.h"
#include "dhashclient.h"
#include <sfscrypt.h>
#include <sys/time.h>

void store_cb_pk (dhashclient dhash, dhash_stat status, ptr<insert_info> i);
void store_cb_ch (dhashclient dhash, dhash_stat status, ptr<insert_info> i);
void store_cb_noauth (dhashclient dhash, dhash_stat status, ptr<insert_info> i);
void store_cb_append (dhashclient dhash, dhash_stat status, ptr<insert_info> i);
void fetch_cb_append (dhashclient dhash, dhash_stat stat, ptr<dhash_block> blk, vec<chordID> path);
void store_cb_append_second (dhashclient dhash, dhash_stat status, 
			     ptr<insert_info> i);
void fetch_cb_append_second (dhashclient dhash, dhash_stat stat, ptr<dhash_block> blk, vec<chordID> path);

void fetch_cb (dhash_stat stat, ptr<dhash_block> blk, vec<chordID> path);
void store_cb_ch2 (dhashclient dhash, chordID pred, 
		   dhash_stat status, ptr<insert_info> i);
char *data_one = "This is some test data";
char *data_too = " so is this.";
char data[8192];
bool useGuess = false;

str control_socket;
unsigned int datasize;

#define CONTENT_HASH 1
#define PUB_KEY 2
#define APPEND 3
#define NOAUTH 4

void 
store_cb_ch (dhashclient dhash, dhash_stat status, ptr<insert_info> i)
{
  if (status != DHASH_OK)
    warn << "contenthash store error\n";
  else
    warn << "contenthash store success\n";

  ptr<option_block> options;
  if (useGuess) {
    options = New refcounted<option_block> ();
    options->flags = DHASHCLIENT_GUESS_SUPPLIED;
    warn << "options->flags " << options->flags << "\n";
    chordID dest = i->path.pop_back ();
    chordID pred = i->path.pop_back ();
    warn << "dest " << dest << " pred " << pred << "\n";
    options->guess = pred;
    dhash.insert (data, datasize, wrap(&store_cb_ch2, dhash, pred), options);
  } else {
    options = NULL;
  }

  dhash.retrieve (i->key, DHASH_CONTENTHASH, wrap (&fetch_cb), options);
}


void 
store_cb_ch2 (dhashclient dhash, chordID pred, 
	      dhash_stat status, ptr<insert_info> i)
{
  if (status != DHASH_OK)
    warn << "contenthash store error\n";
  else
    warn << "contenthash store success\n";

  warn << "path: ";
  for (unsigned int j = 0; j < i->path.size (); j++)
    warnx << i->path[j];
  warnx << "\n";

  ptr<option_block> options;
  options = New refcounted<option_block> ();
  options->flags = DHASHCLIENT_GUESS_SUPPLIED;
  warn << "2 pred " << pred << "\n";
  options->guess = pred;
  dhash.retrieve (i->key, DHASH_CONTENTHASH, wrap (&fetch_cb), options);
}

void
store_cb_pk (dhashclient dhash, dhash_stat status, ptr<insert_info> i)
{
  if (status != DHASH_OK)
    warn << "pk store error\n";
  else
    warn << "pk store successful\n";

  dhash.retrieve (i->key, DHASH_KEYHASH, wrap (&fetch_cb));
}

void
store_cb_noauth (dhashclient dhash, dhash_stat status, ptr<insert_info> i)
{
  if (status != DHASH_OK)
    warn << "noauth store error " << i->key << "\n";
  else
    warn << "noauth store successful " << i->key << "\n";

  dhash.retrieve (i->key, DHASH_NOAUTH, wrap (&fetch_cb));
}

void
store_cb_append (dhashclient dhash, dhash_stat status, ptr<insert_info> i)
{
  if (status != DHASH_OK)
    warn << "store db error " << i->key << "\n";
  else
    warn << "store db successful " << i->key << "\n";
  
  dhash.retrieve (i->key, DHASH_APPEND, wrap (&fetch_cb_append, dhash));
}

void
fetch_cb_append (dhashclient dhash, dhash_stat stat, ptr<dhash_block> blk, vec<chordID> path) 
{
  if (!blk)
    fatal << "append: error\n";

  dhash.append (bigint (20), data_too, strlen (data_too), 
		wrap (&store_cb_append_second, dhash));
}

void
store_cb_append_second (dhashclient dhash, dhash_stat status,
                        ptr<insert_info> i)
{
  if (status != DHASH_OK)
    warn << "append error " << i->key << "\n";

  dhash.retrieve (i->key, DHASH_APPEND, wrap (&fetch_cb_append_second, dhash));
}

void
fetch_cb_append_second (dhashclient dhash, dhash_stat stat, ptr<dhash_block> blk, vec<chordID> path) 
{
  if (!blk)
    fatal << "append (second): error\n";
  
  warn << "data (" << blk->len << " bytes) was " << str(blk->data, blk->len) << "\n";
  exit (0);
}

void
fetch_cb (dhash_stat stat, ptr<dhash_block> blk, vec<chordID> path)
{

  if (!blk) {
    warn << "Error\n";
  }
  else if (datasize != blk->len || memcmp (data, blk->data, datasize) != 0)
    fatal << "verification failed";
  else {
    warn << "success\n path: ";
    for (unsigned int i = 0; i < path.size (); i++)
      warnx << path[i] << " ";
    warnx << "\n";
  }
  
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
      if (atoi(argv[4]) == 1) useGuess = true;
      dhash.insert (data, datasize, wrap(&store_cb_ch, dhash));
      break;
    }
  case PUB_KEY:
    {
    assert (argc == 5 && "last argument is pubkey file");

    str key = file2wstr (argv[4]);
    ptr<sfspriv> sk = sfscrypt.alloc_priv (key, SFS_SIGN);
    keyhash_payload p (0, str (data, datasize));
    sfs_pubkey2 pk;
    sfs_sig2 s;
    p.sign (sk, pk, s);
    dhash.insert (p.id (pk), pk, s, p, wrap (&store_cb_pk, dhash));
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
