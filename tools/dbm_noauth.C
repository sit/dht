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
#include <dhash_prot.h>
#include <dhashclient.h>
#include <dhblock.h>
#include <dbfe.h>
#include <crypt.h>
#include <sys/time.h>

str control_socket;
static FILE *outfile;
unsigned int datasize;

ptr<axprt_stream> xprt;
int fconnected = 0;
int out = 0;
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
store_cb (u_int64_t start, dhash_stat status, ptr<insert_info> i)
{
  out++;

  strbuf s;
  if (status != DHASH_OK) {
    s << "store_cb: " << i->key << " " << status << "\n";
  } else {
    bps++;
    s << i->key << " / " << (getusec () - start)/1000 << " /";
    for (size_t j = 0; j < i->path.size (); j++)
      s << " " << i->path[j];
    s << "\n";
  }
  str buf (s);
  fprintf (outfile, "%s", buf.cstr ());
  if (outfile != stdout)
    warnx << buf;
}


int
store (dhashclient *dhash, char *key, char *value) 
{

  dhash->insert (compute_hash (key, strlen(key)), 
		 value, strlen(value) + 1, 
		 wrap (store_cb, getusec ()), NULL, DHASH_NOAUTH);

  dhash->insert (compute_hash (key, strlen(key)), 
		 value, strlen(value) + 1, 
		 wrap (store_cb, getusec ()), NULL, DHASH_NOAUTH);

  while (out == 0) acheck ();
  return 0;
}



void
fetch_cb (dhash_stat stat, ptr<dhash_block> blk, vec<chordID> path)
{
  out++;

  if (!blk) {
    strbuf buf;
    fprintf (outfile, str (buf).cstr ());
  }

  if (blk) {
    strbuf buf;


    bps++;
    buf << " /";
    for (u_int i = 0; i < blk->times.size (); i++)
      buf << " " << blk->times[i];
    buf << " /";

    buf << " " << blk->hops << " " <<  blk->errors
	<< " " << blk->retries << " ";
    for (u_int i=0; i < path.size (); i++) {
      buf << path[i] << " ";
    }
    
    buf << " : ";
    if (blk->vData.size () > 0) {
      warn << blk->vData.size () << "\n";
      for (unsigned int i = 0; i < blk->vData.size (); i++) {
	buf << "    " << i << ": " << blk->vData[i] << " -- \n";
	warn << blk->vData[i] << "\n";
      }
    } else {
      buf << blk->data << "\n";
    }
    buf << "\n\n";

    fprintf (outfile, str (buf).cstr ());
    if (outfile != stdout)
      warnx << buf;
  } 
}


void
fetch (dhashclient &dhash, char *key)
{

  
  dhash.retrieve (compute_hash (key, strlen(key)), DHASH_NOAUTH, 
		  wrap (fetch_cb));

  while (out == 0) acheck ();
}

void
usage (char *progname) 
{
  warn << "control_socket f|s key [value]\n";
  exit(0);
}

void
cleanup (void)
{
  if (outfile) {
    fclose (outfile);
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

  //dbm sock [f|s] key value

  fconnected = 1;
  outfile = stdout;

  struct timeval start;
  gettimeofday (&start, NULL);

  if (argv[2][0] == 's')
    store (dhash, argv[3], argv[4]);
  else
    fetch (*dhash, argv[3]);
  

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

  if (argc < 4) usage (argv[0]);

  sigcb (SIGTERM, wrap (&cleanup));
  sigcb (SIGINT, wrap (&cleanup));

  control_socket = argv[1];
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


