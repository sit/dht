/*
 *
 * Copyright (C) 2001  Josh Cates (cates@mit.edu),
 *                     Frank Dabek (fdabek@lcs.mit.edu), 
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


#include "starcd.h"
#include "sfscd_prot.h"

#define CACHE_MAXSIZE 1000

void gotrootfh (chord_server *server, ref<nfsserv> ns, sfsserver::fhcb cb, 
		nfs_fh3 *fh);
void
chord_server_alloc (sfsprog *prog, ref<nfsserv> ns, int tcpfd,
		 sfscd_mountarg *ma, sfsserver::fhcb cb)
{
  if (!ma->cres || (ma->carg.civers == 5
		    && !sfs_parsepath (ma->carg.ci5->sname))) {

    warn << "cache_size " << CACHE_MAXSIZE << "\n";
    chord_server *server = New chord_server (CACHE_MAXSIZE);
    warn << "request to mount " << ma->carg.ci5->sname << "\n";
    server->setrootfh (ma->carg.ci5->sname, wrap (&gotrootfh, server, ns, cb));
    return;
  }
  fatal << "CFS must run in a connectinfo version 5 SFS environment\n";
}

void
gotrootfh (chord_server *server, ref<nfsserv> ns, sfsserver::fhcb cb, 
	   nfs_fh3 *fh)
{
  if (fh) warn << "fetched the root block\n";
  else warn << "error finding root block\n";
  if (fh) {
    if (!ns->encodefh (*fh)) (*cb) (NULL);
    else {
      (*cb) (fh);
      ns->setcb (wrap (server, &chord_server::dispatch, ns));
    }
  }
	
}

int
main (int args, char **argv) 
{

  setprogname (argv[0]);
  warn << "chordcd version 0.1 running under PID " << getpid () << "\n";
  sfsconst_init ();
  if (ptr<axprt_unix> x = axprt_unix_stdin ())
    vNew sfsprog (x, &chord_server_alloc);
  else
    fatal ("Couldn't allocate unix transport.\n");

  amain();
}
