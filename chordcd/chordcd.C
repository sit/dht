#include "chordcd.h"
#include "sfscd_prot.h"

void gotrootfh (chord_server *server, ref<nfsserv> ns, sfsserver::fhcb cb, 
		nfs_fh3 *fh);
void
chord_server_alloc (sfsprog *prog, ref<nfsserv> ns, int tcpfd,
		 sfscd_mountarg *ma, sfsserver::fhcb cb)
{
  if (!ma->cres || (ma->carg.civers == 5
		    && !sfs_parsepath (ma->carg.ci5->sname))) {

    chord_server *server = New chord_server ();
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
