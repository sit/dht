#include "chordcd.h"
#include "sfscd_prot.h"

void
chord_server_alloc (sfsprog *prog, ref<nfsserv> ns, int tcpfd,
		 sfscd_mountarg *ma, sfsserver::fhcb cb)
{
  if (!ma->cres || (ma->carg.civers == 5
		    && !sfs_parsepath (ma->carg.ci5->sname))) {

    ptr<chord_server> server = New refcounted<chord_server>();
    server->setrootfh (ma->carg.ci5->sname);
    ns->setcb (wrap (server, &chord_server::dispatch));
    return;
  }
  fatal << "CFS must run in a connectinfo version 5 SFS environment\n";
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
