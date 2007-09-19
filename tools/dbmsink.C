/*
 * A data sink uesful for isolating the impact of dbm settings.
 *
 * Can compare with ttcp to see overhead of arpc and dbm parallelism.
 */
#include <arpc.h>
#include <dhashgateway_prot.h>

bool verbose = false;

void
dispatch (ptr<asrv> s, svccb *sbp)
{
  if (!sbp) {
    warn << "EOF\n";
    s = NULL;
    return;
  }

  switch (sbp->proc ()) {
    case DHASHPROC_NULL:
      sbp->reply (NULL);
      break;
    case DHASHPROC_INSERT:
      {
	dhash_insert_arg *arg = sbp->Xtmpl getarg<dhash_insert_arg> ();
	if (verbose)
	  warnx << arg->ctype << " "
	        << arg->blockID << " ("
		<< arg->block.size () << ")\n";
	dhash_insert_res res (DHASH_OK);
	res.resok->path.setsize (0);
	sbp->replyref (res);
	break;
      }
    case DHASHPROC_RETRIEVE:
    default:
      sbp->reject (PROC_UNAVAIL);
      break;
  }
}

void
do_accept (int fd)
{
  sockaddr_in sin;
  socklen_t sinlen = sizeof (sin);
  bzero (&sin, sinlen);
  int newfd = accept (fd, reinterpret_cast<sockaddr *> (&sin), &sinlen);
  if (newfd >= 0) {
    warn ("accepting connection from %s\n", inet_ntoa (sin.sin_addr));
    ptr<axprt_stream> x = axprt_stream::alloc (newfd, 1024*1025);
    ptr<asrv> s = asrv::alloc (x, dhashgateway_program_1);
    s->setcb (wrap (&dispatch, s));
  } else if (errno != EAGAIN) {
    warn ("accept: %m\n");
  }
}

int main (int argc, char *argv[])
{
  int port = 5555;
  if (argc > 1) {
    int argi = 1;
    if (!strcmp (argv[argi], "-v")) {
      argi++;
      verbose = true;
    }
    if (argi < argc) {
      char *end = NULL;
      port = strtol (argv[argi], &end, 0);
      if (end == argv[argi])
	fatal << "Usage: dbmsink [-v] [port]\n";
    }
  }

  int fd = inetsocket (SOCK_STREAM, port);
  if (fd < 0)
    fatal ("inetsocket: %m\n");
  if (listen (fd, 5) < 0) {
    fatal ("listen: %m\n");
    close (fd);
  }
  fdcb (fd, selread, wrap (&do_accept, fd));
  amain ();
}
