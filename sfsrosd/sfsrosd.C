#include <sfsrosd.h>
#include "parseopt.h"
#include "rxx.h"

static str sfsrodbfile;
static sfs_hostname hostname;
int sfssfd;

void getcres_cb();
void getinfo_cb();

ptr<axprt_stream>
client_accept (int fd)
{
  if (fd < 0)
    fatal ("EOF from sfssd\n");
  tcp_nodelay (fd);
  //  ref<axprt_crypt> x = axprt_crypt::alloc (fd);
  ref<axprt_stream> x = axprt_stream::alloc (fd);
  vNew sfsroclient (x);

  return x;
}

static void
client_accept_standalone ()
{
  sockaddr_in sin;
  bzero (&sin, sizeof (sin));
  socklen_t sinlen = sizeof (sin);
  int fd = accept (sfssfd, reinterpret_cast<sockaddr *> (&sin), &sinlen);
  if (fd >= 0)
    client_accept (fd);
  else if (errno != EAGAIN)
    fatal ("accept: %m\n");
}


static void
start_server ()
{
  setgid (sfs_gid);
  setgroups (0, NULL);

  warn ("version %s, pid %d\n", VERSION, getpid ());
  warn << "serving " << sfsroot << "/" 
       << sfs_hostinfo2path (cres.reply->servinfo.host) << "\n";
  /*
  sfs_hash hostid;
  if (!sfs_mkhostid (&hostid, cres.reply->servinfo.host))
    fatal << "could not marshall hostid\n";

       << cres.reply->servinfo.host.hostname << ":"
       << armor32 (str (hostid.base (), sizeof (sfs_hash))) << "\n";
  
  sfs_suidserv ("sfsrosd", wrap (unixaccept));

  if (!cloneserv (0, wrap (cloneaccept)))
    warn ("No sfssd detected, only accepting unix-domain clients\n");
  */

  if (cloneserv (0, wrap (client_accept)))
    return;

  warn ("No sfssd detected, running in standalone mode\n");
  sfssfd = inetsocket (SOCK_STREAM, sfs_port);
  if (sfssfd < 0)
    fatal ("binding TCP port %d: %m\n", sfs_port);
  listen (sfssfd, 1000);
  fdcb (sfssfd, selread, wrap (client_accept_standalone));

}


static void
usage ()
{
  warnx << "usage: " << progname << " -f <sfsrodbfile>\n";
  exit (1);
}

int
main (int argc, char **argv)
{
  setprogname (argv[0]);

  int ch;
  while ((ch = getopt (argc, argv, "f:")) != -1)
    switch (ch) {
    case 'f':
      sfsrodbfile = optarg;
      break;
    case '?':
    default:
      usage ();
    }
  argc -= optind;
  argv += optind;

  if (!sfsrodbfile)
    usage ();

  sfsconst_init ();


  db = sfsrodb (sfsrodbfile);	// XXX - this is very poor style


  
  db.getconnectres (&cres);
  db.getinfo(&fsinfores);

  sigcb (SIGINT, wrap (exit, 1));
  sigcb (SIGTERM, wrap (exit, 1));

  start_server ();
  amain();
}



