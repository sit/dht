#include <sfsrosd.h>
#include "parseopt.h"
#include "rxx.h"

static str sfsrodbfile;
static str mirrorhost;
static sfs_hostname hostname;
int sfssfd;
vec<sfsro_mirrorarg> mirrors;

void getcres_cb();
void getinfo_cb();
void add_mirror_cb(int fd);
void added_mirror(clnt_stat err);

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

void
add_mirror_cb(int fd) {
  
  ref<axprt_stream> x = axprt_stream::alloc (fd);
  ptr<aclnt> clnt = aclnt::alloc (x, sfsro_program_1);
  char thishost[256];
  gethostname(thishost, 256);
  warn << thishost << "\n";

  ptr<sfsro_mirrorarg>  arg = new refcounted<sfsro_mirrorarg> ();
  arg->host = sfs_hostname(thishost);
  warn << "adding " << arg->host << "as a mirror of " << mirrorhost << "\n";

  void *res = malloc(4);
  clnt->call (SFSROPROC_ADDMIRROR, arg, res, wrap (&added_mirror));
}

void
added_mirror(clnt_stat err) {
  warn << "made mirror_add RPC successfully\n";
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

  //now that we are running, add ourselves to the mirror list if necessary
  if (mirrorhost) {
    warn << "mirrorhost is " << mirrorhost << "\n"; 
    tcpconnect(mirrorhost, sfs_port, wrap (&add_mirror_cb));
  }

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
  while ((ch = getopt (argc, argv, "f:m:")) != -1)
    switch (ch) {
    case 'f':
      sfsrodbfile = optarg;
      break;
    case 'm':
      mirrorhost = optarg;
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

  mirrors = vec<sfsro_mirrorarg>();

  db = sfsrodb (sfsrodbfile);	// XXX - this is very poor style
  db.getconnectres (&cres, wrap(&getcres_cb));
  sigcb (SIGINT, wrap (exit, 1));
  sigcb (SIGTERM, wrap (exit, 1));
  
  amain();
}

void getcres_cb() {
  db.getinfo(&fsinfores, wrap(&getinfo_cb));
}

void getinfo_cb() {

  //FED - initial fsinfo tells us which part of the DB we are serving
  if (fsinfores.sfsro->v1->mirrors.size () > 0) {
    warnx << "this database contains mirror info\n";
  } else
    warnx << "database did not contain mirror info\n";
  

  start_server ();
}





