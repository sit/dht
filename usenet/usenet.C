#include <async.h>
#include <aios.h>
#include <dbfe.h>
#include <dhashclient.h>

#include "nntp.h"
#include "newspeer.h"
#include "usenet.h"

dbfe *group_db, *header_db;
// in group_db, each key is a group name. each record contains artnum,messageID,chordID
// in header_db, each key is a messageID. each record is a header (plus lines and other info)
dhashclient *dhash;

str
collect_stats ()
{
  strbuf s;
  s << "nconn " << nntp::nconn () << "\r\n";
  s << "fedinbytes " << nntp::fedinbytes () << "\r\n";
  s << "dhashbytes " << nntp::dhashbytes () << "\r\n";
  s << "npeers " << peers.size () << "\r\n";

  u_int64_t totalout (0);
  for (size_t i = 0; i < peers.size (); i++)
    totalout += peers[i]->fedoutbytes ();
  s << "fedoutbytes " << totalout << "\r\n";

  return s;
}

void
stop ()
{
  // XXX shutdown open connections cleanly....
  warn << "Shutting down on signal.\n";
  exit (1);
}

bool
create_group (char *group)
{
  // xxx sanity check group name
  static ref<dbrec> d = New refcounted<dbrec> ("", 0);
  ref<dbrec> k = New refcounted<dbrec> (group, strlen (group));
  group_db->insert (k, d);
  return true;
}

// boring network accept code

void
tryaccept (int s)
{
  int new_s;
  struct sockaddr *addr;
  unsigned int addrlen = sizeof (struct sockaddr_in);

  addr = (struct sockaddr *) calloc (1, addrlen);
  new_s = accept (s, addr, &addrlen);
  if (new_s > 0) {
    make_async (new_s);
    vNew nntp (new_s);
  } else
    perror (progname);
  free (addr);
}

void
startlisten (void)
{
  int s = inetsocket (SOCK_STREAM, opt->listen_port, INADDR_ANY);
  if (s > 0) {
    make_async (s);
    listen (s, 5);
    fdcb (s, selread, wrap (&tryaccept, s));
  }
}

void
syncdb (void)
{
  group_db->sync ();
  header_db->sync ();
  delaycb (opt->sync_interval, wrap (&syncdb));
}

void
usage ()
{
  fatal ("Usage: usenet -S chord_socket [-d rootdir] [-f conf] [-g]\n");
}

int
main (int argc, char *argv[])
{
  setprogname (argv[0]);

  char *sock = "/tmp/chord-sock";
  char *dirbase = NULL;
  char *cffile  = NULL;
  char *root    = NULL;
  bool create_groups = false;

  int ch;

  while ((ch = getopt (argc, argv, "d:f:gS:")) != -1) {
    switch (ch) {
    case 'd':
      dirbase = optarg;
      break;
    case 'f':
      cffile = optarg;
      break;
    case 'g':
      create_groups = true;
      break;
    case 'S':
      sock = optarg;
      break;
    default:
      usage ();
    }
  }

  if (dirbase && chdir (dirbase) < 0)
    fatal << "chdir(" << dirbase << "): " << strerror (errno) << "\n";

  if (cffile && !parseconfig (opt, cffile))
    fatal << "errors parsing configuration file\n";

  dhash = New dhashclient (sock);

  //set up the options we want
  dbOptions opts;
  opts.addOption ("opt_async", 1);
  opts.addOption ("opt_cachesize", 1000);
  opts.addOption ("opt_nodesize", 4096);

  group_db = New dbfe ();
  if (int err = group_db->opendb ("groups", opts)) {
    warn << "open returned: " << strerror (err) << "\n";
    exit (-1);
  }
  header_db = New dbfe ();
  if (int err = header_db->opendb ("headers", opts)) {
    warn << "open returned: " << strerror (err) << "\n";
    exit (-1);
  }

  if (create_groups) {
    create_group ("foo");
    create_group ("usenetdht.test");
    
    create_group ("rec.bicycles.misc");
    create_group ("alt.binaries.pictures.rail");
  }

  sigcb (SIGINT,  wrap (&stop));
  sigcb (SIGTERM, wrap (&stop));

  startlisten ();
  delaycb (opt->sync_interval, wrap (&syncdb));
  amain ();

  return 0;
}
