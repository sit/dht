#include <async.h>
#include <aios.h>
#include <dbfe.h>
#include <dhashclient.h>

#include "nntp.h"
#include "newspeer.h"
#include "usenet.h"
#include "usenetdht_storage.h"

ptr<dbfe> group_db, header_db;
// in group_db, each key is a group name. each record contains artnum,messageID,chordID
// in header_db, each key is a messageID. each record is a header (plus lines and other info)
dhashclient *dhash;
static char *sock = "/tmp/chord-sock";

str
collect_stats ()
{
  strbuf s;
  s << "nconn " << nntp::nconn () << "\r\n";
  s << "fedinbytes " << nntp::fedinbytes () << "\r\n";
  s << "dhashbytes " << nntp::dhashbytes () << "\r\n";
  s << "npeers " << opt->peers.size () << "\r\n";
  s << "fedoutbytes " << newspeer::totalfedbytes () << "\r\n";

  return s;
}

static void
stop ()
{
  // XXX shutdown open connections cleanly....
  warn << "Shutting down on signal.\n";
  delete dhash;
  exit (1);
}

static void
reconfig ()
{
  warn << "Re-reading configuration file\n";
  options *nopt = New options;
  if (!parseconfig (nopt, config_file)) {
    delete nopt;
    warn ("errors found in config file, keeping old configuration\n");
    return;
  }

  delete opt;
  opt = nopt;
}

// boring network accept code

void
tryaccept (int s)
{
  sockaddr_in sin;
  socklen_t sinlen = sizeof (sin);
  bzero (&sin, sizeof (sin));

  int fd = accept (s, (sockaddr *) &sin, &sinlen);
  if (fd < 0) {
    if (errno != EAGAIN)
      warn ("accept: %m\n");
    return;
  }

  make_async (fd);
  vNew nntp (fd, sin);
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
  group_db->checkpoint ();
  header_db->checkpoint ();
  delaycb (opt->sync_interval, wrap (&syncdb));
}

static void
eofhandler (void)
{
  warn << "Unexpected EOF from DHash client; attempting to recover...\n";
  delete dhash;

  dhash = New dhashclient (sock);
  dhash->seteofcb (wrap (eofhandler));
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

  char *dirbase = NULL;
  bool create_groups = opt->create_unknown_groups;

  int ch;

  while ((ch = getopt (argc, argv, "d:f:gS:")) != -1) {
    switch (ch) {
    case 'd':
      dirbase = optarg;
      break;
    case 'f':
      config_file = optarg;
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

  if (!parseconfig (opt, config_file))
    fatal << "errors parsing configuration file\n";

  // Override any configuration file settings from command line.
  opt->create_unknown_groups = create_groups;

  dhash = New dhashclient (sock);
  dhash->seteofcb (wrap (eofhandler));

  dbOptions opts;
  group_db = New refcounted<dbfe> ();
  if (int err = group_db->opendb ("groups", opts)) {
    warn << "open returned: " << strerror (err) << "\n";
    exit (-1);
  }
  header_db = New refcounted<dbfe> ();
  if (int err = header_db->opendb ("headers", opts)) {
    warn << "open returned: " << strerror (err) << "\n";
    exit (-1);
  }

  sigcb (SIGINT,  wrap (&stop));
  sigcb (SIGTERM, wrap (&stop));
  sigcb (SIGHUP, wrap (&reconfig));

  startlisten ();
  delaycb (opt->sync_interval, wrap (&syncdb));
  amain ();

  return 0;
}
