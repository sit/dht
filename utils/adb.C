#include "async.h"
#include "arpc.h"
#include "dbfe.h"
#include "adb_prot.h"
#include "sha1.h"
#include "id_utils.h"
#include "rxx.h"
#include "libadb.h"

static bool dbstarted (false);
static str dbsock;
int clntfd (-1);

void usage ();
static void listen_unix (str sock_name, ptr<dbfe> db);
void accept_cb (int lfd, ptr<dbfe> db);
void dispatch (ref<axprt_stream> s, ptr<asrv> a, ptr<dbfe> db, svccb *sbp);

EXITFN (cleanup);

static void
cleanup ()
{
  if (dbstarted) {
    if (clntfd >= 0)
      fdcb (clntfd, selread, NULL);
    unlink (dbsock);
  }
}

static void
halt ()
{
  warn << "Exiting on command.\n";
  exit (0);
}

void
do_store (ptr<dbfe> db, svccb *sbp)
{
  adb_storearg *arg = sbp->template getarg<adb_storearg> ();

  ref<dbrec> d = New refcounted<dbrec> (arg->data.base (), arg->data.size ());
  ref<dbrec> k = id_to_dbrec (arg->key, arg->name);

  db->insert (k, d);
  sbp->replyref (ADB_OK);
}

void
do_fetch (ptr<dbfe> db, svccb *sbp)
{
  adb_fetcharg *arg = sbp->template getarg<adb_fetcharg> ();
 
  ref<dbrec> k = id_to_dbrec (arg->key, arg->name);
  ptr<dbrec> dbres = db->lookup (k);

  adb_fetchres *res = New adb_fetchres ();
  if (dbres) {
    res->resok->key = arg->key;
    res->resok->data.setsize (dbres->len);
    memcpy(res->resok->data.base (), dbres->value, dbres->len);
  } else
    res->set_status (ADB_NOTFOUND);

  sbp->reply (res);
  
  delete res;
}

void
do_getkeys (ptr<dbfe> db, svccb *sbp)
{
  adb_getkeysarg *arg = sbp->template getarg<adb_getkeysarg> ();
  
  
  ptr<dbEnumeration> iter = db->enumerate ();
  ptr<dbPair> entry = iter->nextElement (id_to_dbrec(arg->start, arg->name));

  adb_getkeysres *res = New adb_getkeysres ();

  int num_keys = 0;
  str argname = arg->name;
  vec<chordID> ret;
  while (entry && num_keys < 128 && dbrec_to_name(entry->key) == argname) {
    chordID cur = dbrec_to_id (entry->key);
    ret.push_back (cur);
    num_keys++;
    entry = iter->nextElement ();
  } 
  res->resok->complete =  (!entry || 
			   (entry && dbrec_to_name (entry->key) != argname));

  res->resok->keys = ret;
  sbp->reply (res);
  delete res;
}

void
do_delete (ptr<dbfe> db, svccb *sbp)
{
  adb_deletearg *arg = sbp->template getarg<adb_deletearg> ();

  ref<dbrec> k = id_to_dbrec (arg->key, arg->name);
  db->del (k);

  sbp->replyref (ADB_OK);
}

void
dispatch (ref<axprt_stream> s, ptr<asrv> a, ptr<dbfe> db, svccb *sbp)
{
  if (sbp == NULL) {
    warn << "EOF from client\n";
    a = NULL;
    return;
  }

  switch (sbp->proc ()) {
  case ADBPROC_STORE:
    do_store (db, sbp);
    break;
  case ADBPROC_FETCH:
    do_fetch (db, sbp);
    break;
  case ADBPROC_GETKEYS:
    do_getkeys (db, sbp);
    break;
  case ADBPROC_DELETE:
    do_delete (db, sbp);
    break;
  default:
    fatal << "unkown procedure: " << sbp->proc () << "\n";
  }
  
  return;
}

static void
listen_unix (str sock_name, ptr<dbfe> db)
{
  unlink (sock_name);
  clntfd = unixsocket (sock_name);
  if (clntfd < 0) 
    fatal << "Error creating socket (UNIX)" << strerror (errno) << "\n";
  
  if (listen (clntfd, 128) < 0) {
    fatal ("Error from listen: %m\n");
    close (clntfd);
  } else {
    fdcb (clntfd, selread, wrap (accept_cb, clntfd, db));
  }
  dbstarted = true;
}

void 
accept_cb (int lfd, ptr<dbfe> db)
{
  sockaddr_un sun;
  bzero (&sun, sizeof (sun));
  socklen_t sunlen = sizeof (sun);
  int fd = accept (lfd, reinterpret_cast<sockaddr *> (&sun), &sunlen);
  if (fd < 0)
    fatal ("EOF\n");

  ref<axprt_stream> x = axprt_stream::alloc (fd, 1024*1025);

  ptr<asrv> a = asrv::alloc (x, adb_program_1);
  a->setcb (wrap (dispatch, x, a, db));
}

void
usage (char *progname)
{
  warn << progname << ": -d db -S sock\n";
  exit (0);
}

int 
main (int argc, char **argv)
{
  char ch;
  str db_name = "/var/tmp/db";
  dbsock = "/tmp/db-sock";

  sigcb (SIGHUP, wrap (&halt));
  sigcb (SIGINT, wrap (&halt));
  sigcb (SIGTERM, wrap (&halt));

  while ((ch = getopt (argc, argv, "d:S:"))!=-1)
    switch (ch) {
    case 'd':
      db_name = optarg;
      break;
    case 'S':
      dbsock = optarg;
      break;
    default:
      usage (argv[0]);
    }
 
  //open the DB (using dbfe for now)
  dbOptions opts;
  opts.addOption ("opt_cachesize", 1000);
  opts.addOption ("opt_nodesize", 4096);
  opts.addOption ("opt_dbenv", 1);

  ptr<dbfe> db = New refcounted<dbfe> ();
  if (int err = db->opendb (const_cast <char *> (db_name.cstr ()), opts))
  {
    warn << "DBFE open returned: " << strerror (err) << " for " << db_name << "\n";
    exit (-1);\
  }

  //setup the asrv
  listen_unix (dbsock, db);

  amain ();
}
