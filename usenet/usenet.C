#include <async.h>
#include <dbfe.h>

#include <nntp.h>

#define USENET_PORT 11999 // xxx
#define SYNCTM 5

dbfe *group_db, *article_db;
// in group_db, each key is a group name. each record contains messageIDs
// in article_db, each key is a messageID. each record is an article

void
timemark(str foo)
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  warn << foo << " " << tv.tv_sec << " " << tv.tv_usec << "\n";
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
    //    timemark("new");
    vNew nntp (new_s);
  } else
    perror (progname);
  free (addr);
}

void
startlisten (void)
{
  int s = inetsocket (SOCK_STREAM, USENET_PORT, INADDR_ANY);
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
  article_db->sync ();
}

int
main (int argc, char *argv[])
{
  setprogname (argv[0]);

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
  article_db = New dbfe ();
  if (int err = article_db->opendb ("articles", opts)) {
    warn << "open returned: " << strerror (err) << "\n";
    exit (-1);
  }

  if (argc > 1) {
    // construct dummy group
    ref<dbrec> d = New refcounted<dbrec> ("", 0);
    ref<dbrec> k = New refcounted<dbrec> ("foo", 3);
    group_db->insert(k, d);
    k = New refcounted<dbrec> ("bar", 3);
    group_db->insert(k, d);
    k = New refcounted<dbrec> ("baz", 3);
    group_db->insert(k, d);
  }

  startlisten ();
  delaycb (SYNCTM, wrap (&syncdb));
  amain ();

  return 0;
}
