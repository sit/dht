#include <async.h>
#include <dbfe.h>
#include <rxx.h>

#define USENET_PORT 11999 // xxx

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

struct grouplist {
  ptr<dbEnumeration> it;
  ptr<dbPair> d;

  grouplist ();
  void next (str *, int *);
  bool more (void) { return d; };
};

grouplist::grouplist ()
{
  it = group_db->enumerate();
  d = it->nextElement();    
};

static rxx listrx ("^(<.+?>)");

void
grouplist::next (str *f, int *i)
{
  ptr<dbrec> data = group_db->lookup (d->key);
  char *c = data->value;
  int len = data->len, cnt = 0;

  for (; listrx.search (str (c, len))
       ; c += listrx.len (1), len -= listrx.len (1) )
    cnt++;

  *f = str (d->key->value, d->key->len);
  *i = cnt;
  d = it->nextElement();
}

struct group {
  ptr<dbrec> rec;
  //  int cur_art;
  int start, stop;
  char *c;
  int len;

  group () : rec (0) {};
  int open (str);
  str name (void);
  
  void xover (int, int);
  strbuf next (void);
  bool more (void) { return start <= stop; };
};

int
group::open (str g) {
  rec = group_db->lookup(New refcounted<dbrec> (g, g.len ()));
  if (rec == NULL)
    return -1;

  char *c = rec->value;
  int len = rec->len, i = 0;

  for (; listrx.search (str (c, len))
       ; c += listrx.len (1), len -= listrx.len (1) )
    i++;

  return i;
}

str
group::name (void) {
  if (rec)
    return str (rec->value, rec->len);
  return str ();
}

void
group::xover (int a, int b)
{
  start = a;
  stop = b;
  // xxx b == -1
  c = rec->value;
  len = rec->len;
}

strbuf
group::next (void)
{
  ptr<dbrec> art;
  strbuf foo;
  foo << start << "\t";

  if (more () &&
      listrx.search (str (c, len))) {
    art = article_db->lookup(New refcounted<dbrec> (listrx[1],
						    listrx[1].len ()));
    if (art == NULL)
      warn << "missing article\n";
    else
      foo << str (art->value, art->len); // xxx

    c += listrx.len (1);
    len -= listrx.len (1);
    start++;
  }

  foo << "\t\r\n";
  return foo;
}

char *hello = "200 DHash news server - posting allowed\r\n";
char *unknown = "500 command not recognized\r\n";
char *listb = "215 list of newsgroups follows\r\n";
char *period = ".\r\n";
char *groupb = "211 ";
char *groupe = " group selected\r\n";
char *badgroup = "411 no such news group\r\n";
char *nogroup = "412 No news group current selected\r\n";
char *syntax = "501 command syntax error\r\n";
char *overview = "224 Overview information follows\r\n";
struct nntp;

typedef struct c_jmp_entry {
  char *cmd;
  int len;
  cbs fn;

  c_jmp_entry (char *_cmd, cbs _fn) : cmd (_cmd), fn (_fn) {
    len = strlen (cmd);
  };
} c_jmp_entry_t;

struct nntp {
  int s;
  suio out;
  group cur_group;

  nntp (int _s);
  ~nntp ();
  void command (void);
  void output (void);
  void cmd_hello (str);
  void cmd_list (str);
  void cmd_group (str);
  void cmd_over (str);
  void cmd_article (str);
  void cmd_quit (str);

  vec<c_jmp_entry_t> cmd_table;
};

nntp::nntp (int _s) : s (_s)
{
  warn << "connect " << s << "\n";
  fdcb (s, selread, wrap (this, &nntp::command));

  cmd_hello("\n");

  cmd_table.push_back ( c_jmp_entry_t ("ARTICLE", 
				       wrap (this, &nntp::cmd_article)) );
  cmd_table.push_back ( c_jmp_entry_t ("XOVER", 
				       wrap (this, &nntp::cmd_over)) );
  cmd_table.push_back ( c_jmp_entry_t ("GROUP", 
				       wrap (this, &nntp::cmd_group)) );
  cmd_table.push_back ( c_jmp_entry_t ("LIST", 
				       wrap (this, &nntp::cmd_list)) );

  cmd_table.push_back ( c_jmp_entry_t ("MODE READER",
				       wrap (this, &nntp::cmd_hello)) );

  cmd_table.push_back ( c_jmp_entry_t ("QUIT", wrap (this, &nntp::cmd_quit)) );
  //  cmd_table.push_back ( c_jmp_entry_t ("", wrap (this, &nntp::cmd_quit)) );
  //  timemark("done");
}

nntp::~nntp (void)
{
  fdcb (s, selread, NULL);
  fdcb (s, selwrite, NULL);
  close (s);
}

void
nntp::output (void)
{
  int res;
  char *buf = (char *) malloc (out.resid () + 1);
  out.copyout (buf, out.resid ());
  buf[out.resid ()] = 0;
  warn << buf;
  free (buf);

  res = out.output (s);
  // xxx check res
  if (out.resid () == 0)
    fdcb (s, selwrite, NULL);
}

void
nntp::command (void)
{
  suio in;
  int res;

  //  timemark("cmd");

  res = in.input (s);
  if (res <= 0)
    fdcb (s, selread, NULL);

  str cmd (in);
  for (unsigned int i = 0; i < cmd_table.size(); i++) {
    if (!strncmp (cmd, cmd_table[i].cmd, cmd_table[i].len)) {
      cmd_table[i].fn (cmd);
      return;
    }
  }

  warn << "unknown command: " << cmd;
  out.print (unknown, strlen (unknown));
  fdcb (s, selwrite, wrap (this, &nntp::output));
}

void
nntp::cmd_hello (str c) {
  warn << "hello: " << c;
  out.print (hello, strlen (hello));
  fdcb (s, selwrite, wrap (this, &nntp::output));
}

void
nntp::cmd_list (str c) {
  warn << "list\n";
  out.print (listb, strlen (listb));
  // foo 2 1 y\r\n
  grouplist g;
  str n;
  int i;
  strbuf foo;

  do {
    g.next (&n, &i);
    out.print (n, n.len ());
    foo << " " << i << " 1 y\r\n";
    out.take (foo.tosuio ());
  } while (g.more ());

  out.print (period, strlen (period));
  fdcb (s, selwrite, wrap (this, &nntp::output));
}

static rxx overrx ("^XOVER ?(\\d+)?(-)?(\\d+)?");

void
nntp::cmd_over (str c) {
  warn << "over " << c;

  if (cur_group.name ().len () == 0) {
    out.print (nogroup, strlen (nogroup));
  } else if (overrx.search (c)) {
    if (overrx[3]) {
      // range
      cur_group.xover (atoi (overrx[1]), atoi (overrx[3]));
    } else if (overrx[2]) {
      // endless
    } else if (overrx[1]) {
      // single
    } else {
      // current
    }

    out.print (overview, strlen (overview));
    do {
      out.take (cur_group.next ().tosuio ());
      out.print (period, strlen (period));
    } while(cur_group.more ());
  } else {
    // xxx er
    warn << "ror\n";
  }

  fdcb (s, selwrite, wrap (this, &nntp::output));
}

static rxx grouprx ("^GROUP (.+)\r\n", "m");

void
nntp::cmd_group (str c) {
  warn << "group " << c;
  strbuf foo;
  int i;

  if (grouprx.search (c)) {
    if ((i = cur_group.open (grouprx[1])) < 0) {
      out.print (badgroup, strlen (badgroup));
    } else {
      out.print (groupb, strlen (groupb));
      foo << i << " 1 " << i << " ";
      out.take (foo.tosuio ());
      out.print (cur_group.name (), cur_group.name ().len ());
      out.print (groupe, strlen (groupe));
    }
  } else
    out.print (syntax, strlen (syntax));

  fdcb (s, selwrite, wrap (this, &nntp::output));
}

void
nntp::cmd_quit (str c) {
  warn << "quit\n";
  delete this;
}


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

  // construct dummy group
  ref<dbrec> d = New refcounted<dbrec> ("<dd>", 4);
  ref<dbrec> k = New refcounted<dbrec> ("foo", 3);
  group_db->insert(k, d);

  // construct a dummy message
  k = New refcounted<dbrec> ("<dd>", 4);
  char *foomsg = "foosub\tfooauth\tfoodate\t<dd>\tfooref\t10000\t100";
  d = New refcounted<dbrec> (foomsg, strlen (foomsg));
  article_db->insert(k, d);

  startlisten ();
  amain ();

  return 0;
}
