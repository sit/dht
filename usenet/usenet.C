#include <async.h>
#include <dbfe.h>
#include <rxx.h>
#include <crypt.h>
#include <chord.h>

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
  int cur_art;
  int start, stop;
  char *c;
  int len;
  str group_name;

  group () : rec (0), cur_art (0) {};
  int open (str);
  str name (void) { return group_name; };
  
  void xover (int, int);
  strbuf next (void);
  bool more (void) { return start <= stop; };
  bool loaded (void) { return rec; };
  str getid (int);
  str getid (void) { return getid (cur_art); };

  void addid (str);
};

int
group::open (str g)
{
  rec = group_db->lookup(New refcounted<dbrec> (g, g.len ()));
  if (rec == NULL) {
    warn << "can't find group " << g << " " << g.len () << "\n";
    return -1;
  }
  warn << "rec len " << rec->len << "\n";

  group_name = g;
  cur_art = 1;
  char *c = rec->value;
  int len = rec->len, i = 0;

  for (; listrx.search (str (c, len))
       ; c += listrx.len (1), len -= listrx.len (1) )
    i++;

  return i;
}

void
group::addid (str id)
{
  assert (rec);
  str foo (rec->value, rec->len);
  ptr<dbrec> k = New refcounted<dbrec> (group_name, group_name.len ());

  warn << "addid " << str (rec->value, rec->len) << "\n";
  foo = strbuf () << foo << id;
  warn << "addid " << str (foo) << "\n";
  rec = New refcounted<dbrec> (foo, foo.len ());
  
  group_db->insert (k, rec);
}

str
group::getid (int index)
{
  if (rec == NULL)
    return str ();

  char *c = rec->value;
  int len = rec->len;
  warn << "rec len " << rec->len << "\n";

  for (; index && listrx.search (str (c, len))
       ; c += listrx.len (1), len -= listrx.len (1) )
    index--;

  if (listrx[1])
    return listrx[1];
  return str (); // xxx bad?
}

void
group::xover (int a, int b)
{
  start = a;
  stop = b;
  // xxx b == -1
  assert (rec);
  c = rec->value;
  len = rec->len;
  warn << "xv " << str (c, len) << "\n";
  for (int i = 1; listrx.search (str (c, len)) && (i < start)
	 ; c += listrx.len (1), len -= listrx.len (1) )
    i++;
}

static rxx oversub ("Subject: (.+)\r");
static rxx overfrom ("From: (.+)\r");
static rxx overdate ("Date: (.+)\r");
static rxx overmsgid ("Message-ID: (.+)\r");
static rxx overref ("References: (.+)\r");

//  char *foomsg = "foosub\tfooauth\tfoodate\t<dd>\tfooref\t10000\t100";
strbuf
group::next (void)
{
  ptr<dbrec> art;
  strbuf resp;
  resp << start << "\t";

  warn << "ss " << start << " - " << stop << "\n";
  warn << "xv " << str (c, len) << "\n";

  if (more () &&
      listrx.search (str (c, len))) {
    art = article_db->lookup(New refcounted<dbrec> (listrx[1],
						    listrx[1].len ()));

    warn << "mgs " << listrx[1] << "\n";

    if (art == NULL)
      warn << "missing article\n";
    else {
      str msg (art->value, art->len); // xxx
      int i, line = 0;
      if (oversub.search (msg) &&
	  overfrom.search (msg) &&
	  overdate.search (msg)) {
	resp << oversub[1] << "\t" << overfrom[1] << "\t";
	resp << overdate[1] << "\t";

	if (overmsgid.search (msg))
	  resp << overmsgid[1] << "\t";
	else
	  resp << listrx[1] << "\t";
	if (overref.search (msg))
	  resp << overref[1];
	for (i = 0; i < art->len; i++)
	  if (art->value[i] == '\n')
	    line++;
	resp << "\t" << art->len  << "\t" << line;
      // xxx filter out tabs
      } else
	warn << "msg parse error\n";
    }

    c += listrx.len (1);
    len -= listrx.len (1);
  }
  
  start++;
  resp << "\t\r\n";
  return resp;
}

char *hello = "200 DHash news server - posting allowed\r\n";
char *unknown = "500 command not recognized\r\n";
char *listb = "215 list of newsgroups follows\r\n";
char *period = ".\r\n";
char *groupb = "211 ";
char *groupe = " group selected\r\n";
char *badgroup = "411 no such news group\r\n";
char *nogroup = "412 No newsgroup selected\r\n";
char *syntax = "501 command syntax error\r\n";
char *overview = "224 Overview information follows\r\n";
char *articleb = "220 ";
char *articlee = " article retrieved - head and body follow\r\n";
char *postgo = "340 send article to be posted. End with <CR-LF>.<CR-LF>\r\n";
char *postok = "240 article posted ok\r\n";
char *postbad = "441 posting failed\r\n";
char *noarticle = "430 no such article found\r\n";
struct nntp;

typedef struct c_jmp_entry {
  const char *cmd;
  int len;
  cbs fn;

  c_jmp_entry (const char *_cmd, cbs _fn) : cmd (_cmd), fn (_fn) {
    len = strlen (cmd);
  };
} c_jmp_entry_t;

struct nntp {
  int s;
  suio out, post;
  group cur_group;

  nntp (int _s);
  ~nntp ();
  void command (void);
  void output (void);
  void add_cmd (const char *, cbs);
  void cmd_hello (str);
  void cmd_list (str);
  void cmd_group (str);
  void cmd_over (str);
  void cmd_article (str);
  void cmd_post (str);
  void read_post (void);
  void cmd_quit (str);

  vec<c_jmp_entry_t> cmd_table;
};

nntp::nntp (int _s) : s (_s)
{
  warn << "connect " << s << "\n";
  fdcb (s, selread, wrap (this, &nntp::command));

  cmd_hello("\n");
  fdcb (s, selwrite, wrap (this, &nntp::output));

  add_cmd ("ARTICLE", wrap (this, &nntp::cmd_article));
  add_cmd ("XOVER", wrap (this, &nntp::cmd_over));
  add_cmd ("GROUP", wrap (this, &nntp::cmd_group));
  add_cmd ("LIST", wrap (this, &nntp::cmd_list));
  add_cmd ("POST", wrap (this, &nntp::cmd_post));

  add_cmd ("MODE READER", wrap (this, &nntp::cmd_hello));
  add_cmd ("QUIT", wrap (this, &nntp::cmd_quit));
}

nntp::~nntp (void)
{
  fdcb (s, selread, NULL);
  fdcb (s, selwrite, NULL);
  close (s);
}

void
nntp::add_cmd (const char *cmd, cbs fn)
{
  cmd_table.push_back (c_jmp_entry_t (cmd, fn));
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
  if (res <= 0) {
    delete this;
    return;
  }
  fdcb (s, selwrite, wrap (this, &nntp::output));

  str cmd (in);
  for (unsigned int i = 0; i < cmd_table.size(); i++) {
    if (!strncmp (cmd, cmd_table[i].cmd, cmd_table[i].len)) {
      cmd_table[i].fn (cmd);
      return;
    }
  }

  warn << "unknown command: " << cmd;
  out.print (unknown, strlen (unknown));
}

void
nntp::cmd_hello (str c) {
  warn << "hello: " << c;
  out.print (hello, strlen (hello));
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
}

static rxx overrx ("^XOVER ?(\\d+)?(-)?(\\d+)?");

void
nntp::cmd_over (str c) {
  warn << "over " << c;

  if (!cur_group.loaded ()) {
    out.print (nogroup, strlen (nogroup));
  } else if (overrx.search (c)) {
    // xxx can crash on bad input? XOVER -3
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
    } while(cur_group.more ());
    out.print (period, strlen (period));
  } else {
    // xxx er
    warn << "ror\n";
  }
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
}

static rxx artrx ("^ARTICLE ?(<.+?>)?(.+?)?");

void
nntp::cmd_article (str c) {
  warn << "article " << c;
  str msgid;
  ptr<dbrec> key, d;
  strbuf foo;

  warn << "a1\n";

  if (!cur_group.loaded ()) {
    out.print (nogroup, strlen (nogroup));
  } else if (artrx.search (c)) {
    if (artrx[2]) {
  warn << "a2\n";
      msgid = cur_group.getid (atoi (artrx[2]));
      cur_group.cur_art = atoi (artrx[2]);
    } else if (artrx[1]) {
      msgid = artrx[1];
    } else {
      msgid = cur_group.getid ();
    }
  warn << "a3\n";

  if (msgid) {

    warn << "msgid " << msgid << "\n";

    key = New refcounted<dbrec> (msgid, msgid.len ());
    d = article_db->lookup (key);

    if (!artrx[1]) {
      out.print (articleb, strlen (articleb));
      foo << cur_group.cur_art << " " << msgid;
      out.take (foo.tosuio ());
      out.print (articlee, strlen (articlee));
    }
    out.print (d->value, d->len);
    out.print ("\r\n", 2);
    out.print (period, strlen (period));
  } else
    out.print (noarticle, strlen (noarticle));
  } else {
    // xxx er
    warn << "ror\n";
  }
}

void
nntp::cmd_post (str c)
{
  warn << "post\n";
  out.print (postgo, strlen (postgo));
  fdcb (s, selread, wrap (this, &nntp::read_post));
}

static rxx postrx ("(.+\n)\\.\r\n", "ms");
static rxx postmrx ("Message-ID: (<.+>)\r");
static rxx postngrx ("Newsgroups: (.+)\r");
static rxx postgrx (",?([^,]+)");

void
nntp::read_post (void)
{
  int res;
  ptr<dbrec> k, d;
  ptr<group> g;
  str ng, msgid;
  bool posted = false;

  res = post.input (s);
  if (res <= 0) {
    delete this;
    return;
  }

  warn << "rp " << str (post) << "\n";

  if (postrx.search (str (post))) {
    if (postmrx.search (postrx[1])) {
      warn << "found msgid " << postmrx[1] << "\n";
      msgid = postmrx[1];
    } else {
      char hashbytes[sha1::hashsize];
      sha1_hash (hashbytes, postrx[1], postrx[1].len ());
      chordID ID;
      mpz_set_rawmag_be (&ID, hashbytes, sizeof (hashbytes));
      msgid = strbuf () << "<" << ID << "@usenetDHT>";
    }

    k = New refcounted<dbrec> (msgid, msgid.len ());
    d = New refcounted<dbrec> (postrx[1], postrx[1].len ());
    article_db->insert(k, d);

    g = New refcounted<group> ();
    if (postngrx.search (postrx[1])) {
      ng = postngrx[1];

      while (postgrx.search (ng)) {
	warn << "n " << postgrx[1] << "\n";
	if (g->open (postgrx[1]) < 0)
	  warn << "tried to post unknown group " << postgrx[1] << "\n";
	else {
	  g->addid (msgid);
	  posted = true;
	}

	ng = ng + postgrx[0].len ();
      }
    }
  }
  post.clear ();

  if (posted)
    out.print (postok, strlen (postok));
  else
    out.print (postbad, strlen (postbad));

  fdcb (s, selwrite, wrap (this, &nntp::output));
  fdcb (s, selread, wrap (this, &nntp::command));
}

void
nntp::cmd_quit (str c)
{
  warn << "quit\n";
  delete this;
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

  // construct dummy group
  ref<dbrec> d = New refcounted<dbrec> ("", 0);
  ref<dbrec> k = New refcounted<dbrec> ("foo", 3);
  group_db->insert(k, d);
  k = New refcounted<dbrec> ("bar", 3);
  group_db->insert(k, d);
  k = New refcounted<dbrec> ("baz", 3);
  group_db->insert(k, d);

#if 0
  // construct a dummy message
  k = New refcounted<dbrec> ("<dd>", 4);
  char *foomsg = "foosub\tfooauth\tfoodate\t<dd>\tfooref\t10000\t100";
  d = New refcounted<dbrec> (foomsg, strlen (foomsg));
  article_db->insert(k, d);
#endif

  startlisten ();
  delaycb (SYNCTM, wrap (&syncdb));
  amain ();

  return 0;
}
