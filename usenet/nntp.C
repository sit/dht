#include <rxx.h>
#include <crypt.h>
#include <chord.h>

#include <usenet.h>
#include <nntp.h>

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
  warn << out;

  int left = out.tosuio ()->output (s);
  // xxx check if left < 0?
  if (!left)
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
  out << unknown;
}

void
nntp::cmd_hello (str c) {
  warn << "hello: " << c;
  out << hello;
}

void
nntp::cmd_list (str c) {
  warn << "list\n";
  out << listb;
  // foo 2 1 y\r\n
  grouplist g;
  str n;
  int i;

  do {
    g.next (&n, &i);
    out << n << " " << i << " 1 y\r\n";
  } while (g.more ());

  out << period;
}

static rxx overrx ("^XOVER ?(\\d+)?(-)?(\\d+)?");

void
nntp::cmd_over (str c) {
  warn << "over " << c;

  if (!cur_group.loaded ()) {
    out << nogroup;
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

    out << overview;
    do {
      out << cur_group.next ();
    } while(cur_group.more ());
    out << period;
  } else {
    // xxx er
    warn << "ror\n";
  }
}

static rxx grouprx ("^GROUP (.+)\r\n", "m");

void
nntp::cmd_group (str c) {
  warn << "group " << c;
  int i;

  if (grouprx.search (c)) {
    if ((i = cur_group.open (grouprx[1])) < 0) {
      out << badgroup;
    } else {
      out << groupb << i << " 1 " << i << " " << cur_group.name () << groupe;
    }
  } else
    out << syntax;
}

static rxx artrx ("^ARTICLE ?(<.+?>)?(.+?)?");

void
nntp::cmd_article (str c) {
  warn << "article " << c;
  str msgid;
  ptr<dbrec> key, d;

  warn << "a1\n";

  if (!cur_group.loaded ()) {
    out << nogroup;
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
      out << articleb << cur_group.cur_art << " " << msgid << articlee;
    }
    out << str (d->value, d->len) << "\r\n";
    out << period;
  } else
    out << noarticle;
  } else {
    // xxx er
    warn << "ror\n";
  }
}

void
nntp::cmd_post (str c)
{
  warn << "post\n";
  out << postgo;
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
    out << postok;
  else
    out << postbad;

  fdcb (s, selwrite, wrap (this, &nntp::output));
  fdcb (s, selread, wrap (this, &nntp::command));
}

void
nntp::cmd_quit (str c)
{
  warn << "quit\n";
  delete this;
}


