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
char *help = "100 help text follow\r\n";
char *ihavesend = "335 send article to be transferred.  End with <CR-LF>.<CR-LF>\r\n";
char *ihaveok = "235 article transferred ok\r\n";
char *ihaveno = "435 article not wanted - do not send it\r\n";
char *ihavebad = "436 transfer failed - try again later\r\n";
char *stream = "203 Streaming is OK\r\n";
char *checksend = "238 no such article found, please send it to me ";
char *checkno = "438 already have it, please don't send it to me ";
char *takethisok = "239 article transferred ok ";
char *takethisbad = "439 article transfer failed ";

nntp::nntp (int _s) : s (_s)
{
  warn << "connect " << s << "\n";
  fdcb (s, selread, wrap (this, &nntp::command));

  cmd_hello("MODE READER");
  fdcb (s, selwrite, wrap (this, &nntp::output));

  add_cmd ("CHECK", wrap (this, &nntp::cmd_check));
  add_cmd ("TAKETHIS", wrap (this, &nntp::cmd_takethis));
  add_cmd ("IHAVE", wrap (this, &nntp::cmd_ihave));

  add_cmd ("ARTICLE", wrap (this, &nntp::cmd_article));
  add_cmd ("XOVER", wrap (this, &nntp::cmd_over));
  add_cmd ("GROUP", wrap (this, &nntp::cmd_group));
  add_cmd ("LIST", wrap (this, &nntp::cmd_list));
  add_cmd ("POST", wrap (this, &nntp::cmd_post));

  add_cmd ("MODE", wrap (this, &nntp::cmd_hello));
  add_cmd ("QUIT", wrap (this, &nntp::cmd_quit));
  add_cmd ("HELP", wrap (this, &nntp::cmd_help));
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

  if (left < 0)
    delete this;
  else if (!left)
    fdcb (s, selwrite, NULL);
}

void
nntp::command (void)
{
  suio in;
  int res;

  res = in.input (s);
  if (res <= 0) {
    delete this;
    return;
  }
  fdcb (s, selwrite, wrap (this, &nntp::output));

  str cmd (in);
  for (unsigned int i = 0; i < cmd_table.size(); i++) {
    if (!strncasecmp (cmd, cmd_table[i].cmd, cmd_table[i].len)) {
      cmd_table[i].fn (cmd);
      return;
    }
  }

  warn << "unknown command: " << cmd;
  out << unknown;
}

static rxx hellorx ("^MODE (READER)?(STREAM)?", "i");

void
nntp::cmd_hello (str c) {
  warn << "hello: " << c;

  if (hellorx.search (c)) {
    if (hellorx[1]) {
      out << hello;
      return;
    }
    if (hellorx[2]) {
      out << stream;
      return;
    }
  }
  out << syntax;
}

// format:  foo 2 1 y\r\n

void
nntp::cmd_list (str c) {
  warn << "list\n";
  grouplist g;
  str n;
  unsigned long i;

  out << listb;
  do {
    g.next (&n, &i);
    out << n << " " << i << " 1 y\r\n";
  } while (g.more ());
  out << period;
}

static rxx overrx ("^XOVER( ((\\d+)-)?(\\d+)?)?", "i");
//( (\\d+)?((\\d+)-(\\d+)?)?)?");
// ?(\\d+)?(-)?(\\d+)?");

void
nntp::cmd_over (str c) {
  warn << "over " << c;
  unsigned long start, stop = -1UL;

  if (!cur_group.loaded ()) {
    out << nogroup;
  } else if (overrx.search (c)) {
    // extract start and stop

    if (overrx[3]) {
      // endless
      start = strtoul (overrx[3], NULL, 10);
      if (overrx[4]) {
	// range
	stop = strtoul (overrx[4], NULL, 10);
      }
    } else if (overrx[4]) {
      // single
      start = stop = strtoul (overrx[4], NULL, 10);
    } else {
      // current
      start = stop = cur_group.cur_art;
    }
    cur_group.xover (start, stop);

    out << overview;
    do {
      out << cur_group.next ();
    } while(cur_group.more ());
    out << period;
  } else {
    out << syntax;
    warn << "error\n";
  }
}

static rxx grouprx ("^GROUP (.+)\r\n", "mi");

void
nntp::cmd_group (str c) {
  warn << "group " << c;
  unsigned long count, first, last;

  if (grouprx.search (c)) {
    if (cur_group.open (grouprx[1], &count, &first, &last) < 0) {
      out << badgroup;
    } else {
      out << groupb << count << " " << first << " " << last << " " << cur_group.name () << groupe;
    }
  } else
    out << syntax;
}

static rxx artrx ("^ARTICLE ?(<.+?>)?(.+?)?", "i");

void
nntp::cmd_article (str c) {
  warn << "article " << c;
  str msgid;
  ptr<dbrec> key, d;

  if (!cur_group.loaded ()) {
    out << nogroup;
  } else if (artrx.search (c)) {
    if (artrx[2]) {
      msgid = cur_group.getid (strtoul (artrx[2], NULL, 10));
      cur_group.cur_art = strtoul (artrx[2], NULL, 10);
    } else if (artrx[1]) {
      msgid = artrx[1];
    } else {
      msgid = cur_group.getid ();
    }

    if (msgid) {

      warn << "msgid " << msgid << "\n";

      key = New refcounted<dbrec> (msgid, msgid.len ());
      d = article_db->lookup (key);
      if (!d) {
	out << noarticle;
	return;
      }

      if (!artrx[1]) {
	out << articleb << cur_group.cur_art << " " << msgid << articlee;
      }
      out << str (d->value, d->len) << "\r\n";
      out << period;
    } else
      out << noarticle;
  } else {
    out << syntax;
    warn << "error\n";
  }
}

void
nntp::cmd_post (str c)
{
  warn << "post\n";
  out << postgo;
  fdcb (s, selread, wrap (this, &nntp::read_post, postok, postbad));
}

static rxx postrx ("(.+\n)\\.\r\n", "ms");
static rxx postmrx ("Message-ID: (<.+>)\r");
static rxx postngrx ("Newsgroups: (.+)\r");
static rxx postgrx (",?([^,]+)");

void
nntp::read_post (const char *resp, const char *bad)
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
    out << resp;
  else
    out << bad;

  fdcb (s, selwrite, wrap (this, &nntp::output));
  fdcb (s, selread, wrap (this, &nntp::command));
}

void
nntp::cmd_quit (str c)
{
  warn << "quit\n";
  delete this;
}

void
nntp::cmd_help (str c)
{
  warn << "help\n";
  out << help;
  for (unsigned int i = 0; i < cmd_table.size(); i++)
    out << cmd_table[i].cmd << "\r\n";
  out << period;
}



// News transfer commands:

static rxx ihaverx ("^IHAVE (<.+?>)", "i");

void
nntp::cmd_ihave (str c)
{
  warn << "ihave\n";
  ptr<dbrec> key, d;

  if (ihaverx.search (c)) {
    key = New refcounted<dbrec> (ihaverx[1], ihaverx[1].len ());
    d = article_db->lookup (key);
    if (!d) {
      out << ihavesend;
      fdcb (s, selread, wrap (this, &nntp::read_post, ihaveok, ihavebad));
    } else
      out << ihaveno;
  } else
    out << syntax;
}    

static rxx checkrx ("^CHECK (<.+?>)", "i");

void
nntp::cmd_check (str c)
{
  warn << "check\n";
  ptr<dbrec> key, d;

  if (checkrx.search (c)) {
    key = New refcounted<dbrec> (checkrx[1], checkrx[1].len ());
    d = article_db->lookup (key);
    if (!d)
      out << checksend << checkrx[1] << "\r\n";
    else
      out << checkno << checkrx[1] << "\r\n";
  } else
    out << syntax;
}

static rxx takethisrx ("^TAKETHIS (<.+?>)", "i");

void
nntp::cmd_takethis (str c)
{
  warn << "takethis\n";
  str resp, bad;

  if (takethisrx.search (c)) {
    resp = strbuf () << takethisok << takethisrx[1] << "\r\n";
    bad = strbuf () << takethisbad << takethisrx[1] << "\r\n";
    fdcb (s, selread, wrap (this, &nntp::read_post, resp, bad));
  } else
    out << syntax;
}
