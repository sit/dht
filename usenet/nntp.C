#include <rxx.h>
#include <crypt.h>
#include <dhash_common.h>
#include <dhashclient.h>
#include <verify.h>

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
char *checksendb = "238 ";
char *checksende = " no such article found, please send it to me\r\n";
char *checknob = "438 ";
char *checknoe = " already have it, please don't send it to me\r\n";
char *takethisokb = "239 ";
char *takethisoke = " article transferred ok\r\n";
char *takethisbadb = "439 ";
char *takethisbade = " article transfer failed\r\n";

nntp::nntp (int _s) : s (_s), process_input (wrap (this, &nntp::command)),
		      posting (false)
{
  warn << "connect " << s << "\n";
  fdcb (s, selread, wrap (this, &nntp::input));

  cmd_hello("MODE READER\r\n");
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
  add_cmd ("HELP", wrap (this, &nntp::cmd_help));
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
  warn << "out: " << out << "\n";

  int left = out.tosuio ()->output (s);

  if (left < 0) {
    warn << "bye bye love " << s << "\n";
    delete this;
  } else if (!left)
    fdcb (s, selwrite, NULL);
}

void
nntp::input (void)
{
  int res;

  res = in.input (s);
  if (res <= 0) {
    warn << "bye bye happiness " << s << "\n";
    delete this;
    return;
  }
  fdcb (s, selwrite, wrap (this, &nntp::output));

  process_input ();
}

static rxx cmdrx ("(.+\\r\\n)", "m");

void
nntp::command (void)
{
  unsigned int i;
  str cmd;

  while (cmdrx.search (str (in))) {  // xxx makes a big str if "in" is big
    in.rembytes (cmdrx.len (0));

    for (i = 0; i < cmd_table.size (); i++) {
      if (!strncasecmp (cmdrx[1], cmd_table[i].cmd, cmd_table[i].len)) {
	cmd_table[i].fn (cmdrx[1]);
	break;
      }
    }

    if (posting)
      return;

    if (i == cmd_table.size ()) {
      warn << "unknown command: " << cmdrx[1];
      out << unknown;
    }
  }
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

static rxx grouprx ("^GROUP (.+)\\r", "i");

void
nntp::cmd_group (str c) {
  warn << "group " << c;
  unsigned long count, first, last;

  if (grouprx.search (c)) {
    if (cur_group.open (grouprx[1], &count, &first, &last) < 0) {
      out << badgroup;
    } else {
      out << groupb << count << " " << first << " " << last << " "
	  << cur_group.name () << groupe;
    }
  } else
    out << syntax;
}

static rxx artrx ("^ARTICLE ?(<[^>]+>)?(\\d+)?", "i");

void
nntp::cmd_article (str c) {
  warn << "article " << c;
  chordID msgkey;
  ptr<dbrec> key, d;

  if (!cur_group.loaded ()) {
    out << nogroup;
  } else if (artrx.search (c)) {
    if (artrx[2]) {
      msgkey = cur_group.getid (strtoul (artrx[2], NULL, 10));
      cur_group.cur_art = strtoul (artrx[2], NULL, 10);
    warn << "msgkeytt " << msgkey << "\n";

    } else if (artrx[1]) {
      // xxx fixme, need to look at header_db? 
      //            or enhance the group_db to have a msgid index?
      msgkey = cur_group.getid (artrx[1]);
    } else {
      msgkey = cur_group.getid ();
    }

    warn << "msgkey " << msgkey << "\n";

    if (msgkey != 0)
      dhash->retrieve (msgkey,
		       wrap (this, &nntp::cmd_article_cb, !artrx[1], msgkey));
    else
      out << noarticle;
  } else {
    out << syntax;
    warn << "error\n";
  }
}

void
nntp::cmd_article_cb (bool head, chordID msgkey,
		      dhash_stat status, ptr<dhash_block> blk, vec<chordID> r)
{
  if (status != DHASH_OK) {
    out << noarticle;
    return;
  }
  if (head) {
    out << articleb << cur_group.cur_art << " " << msgkey << articlee;
  }
  out << str (blk->data, blk->len) << "\r\n";
  out << period;
  fdcb (s, selwrite, wrap (this, &nntp::output));
}

void
nntp::cmd_post (str c)
{
  warn << "post\n";
  out << postgo;
  posting = true;
  process_input = wrap (this, &nntp::read_post, postok, postbad);
}

static rxx postrx ("^((.+?\\n\\r\\n)(.+?\\n))\\.\\r\\n", "ms");
static rxx postmrx ("Message-ID: (<.+>)\\r");
static rxx postngrx ("Newsgroups: (.+)\\r");
static rxx postgrx (",?([^,]+)");

void
nntp::read_post (str resp, str bad)
{
  ptr<dbrec> k, d;
  ptr<group> g;
  str ng, msgid;
  bool posted = false;
  chordID ID;

  warn << "rp " << hexdump (str (in), in.resid ()) << "\n";
  warn << str (in) << "\n";

  if (postrx.search (str (in))) {
warn << " resid " << in.resid () << " rem " << postrx.len (0) << "\n";
    ID = compute_hash (postrx[1], postrx[1].len ());

    if (postmrx.search (postrx[2])) {
      warn << "found msgid " << postmrx[1] << "\n";
      warn << "mdg len " << postrx.len (0) << "\n";
      msgid = postmrx[1];
    } else {
      msgid = strbuf () << "<" << ID << "@usenetDHT>";
    }

    dhash->insert (postrx[1], postrx[1].len (),
		   wrap (this, &nntp::read_post_cb));

    int line = 0;

    for (unsigned int i = 0; i < postrx[3].len (); i++)
      if (postrx[3][i] == '\n')
	line++; // xxx make this only count lines of body

    str header = strbuf () << postrx[2] << "Lines: " << line << "\r\n" <<
      "ChordID: " << ID << "\r\n";

    k = New refcounted<dbrec> (msgid, msgid.len ());
    d = New refcounted<dbrec> (header, header.len ());
    header_db->insert(k, d);

    g = New refcounted<group> ();
    if (postngrx.search (postrx[2])) {
      ng = postngrx[1];

      while (postgrx.search (ng)) {
	warn << "n " << postgrx[1] << "\n";
	if (g->open (postgrx[1]) < 0)
	  warn << "tried to post unknown group " << postgrx[1] << "\n";
	else {
	  g->addid (msgid, ID);
	  posted = true;
	}

	ng = ng + postgrx[0].len ();
      }
    }
    in.rembytes (postrx.len (0));
    warn << "ok ";

    if (posted) {
      warnx << resp << "\n";
      out << resp;
    } else {
      warnx << bad << "\n";
      out << bad;
    }

    fdcb (s, selwrite, wrap (this, &nntp::output));
    posting = false;
    process_input = wrap (this, &nntp::command);
    process_input ();
  }
}

void
nntp::read_post_cb (dhash_stat status, ptr<insert_info> i)
{
  if (status != DHASH_OK)
    warn << "didn't store in DHASH after all\n";
}

void
nntp::cmd_quit (str c)
{
  warn << "quit " << s << "\n";
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
  warn << "ihave " << c;
  ptr<dbrec> key, d;

  if (ihaverx.search (c)) {
    key = New refcounted<dbrec> (ihaverx[1], ihaverx[1].len ());
    d = header_db->lookup (key);
    if (!d) {
      out << ihavesend;
      posting = true;
      process_input = wrap (this, &nntp::read_post, ihaveok, ihavebad);
    } else
      out << ihaveno;
  } else
    out << syntax;
}    

static rxx checkrx ("^CHECK (<.+?>)", "i");

void
nntp::cmd_check (str c)
{
  warn << "check " << c;
  ptr<dbrec> key, d;

  if (checkrx.search (c)) {
    key = New refcounted<dbrec> (checkrx[1], checkrx[1].len ());
    d = header_db->lookup (key);
    if (!d)
      out << checksendb << checkrx[1] << checksende;
    else
      out << checknob << checkrx[1] << checknoe;
  } else
    out << syntax;
}

static rxx takethisrx ("^TAKETHIS (<.+?>)", "i");

void
nntp::cmd_takethis (str c)
{
  warn << "takethis " << c;
  str resp, bad;

  if (takethisrx.search (c)) {
    resp = strbuf () << takethisokb << takethisrx[1] << takethisoke;
    bad = strbuf () << takethisbadb << takethisrx[1] << takethisbade;
    posting = true;
    process_input = wrap (this, &nntp::read_post, resp, bad);
    process_input ();
  } else
    out << syntax;
}
