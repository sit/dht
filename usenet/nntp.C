#include <aios.h>
#include <parseopt.h>
#include <rxx.h>
#include <crypt.h>
#include <dhash_common.h>
#include <dhashclient.h>
#include <verify.h>

#include <usenet.h>
#include <nntp.h>

#define TIMEOUT 600

nntp::nntp (int _s) :
	s (_s),
	aio (aios::alloc (_s)),
	process_input (wrap (this, &nntp::command)),
        posting (false)
{
  warn << "connect " << s << "\n";
  aio->settimeout (TIMEOUT);
  aio->setdebug (strbuf("%d", s));

  deleted = New refcounted<bool> (false);

  cmd_hello("MODE READER\r\n");

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

  aio->readline (wrap (this, &nntp::process_line));
}

nntp::~nntp (void)
{
  close (s);
}

void
nntp::process_line (const str data, int err)
{
  if (err < 0) {
    warnx << "aio oops " << err << "\n";
    if (err == ETIMEDOUT) {
      died ();
      return;
    }
  }
  if (!data || !data.len()) {
    warnx << "data oops\n";
    died();
    return;
  }
  lines.push_back(data);
  process_input ();
  aio->readline (wrap (this, &nntp::process_line));
}

void
nntp::died (void)
{
  *deleted = true;
  delete this;
}

void
nntp::add_cmd (const char *cmd, cbs fn)
{
  cmd_table.push_back (c_jmp_entry_t (cmd, fn));
}

static rxx cmdrx ("^(.+)");
char *unknown = "500 command not recognized\r\n";

void
nntp::command (void)
{
  unsigned int i = 0;
  str cmd;

  if (cmdrx.search (lines[0])) {  // xxx makes a big str if "in" is big
    for (i = 0; i < cmd_table.size (); i++) {
      if (!strncasecmp (cmdrx[1], cmd_table[i].cmd, cmd_table[i].len)) {
	cmd_table[i].fn (cmdrx[1]);
	break;
      }
    }

    if (posting) {
      lines.pop_front();
      return;
    }

    if (i == cmd_table.size ()) {
      warn << "unknown command: " << cmdrx[1];
      aio << unknown;
    }
  } else {
    aio << unknown;
  }
  lines.pop_front();
}

// --- basic commands

static rxx hellorx ("^MODE (READER)?(STREAM)?", "i");
char *hello = "200 DHash news server - posting allowed\r\n";
char *stream = "203 Streaming is OK\r\n";
char *syntax = "501 command syntax error\r\n";

void
nntp::cmd_hello (str c) {
  warn << "hello: " << c;

  if (hellorx.search (c)) {
    if (hellorx[1]) {
      aio << hello;
      return;
    }
    if (hellorx[2]) {
      aio << stream;
      return;
    }
  }
  aio << syntax;
}

void
nntp::cmd_quit (str c)
{
  warn << "quit " << s << "\n";
  delete this;
}

char *help = "100 help text follow\r\n";
char *period = ".\r\n";

void
nntp::cmd_help (str c)
{
  warn << "help\n";
  aio << help;
  for (unsigned int i = 0; i < cmd_table.size(); i++)
    aio << cmd_table[i].cmd << "\r\n";
  aio << period;
}

// --- list newgroups

// format:  foo 2 1 y\r\n
char *listb = "215 list of newsgroups follows\r\n";

void
nntp::cmd_list (str c) {
  warn << "list\n";
  grouplist g;
  str n;
  unsigned long i;

  aio << listb;
  do {
    g.next (&n, &i);
    aio << n << " " << i << " 1 y\r\n";
  } while (g.more ());
  aio << period;
}

static rxx overrx ("^XOVER( ((\\d+)-)?(\\d+)?)?", "i");
// poorly written regexes follow:
//( (\\d+)?((\\d+)-(\\d+)?)?)?");
// ?(\\d+)?(-)?(\\d+)?");
char *overview = "224 Overview information follows\r\n";
char *nogroup = "412 No newsgroup selected\r\n";

void
nntp::cmd_over (str c) {
  warn << "over " << c;
  unsigned long start, stop = -1UL;

  if (!cur_group.loaded ()) {
    aio << nogroup;
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

    aio << overview;
    do {
      aio << cur_group.next ();
    } while(cur_group.more ());
    aio << period;
  } else {
    aio << syntax;
    warn << "error\n";
  }
}

static rxx grouprx ("^GROUP (.+)$", "i");
char *groupb = "211 ";
char *groupe = " group selected\r\n";
char *badgroup = "411 no such news group\r\n";

void
nntp::cmd_group (str c) {
  warn << "group " << c;
  unsigned long count, first, last;

  if (grouprx.search (c)) {
    if (cur_group.open (grouprx[1], &count, &first, &last) < 0) {
      aio << badgroup;
    } else {
      aio << groupb << count << " " << first << " " << last << " "
	  << cur_group.name () << groupe;
    }
  } else
    aio << syntax;
}

// --- retrieve article

static rxx artrx ("^ARTICLE (.+)\\s*$", "i");
char *noarticle = "430 no such article found\r\n";

void
nntp::cmd_article (str c) {
  warn << "article " << c;
  chordID msgkey;
  ptr<dbrec> key, d;

  if (!cur_group.loaded ()) {
    aio << nogroup;
  } else if (artrx.search (c)) {
    unsigned long cur_art;
    if (convertint(artrx[1], &cur_art)) {
      msgkey = cur_group.getid (cur_art);
      cur_group.cur_art = cur_art;
    } else {
      msgkey = cur_group.getid (artrx[1]);
    } 

    warn << "msgkey " << msgkey << "\n";

    if (msgkey != 0)
      dhash->retrieve (msgkey,
		       wrap (this, &nntp::cmd_article_cb, deleted,
			     false, msgkey));
    else
      aio << noarticle;
  } else {
    aio << syntax;
    warn << "error\n";
  }
}

char *articleb = "220 ";
char *articlee = " article retrieved - head and body follow\r\n";

void
nntp::cmd_article_cb (ptr<bool> deleted, bool head, chordID msgkey,
		      dhash_stat status, ptr<dhash_block> blk, vec<chordID> r)
{
  if (*deleted)
    return;
  if (status != DHASH_OK) {
    aio << noarticle;
    return;
  }
  if (head) {
    aio << articleb << cur_group.cur_art << " " << msgkey << articlee;
  }
  aio << str (blk->data, blk->len) << period;
}

// --- post article

char *postgo = "340 send article to be posted. End with <CR-LF>.<CR-LF>\r\n";
char *postok = "240 article posted ok\r\n";
char *postbad = "441 posting failed\r\n";

void
nntp::cmd_post (str c)
{
  warn << "post\n";
  aio << postgo;
  posting = true;
  process_input = wrap (this, &nntp::read_post, postok, postbad);
}

static rxx postmrx ("^Message-ID: (<.+>)\\s*$", "i");
static rxx postngrx ("^Newsgroups: (.+?)\\s*$", "i");
static rxx postgrx (",?([^,]+)");
static rxx postcontrol ("^Control: (.+?)\\s*$", "m");
static rxx postend ("^\\.$");
static rxx postheadend ("^\\s?$");

void
nntp::read_post (str resp, str bad)
{
  ptr<dbrec> k, d;
  ptr<newsgroup> g;
  str ng, msgid;
  bool posted = false;
  chordID ID;

  if (!postend.search (lines.back())) 
    return;
  lines.pop_back();
  
  int headerend  = 0;
  int msgid_line = 0;

  for (size_t i = 0; i < lines.size(); i++) {
    // warnx << "Checking... ||" << lines[i] << "||\n";
    if (!headerend && postheadend.search (lines[i])) {
      warnx << "headerend = " << i << "\n";
      headerend = i;
      break;
    }
    if (postcontrol.search (lines[i])) {
      docontrol (postcontrol[1]);
      lines.setsize(0);
      return;
    } else if (postmrx.search (lines[i])) {
      warn << "found msgid " << postmrx[1] << "\n";
      msgid = postmrx[1];
      msgid_line = i;
    } else if (postngrx.search (lines[i])) {
      warn << "found newsgroup list " << postngrx[1] << "\n";
      ng = postngrx[1];
    }
  }
  int linecount = lines.size () - headerend;

  strbuf header;
  strbuf body;
  while (headerend > 0) {
    header << lines.pop_front() << "\r\n";
    headerend--;
  }
  while (!lines.empty())
    body << lines.pop_front() << "\r\n";

  str wholeart = strbuf () << header << body;

  ID = compute_hash (wholeart, wholeart.len ()); 
  if (!msgid_line) 
      msgid = strbuf () << "<" << ID << "@usenetDHT>"; // default msgid

  dhash->insert (wholeart, wholeart.len (),
		 wrap (this, &nntp::read_post_cb));

  header << "X-Lines: " << linecount << "\r\n" << "X-ChordID: " << ID << "\r\n";

  str h (header);
  k = New refcounted<dbrec> (msgid, msgid.len ());
  d = New refcounted<dbrec> (h, h.len ());
  header_db->insert(k, d);
  warnx << "----\n" << h << "----\n";

  if (ng) { 
    g = New refcounted<newsgroup> ();
    while (postgrx.search (ng)) {
      if (g->open (postgrx[1]) < 0)
	warn << "tried to post unknown group " << postgrx[1] 
	     << ", ignoring.\n";
      else {
	g->addid (msgid, ID);
	posted = true;
	warn << "posted to " << postgrx[1] << "\n";
      }

      ng = ng + postgrx[0].len ();
    }
  }
    
  if (posted)
    aio << resp;
  else
    aio << bad;

  posting = false;
  process_input = wrap (this, &nntp::command);
}

void
nntp::read_post_cb (dhash_stat status, ptr<insert_info> i)
{
  if (status != DHASH_OK)
    warn << "didn't store in DHASH after all\n";
}

void
nntp::docontrol (str ctrl)
{
  warn << "received control message: " << ctrl << "\n";
  // xxx store control message in dhash?
  // xxx propogate control message?
}

// --- News transfer commands:

char *ihavesend = "335 send article to be transferred.  End with <CR-LF>.<CR-LF>\r\n";
char *ihaveok = "235 article transferred ok\r\n";
char *ihaveno = "435 article not wanted - do not send it\r\n";
char *ihavebad = "436 transfer failed - try again later\r\n";
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
      aio << ihavesend;
      posting = true;
      process_input = wrap (this, &nntp::read_post, ihaveok, ihavebad);
    } else
      aio << ihaveno;
  } else
    aio << syntax;
}    

static rxx checkrx ("^CHECK (<.+?>)", "i");
char *checksendb = "238 ";
char *checksende = " no such article found, please send it to me\r\n";
char *checknob = "438 ";
char *checknoe = " already have it, please don't send it to me\r\n";

void
nntp::cmd_check (str c)
{
  warn << "check " << c;
  ptr<dbrec> key, d;

  if (checkrx.search (c)) {
    key = New refcounted<dbrec> (checkrx[1], checkrx[1].len ());
    d = header_db->lookup (key);
    if (!d)
      aio << checksendb << checkrx[1] << checksende;
    else
      aio << checknob << checkrx[1] << checknoe;
  } else
    aio << syntax;
}

static rxx takethisrx ("^TAKETHIS (<.+?>)", "i");
char *takethisokb = "239 ";
char *takethisoke = " article transferred ok\r\n";
char *takethisbadb = "439 ";
char *takethisbade = " article transfer failed\r\n";

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
    aio << syntax;
}
