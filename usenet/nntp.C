#include <aios.h>
#include <parseopt.h>
#include <rxx.h>
#include <dhash_common.h>
#include <dhashclient.h>
#include <verify.h>
#include <dbfe.h>

#include "usenet.h"
#include "group.h"
#include "nntp.h"

/**
 * Setting nntp_trace causes more debugging information to be emitted.
 *   0  None
 *   1  Connection open/close
 *   2  Command dispatch
 *   3  Article posting
 *   5  Header parsing
 *   8  Command parsing
 *   9  Complete client I/O
 */
static int nntp_trace (getenv ("NNTP_TRACE") ? atoi (getenv ("NNTP_TRACE")) : 0);
u_int64_t nntp::nconn_ (0);
u_int64_t nntp::fedinbytes_ (0);
u_int64_t nntp::dhashbytes_ (0);

nntp::nntp (int _s) :
	s (_s),
	aio (aios::alloc (_s)),
	cur_group (New refcounted<newsgroup> ()),
	process_input (wrap (this, &nntp::command)),
        posting (false)
{
  nconn_++;
  if (nntp_trace >= 1)
    warn << s << ": connected\n";
  aio->settimeout (opt->client_timeout);
  if (nntp_trace >= 9)
    aio->setdebug (strbuf("%d", s));

  deleted = New refcounted<bool> (false);

  cmd_hello ("READER");

  add_cmd ("CHECKDHT", wrap (this, &nntp::cmd_check));
  add_cmd ("TAKEDHT", wrap (this, &nntp::cmd_takethis, true));

  add_cmd ("CHECK", wrap (this, &nntp::cmd_check));
  add_cmd ("TAKETHIS", wrap (this, &nntp::cmd_takethis, false));

  add_cmd ("IHAVE", wrap (this, &nntp::cmd_ihave));

  add_cmd ("ARTICLE", wrap (this, &nntp::cmd_article));
  add_cmd ("XOVER", wrap (this, &nntp::cmd_over));
  add_cmd ("GROUP", wrap (this, &nntp::cmd_group));
  add_cmd ("LIST", wrap (this, &nntp::cmd_list));
  add_cmd ("POST", wrap (this, &nntp::cmd_post));

  add_cmd ("MODE", wrap (this, &nntp::cmd_hello));
  add_cmd ("HELP", wrap (this, &nntp::cmd_help));
  add_cmd ("QUIT", wrap (this, &nntp::cmd_quit));

  add_cmd ("STATS", wrap (this, &nntp::cmd_stats));

  aio->readline (wrap (this, &nntp::process_line));
}

nntp::~nntp (void)
{
  *deleted = true;
  if (nntp_trace >= 1)
    warn << s << ": closed\n";
  // s is closed by aio destructor
}

void
nntp::process_line (const str data, int err)
{
  if (err < 0) {
    warn << s << ": nntp aio oops " << err << "\n";
    if (err == ETIMEDOUT) {
      aio << "205 Timed out.\r\n";
      delete this;
      return;
    }
  }
  if (!data || !data.len()) {
    warn << s << ": nntp data oops\n";
    delete this;
    return;
  }
  lines.push_back (data);
  if (posting)
    fedinbytes_ += data.len ();
  
  ptr<bool> d = deleted;
  process_input ();
  if (!*d)
    aio->readline (wrap (this, &nntp::process_line));
}

void
nntp::add_cmd (const char *cmd, cbs fn)
{
  cmd_table.push_back (c_jmp_entry_t (cmd, fn));
}

char *unknown = "500 command not recognized\r\n";

void
nntp::command (void)
{
  unsigned int i = 0;
  
  str line = lines.pop_front ();
  vec<str> cmdargs;
  int n = split (&cmdargs, rxx("\\s+"), line, 2);
  assert (n >= 0);
  if (n == 0)
    return;
  
  for (i = 0; i < cmd_table.size (); i++) {
    if (!strcasecmp (cmdargs[0], cmd_table[i].cmd)) {
      if (nntp_trace >= 2)
	warn << s << ": dispatching " << cmdargs[0] << "\n";
      cmd_table[i].fn ((n > 1) ? cmdargs[1] : str(""));
      return;
    }
  }

  if (nntp_trace >= 2)
    warn << s << ": unknown command: " << cmdargs[0] << "\n";
  aio << unknown;
}

// --- basic commands

char *hello = "200 DHash news server - posting allowed\r\n";
char *stream = "203 Streaming is OK\r\n";
char *syntax = "501 command syntax error\r\n";

void
nntp::cmd_hello (str c) {
  if (c) {
    if (!strcasecmp (c, "reader")) {
      aio << hello;
      return;
    }
    else if (!strcasecmp (c, "stream")) {
      aio << stream;
      return;
    }
  }
  
  aio << syntax;
}

void
nntp::cmd_quit (str c)
{
  aio << "205 thank you, come again.\r\n";
  delete this;
}

char *help = "100 help text follow\r\n";
char *period = ".\r\n";

void
nntp::cmd_help (str c)
{
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
  grouplist g;
  str n;
  unsigned long i;

  aio << listb;
  while (g.more ())
  {
    g.next (&n, &i);
    aio << n << " " << i << " 1 y\r\n";
  }
  aio << period;
}

static rxx overrx ("((\\d+)-)?(\\d+)?", "i");
// poorly written regexes follow:
//( (\\d+)?((\\d+)-(\\d+)?)?)?");
// ?(\\d+)?(-)?(\\d+)?");
char *overview = "224 Overview information follows\r\n";
char *nogroup = "412 No newsgroup selected\r\n";

void
nntp::cmd_over (str c) {
  unsigned long start, stop = -1UL;
  
  if (!cur_group->loaded ()) {
    aio << nogroup;
  } else if (!c) {
    start = stop = cur_group->cur_art;
  } else {
    vec<str> limits;
    int n = split (&limits, rxx("-"), c, (size_t) -1, true);
    warn << s << ": xover " << c << " splits to " << n << "/" << limits.size();
    for (size_t i = 0; i < limits.size (); i++)
      warnx << " '" << limits[i] << "'";
    warnx << "\n";
    if (n > 0) {
      // grab the first number...
      if (!convertint (limits[0], &start)) {
	aio << syntax;
	return;
      }
      // and see if there is a second...
      if (n > 1) {
	if (limits[1].len () > 0) {
	  if (!convertint (limits[1], &stop)) {
	    aio << syntax;
	    return;
	  }
	} else {
	  // get all until the end; counter the plus one below...
	  stop -= 1; 
	}
      } else {
	// single
	stop = start;
      }
    } 
    cur_group->xover (start, stop + 1);
    aio << overview;
    do {
      aio << cur_group->next ();
    } while (cur_group->more ());
    aio << period;
  }
}

char *groupb = "211 ";
char *groupe = " group selected\r\n";
char *badgroup = "411 no such news group\r\n";

void
nntp::cmd_group (str c) {
  unsigned long count, first, last;

  if (c) {
    if (cur_group->open (c, &count, &first, &last) < 0) {
      aio << badgroup;
    } else {
      aio << groupb << count << " " << first << " " << last << " "
	  << cur_group->name () << groupe;
    }
  } else
    aio << syntax;
}

// --- retrieve article
char *noarticle = "430 no such article found\r\n";

void
nntp::cmd_article (str c) {
  chordID msgkey;
  ptr<dbrec> key, d;

  if (!cur_group->loaded ()) {
    aio << nogroup;
  } else {
    unsigned long cur_art;
    if (!c) {
      msgkey = cur_group->getid (cur_group->cur_art);
    }
    else if (convertint (c, &cur_art)) {
      msgkey = cur_group->getid (cur_art);
      cur_group->cur_art = cur_art;
    } else {
      msgkey = cur_group->getid (c);
    }

    if (nntp_trace >= 8)
      warn << s << ": msgkey " << msgkey << "\n";

    if (msgkey != 0)
      dhash->retrieve (msgkey,
		       wrap (this, &nntp::cmd_article_cb, deleted,
			     true, msgkey));
    else
      aio << noarticle;
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
    aio << articleb << cur_group->cur_art << " " << msgkey << articlee;
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
  aio << postgo;
  posting = true;
  process_input = wrap (this, &nntp::read_post, postok, postbad, false);
}

static rxx postmrx ("^Message-ID: (<.+>)\\s*$", "i");
static rxx postngrx ("^Newsgroups: (.+?)\\s*$", "i");
static rxx postgrx (",?([^,]+)");
static rxx postcontrol ("^Control: (.+?)\\s*$", "i");
static rxx postend ("^\\.$");
static rxx postheadend ("^\\s?$");
static rxx postchordid ("^X-ChordID: (.+?)\\s*$", "i");

void
nntp::read_post (str resp, str bad, bool takedht)
{
  str ng, msgid;
  chordID ID;
  
  strbuf prefix ("%d: read_post: ", s);

  if (!postend.search (lines.back())) 
    return;

  lines.pop_back();
  posting = false;
  process_input = wrap (this, &nntp::command);
    
  int headerend  = 0;

  for (size_t i = 0; i < lines.size (); i++) {
    // warnx << "Checking... ||" << lines[i] << "||\n";
    if (!headerend && postheadend.search (lines[i])) {
      if (nntp_trace >= 8)
	warn << prefix << "headerend = " << i << "\n";
      headerend = i;
      break;
    }
    if (postcontrol.search (lines[i])) {
      docontrol (postcontrol[1]);
      lines.setsize (0);
      aio << resp;
      return;
    } else if (postmrx.search (lines[i])) {
      if (nntp_trace >= 8)
	warn << prefix << "found msgid " << postmrx[1] << "\n";
      msgid = postmrx[1];
    } else if (postngrx.search (lines[i])) {
      if (nntp_trace >= 8)
	warn << prefix << "found newsgroup list " << postngrx[1] << "\n";
      ng = postngrx[1];
    } else if (postchordid.search (lines[i])) {
      if (nntp_trace >= 8) {
	warn << prefix << "found "
	     << (takedht ? "" : "un") << "expected chordID "
	     << postchordid[1] << "\n";
	ID = bigint (postchordid[1], 16);
      }
    }
  }
  int linecount = lines.size () - headerend;

  if (!msgid || !ng || linecount <= 0 || (takedht && !ID)) {
    aio << bad;
    lines.setsize (0);
    return;
  } else
    aio << resp;

  // Satisified that we have received a valid article;
  // now try and post it somewhere.
  bool posted (false);
  strbuf header;
  strbuf body;
  while (headerend > 0) {
    header << lines.pop_front() << "\r\n";
    headerend--;
  }
  while (!lines.empty())
    body << lines.pop_front() << "\r\n";

  str wholeart = strbuf () << header << body;

  if (!takedht) {
    ID = compute_hash (wholeart, wholeart.len ());  
    header << "X-Lines: " << linecount << "\r\n"
	   << "X-ChordID: " << ID << "\r\n";
  }
  str h (header);
  if (nntp_trace >= 5)
    warn << "----\n" << h << "----\n";

  vec<str> groups;
  ptr<newsgroup> g = New refcounted<newsgroup> ();
  while (postgrx.search (ng)) {
    groups.push_back (postgrx[1]);
    strbuf postlog;
    postlog << prefix << "group " << postgrx[1] << ": ";
    if (g->open (postgrx[1]) < 0) {
      // Initial open failure, try to create group if allowed by opts.
      if (opt->create_unknown_groups) {
	if (create_group (postgrx[1]) && 
	    g->open (postgrx[1]) >= 0)
	  postlog << "created, ";
	else
	  postlog << "creation failed!\n";
      } else {
	postlog << "unknown, so ignoring.\n";
      }
    } 
    if (g->loaded ()) {
      g->addid (msgid, ID);
      posted = true;
      postlog << msgid << " (" << ID << ") posted.\n";
    }
    if (nntp_trace >= 3)
      warn << postlog;
    
    ng = ng + postgrx[0].len ();
  }
  
  if (posted) {
    ptr<dbrec> k = New refcounted<dbrec> (msgid, msgid.len ());
    ptr<dbrec> d = New refcounted<dbrec> (h, h.len ());
    header_db->insert (k, d);
    if (!takedht) {
      dhash->insert (wholeart, wholeart.len (),
		     wrap (this, &nntp::read_post_cb,
			   wholeart.len (), k, groups));
    }
  }
}

void
nntp::read_post_cb (size_t len, ptr<dbrec> msgid, vec<str> groups,
		    dhash_stat status, ptr<insert_info> i)
{
  str k (msgid->value, msgid->len);
  if (status == DHASH_OK) {
    dhashbytes_ += len;
    feed_article (k, groups);
    return;
  }
  // Clean up state after something goes wrong...
  warn << s << ": didn't store " << k
       << " in DHash after all: " << status << "\n";
  header_db->del (msgid);
}

void
nntp::docontrol (str ctrl)
{
  warn << s << ": received control message: " << ctrl << "\n";
  // xxx store control message in dhash?
  // xxx propogate control message?
}

// --- News transfer commands:

char *ihavesend = "335 send article to be transferred.  End with <CR-LF>.<CR-LF>\r\n";
char *ihaveok = "235 article transferred ok\r\n";
char *ihaveno = "435 article not wanted - do not send it\r\n";
char *ihavebad = "436 transfer failed - try again later\r\n";

void
nntp::cmd_ihave (str c)
{
  ptr<dbrec> key, d;

  if (c.len ()) {
    key = New refcounted<dbrec> (c, c.len ());
    d = header_db->lookup (key);
    if (!d) {
      aio << ihavesend;
      posting = true;
      process_input = wrap (this, &nntp::read_post, ihaveok, ihavebad, false);
    } else
      aio << ihaveno;
  } else
    aio << syntax;
}    

char *checksendb = "238 ";
char *checksende = " no such article found, please send it to me\r\n";
char *checknob = "438 ";
char *checknoe = " already have it, please don't send it to me\r\n";

void
nntp::cmd_check (str c)
{
  ptr<dbrec> key, d;

  if (c.len ()) {
    key = New refcounted<dbrec> (c, c.len ());
    d = header_db->lookup (key);
    if (!d)
      aio << checksendb << c << checksende;
    else
      aio << checknob << c << checknoe;
  } else
    aio << syntax;
}

char *takethisokb = "239 ";
char *takethisoke = " article transferred ok\r\n";
char *takethisbadb = "439 ";
char *takethisbade = " article transfer failed\r\n";

void
nntp::cmd_takethis (bool takedht, str c)
{
  str resp, bad;

  if (c.len ()) {
    resp = strbuf () << takethisokb << c << takethisoke;
    bad = strbuf () << takethisbadb << c << takethisbade;
    posting = true;
    process_input = wrap (this, &nntp::read_post, resp, bad, takedht);
  } else
    aio << syntax;
}

void
nntp::cmd_stats (str c)
{
  aio << "280 stats to follow\r\n";
  aio << collect_stats ();
  aio << period;
}
