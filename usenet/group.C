#include <rxx.h>

#include <usenet.h>
#include <group.h>

grouplist::grouplist ()
{
  it = group_db->enumerate();
  d = it->nextElement();    
};

static rxx listrx ("^(\\d+)(<[^>]+>)([^:]+):");

void
grouplist::next (str *f, unsigned long *i)
{
  ptr<dbrec> data = group_db->lookup (d->key);
  char *c = data->value;
  int len = data->len, cnt = 0;

  for (; listrx.search (str (c, len))
       ; c += listrx.len (0), len -= listrx.len (0) )
    cnt++;

  *f = str (d->key->value, d->key->len);
  *i = cnt;
  d = it->nextElement();
}


newsgroup::newsgroup () :
  rec (NULL),
  cur_art (0),
  start (0),
  stop (0),
  c (NULL),
  len (0)
{
}

int
newsgroup::open (str g)
{
  rec = group_db->lookup (New refcounted<dbrec> (g, g.len ()));
  if (rec == NULL)
    return -1;

  warn << "group " << g << " rec len " << rec->len << "\n";

  group_name = g;
  cur_art = 1;

  return 0;
}

int
newsgroup::open (str g, unsigned long *count,
	     unsigned long *first, unsigned long *last)
{
  if (open (g) < 0)
    return -1;

  char *c = rec->value;
  int len = rec->len;
  *count = 0;
  *first = 0;
  *last = 0;

  for (; listrx.search (str (c, len))
       ; c += listrx.len (0), len -= listrx.len (0) ) {
    if (*count == 0)
      *first = strtoul (listrx[1], NULL, 10);
    (*count)++;
    *last = strtoul (listrx[1], NULL, 10);
  }

  return 0;
}

static rxx listrxend ("(\\d+)(<[^>]+>)([^:]+):$");

void
newsgroup::addid (str msgid, chordID ID)
{
  assert (rec);
  str old (rec->value, rec->len), updated;
  ptr<dbrec> k = New refcounted<dbrec> (group_name, group_name.len ());

  if (listrxend.search (old))
    updated = strbuf () << old <<
      strtoul (listrxend[1], NULL, 10) + 1 << msgid << ID << ":";
  else
    updated = strbuf () << "1" << msgid << ID << ":";

  rec = New refcounted<dbrec> (updated, updated.len ());
  
  group_db->insert (k, rec);
}

// now returns the chordid
chordID
newsgroup::getid (unsigned long index)
{
  if (rec == NULL)
    return 0;

  char *c = rec->value;
  int len = rec->len;

  for (; listrx.search (str (c, len))
       ; c += listrx.len (0), len -= listrx.len (0) ) {
    if (index == strtoul (listrx[1], NULL, 10))
      return bigint (listrx[3], 16);
    else if (index < strtoul (listrx[1], NULL, 10))
      return 0;
  }

  return 0;
}

static rxx getchordid ("^X-ChordID: (.+)$", "m");
chordID
newsgroup::getid (str msgid)
{
  ptr<dbrec> key, d;

  key = New refcounted<dbrec> (msgid, msgid.len ());
  d = header_db->lookup (key);

  if (d && getchordid.search (str (d->value, d->len)))
    return bigint (getchordid[1], 16);
  else
    return 0;
}

void
newsgroup::xover (unsigned long a, unsigned long b)
{
  start = a;
  stop = b;

  assert (rec);
  c = rec->value;
  len = rec->len;
  while (listrx.search (str (c, len)) &&
	 (strtoul (listrx[1], NULL, 10) < start))
    c += listrx.len (0), len -= listrx.len (0);
}

static rxx oversub ("Subject: (.+)\\r");
static rxx overfrom ("From: (.+)\\r");
static rxx overdate ("Date: (.+)\\r");
static rxx overmsgid ("Message-ID: (.+)\\r");
static rxx overref ("References: (.+)\\r");
static rxx overlines ("X-Lines: (.+)\\r");

str
tabfilter (str f)
{
  unsigned int i;
  strbuf out;

  for (i=0; i<f.len (); i++)
    if (f[i] == '\t')
      out << " ";
    else
      out.tosuio ()->copy (f.cstr() + i, 1); // xxx bleh

  return str (out);
}

// format: "subject\tauthor\tdate\t<msgid>\treferences\tsize\tlines"
strbuf
newsgroup::next (void)
{
  ptr<dbrec> art (NULL);
  strbuf resp;

  while (more () &&
	 listrx.search (str (c, len)) &&
	 art == NULL)
  {
    art = header_db->lookup (New refcounted<dbrec> (listrx[2],
						    listrx[2].len ()));
    c += listrx.len (0);
    len -= listrx.len (0);
    if (art == NULL)
      warn << "missing article " << start << "\n";
    start++;
  }
    
  if (art == NULL)
    return resp;
  
  str msg (art->value, art->len);
  
  if (oversub.search (msg) &&
      overfrom.search (msg) &&
      overdate.search (msg)) {
    resp << start - 1 << "\t";
    resp << tabfilter (oversub[1]) << "\t" 
	 << tabfilter (overfrom[1]) << "\t";
    resp << tabfilter (overdate[1]) << "\t";
    
    if (overmsgid.search (msg))
      resp << tabfilter (overmsgid[1]) << "\t";
    else
      resp << tabfilter (listrx[2]) << "\t";
    if (overref.search (msg))
      resp << tabfilter (overref[1]);
    resp << "\t" << art->len << "\t";
    if (overlines.search (msg))
      resp << tabfilter (overlines[1]);
    else
      resp << "0";
    resp << "\t\r\n";
  } else {
    warn << "msg parse error\n";
    warn << "msg " << listrx[2] << "\n";
    warn << "header " << msg << "\n";
  }
  
  return resp;
}
