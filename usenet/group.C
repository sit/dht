#include <rxx.h>

#include <usenet.h>
#include <group.h>

grouplist::grouplist ()
{
  it = group_db->enumerate();
  d = it->nextElement();    
}

void
grouplist::next (str *f, unsigned long *i)
{
  *f = str (d->key->value, d->key->len);

  ptr<dbrec> rec = group_db->lookup (d->key);

  xdrmem x (rec->value, rec->len, XDR_DECODE);
  group_entry *group = New group_entry;
  bzero (group, sizeof (*group));
  if (xdr_group_entry (x.xdrp (), group)) {
    *i = group->articles.size () ;
    xdr_delete (reinterpret_cast<xdrproc_t> (xdr_group_entry), group);
  } else {
    *i = 0;
  }

  d = it->nextElement();
}


newsgroup::newsgroup () :
  group (NULL),
  start (0),
  stop (0),
  next_idx (0),
  group_name_rec (NULL),
  cur_art (0)
{
}

newsgroup::~newsgroup () 
{
  if (group) {
    xdr_delete (reinterpret_cast<xdrproc_t> (xdr_group_entry), group);
    group = NULL;
  }
}

group_entry *
newsgroup::load (ptr<dbrec> g)
{
  ptr<dbrec> rec (NULL);
  rec = group_db->lookup (g);
  if (rec == NULL)
    return NULL; 

  xdrmem x (rec->value, rec->len, XDR_DECODE);
  group_entry *group = New group_entry;
  bzero (group, sizeof (*group));
  if (xdr_group_entry (x.xdrp (), group)) 
    return group;
  xdr_delete (reinterpret_cast<xdrproc_t> (xdr_group_entry), group);
  group = NULL;
  return NULL;
}

int
newsgroup::open (str g)
{
  ptr<dbrec> gn = New refcounted<dbrec> (g, g.len ());
  group = load (gn);
  if (group == NULL)
    return -1;

  group_name = g;
  group_name_rec = gn;
  cur_art = 1;

  return 0;
}

int
newsgroup::open (str g, unsigned long *count,
	         unsigned long *first, unsigned long *last)
{
  if (open (g) < 0)
    return -1;

  *count = 0;
  *first = 0;
  *last  = 0;

  // Find the first and last article numbers and also
  // how many articles there are.
  *count = group->articles.size ();
  if (*count > 0) {
    *first = group->articles[0].artno;
    *last  = group->articles[*count - 1].artno;
  }

  return 0;
}

void
newsgroup::addid (str msgid, chordID ID)
{
  // Must get a new copy of data from disk in case of
  // other "simultaneous" connections.
  if (group) {
    xdr_delete (reinterpret_cast<xdrproc_t> (xdr_group_entry), group);
    group = NULL;
  }
  group = load (group_name_rec);
  if (!group) {
    warn << "newsgroup::addid: load failed\n";
    return;
  }

  // Find next article number 
  u_int32_t article_count = group->articles.size (); 
  u_int32_t lastno = 0;
  if (article_count > 0)
    lastno = group->articles[article_count - 1].artno;

  // Add new article to cached listing
  article_mapping nart;
  nart.artno = lastno + 1;
  nart.msgid = msgid;
  nart.blkid = ID;
  group->articles.push_back (nart);

#if 0
  // Dump the complete list of articles for this group
  for (size_t i = 0; i < group->articles.size (); i++)
    warn << i << " " 
         << group->articles[i].artno << " "
         << group->articles[i].msgid << " "
         << group->articles[i].blkid << "\n";
#endif /* 0 */  

  // Marshal listing
  xdrsuio x (XDR_ENCODE);
  if (!xdr_group_entry (x.xdrp (), group)) {
    warn << "newsgroup::addid: marshalling failed\n";
    return;
  }
  mstr m (x.uio ()->resid ());
  x.uio ()->copyout (m);

  // And schedule a write.
  ptr<dbrec> rec = New refcounted<dbrec> (m, m.len ());
  group_db->insert (group_name_rec, rec);
}

// now returns the chordid
chordID
newsgroup::getid (unsigned long index)
{
  if (!loaded ())
    return 0;

  for (size_t i = 0; i < group->articles.size (); i++) {
    if (index == group->articles[i].artno)
      return group->articles[i].blkid;
    else if (group->articles[i].artno > index)
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
  assert (loaded ());

  start = a;
  stop = b;
  for (next_idx = 0; next_idx < group->articles.size (); next_idx++)
    if (group->articles[next_idx].artno >= start)
      break;
}

bool
newsgroup::more ()
{ 
  return start < stop && next_idx < group->articles.size ();
}

static rxx oversub ("Subject: (.+)\\r");
static rxx overfrom ("From: (.+)\\r");
static rxx overdate ("Date: (.+)\\r");
static rxx overmsgid ("Message-ID: (.+)\\r");
static rxx overref ("References: (.+)\\r");
static rxx overlines ("X-Lines: (.+)\\r");

static str
tabfilter (str f)
{
  mstr out (f.len ());
  char *d = out;

  for (unsigned int i = 0; i < f.len (); i++) {
    d[i] = f[i];
    if (d[i] == '\t')
      d[i] = ' ';
  }

  return out;
}

// format: "subject\tauthor\tdate\t<msgid>\treferences\tsize\tlines"
strbuf
newsgroup::next (void)
{
  ptr<dbrec> art (NULL);
  article_mapping m;
  strbuf resp;

  while (more () && art == NULL)
  {
    m = group->articles[next_idx];
    ptr<dbrec> k = New refcounted<dbrec> (m.msgid.cstr (), m.msgid.len ());
    next_idx++;

    art = header_db->lookup (k);
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
      resp << tabfilter (m.msgid) << "\t";
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
    warn << "msg " << m.msgid << "\n";
    warn << "header " << msg << "\n";
  }
  
  return resp;
}
