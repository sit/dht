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

  return 0;
}

int
group::open (str g, volatile unsigned long *count,
	     unsigned long *first, unsigned long *last)
{
  if (open (g) < 0)
    return -1;

  char *c = rec->value;
  int len = rec->len;
  warn << "xv " << str (c, len) << "\n";
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
group::addid (str msgid, chordID ID)
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
group::getid (unsigned long index)
{
  chordID ID;

  if (rec == NULL)
    return 0;

  char *c = rec->value;
  int len = rec->len;

  for (; listrx.search (str (c, len))
       ; c += listrx.len (0), len -= listrx.len (0) ) {
    if (index == strtoul (listrx[1], NULL, 10)) {
      warn << "got ID " << listrx[3] << "\n";
      ID = listrx[3].cstr ();
      //      mpz_set_rawmag_be (&ID, listrx[3], sha1::hashsize);
      return bigint (listrx[3], 16);
    } else if (index < strtoul (listrx[1], NULL, 10))
      return 0;
  }

  return 0; // xxx bad?
}

chordID
group::getid (str msgid)
{
  chordID ID;

  if (rec == NULL)
    return 0;

  char *c = rec->value;
  int len = rec->len;

  for (; listrx.search (str (c, len))
       ; c += listrx.len (0), len -= listrx.len (0) ) {
    if (msgid == listrx[2]) {
      mpz_set_rawmag_be (&ID, listrx[3], sha1::hashsize);
      return ID;
    }
  }

  return 0; // xxx bad?
}

void
group::xover (unsigned long a, unsigned long b)
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

// format: "subject\tauthor\tdate\t<msgid>\treferences\tsize\tlines"
strbuf
group::next (void)
{
  ptr<dbrec> art;
  strbuf resp;
  resp << start << "\t";

  if (more () &&
      listrx.search (str (c, len))) {
#if 0
    dhash->retrieve (listrx[3], wrap (this, &group::next_cb, listrx[2]));

    warn << "mgs " << listrx[3] << "\n";
#endif
    art = header_db->lookup(New refcounted<dbrec> (listrx[2],
						   listrx[2].len ()));

    warn << "mgs " << listrx[2] << "\n";

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
	  resp << listrx[2] << "\t";
	if (overref.search (msg))
	  resp << overref[1];
	for (i = 0; i < art->len; i++)
	  if (art->value[i] == '\n')
	    line++; // xxx make this only count lines of body
	resp << "\t" << art->len  << "\t" << line;
	// xxx filter out tabs
      } else
	warn << "msg parse error\n";
    }

    c += listrx.len (0);
    len -= listrx.len (0);
  }
  
  start++;
  resp << "\t\r\n";
  return resp;
}

#if 0
void
group::next_cb (str altmsgid, 
		dhash_stat status, ptr<dhash_block> blk, vec<chordID> r)
{
  if (status != DHASH_OK)
    warn << "missing article\n";
  else {
    str msg (blk->data, blk->len); // xxx
    int i, line = 0;
    if (oversub.search (msg) &&
	overfrom.search (msg) &&
	overdate.search (msg)) {
      resp << oversub[1] << "\t" << overfrom[1] << "\t";
      resp << overdate[1] << "\t";

      if (overmsgid.search (msg))
	resp << overmsgid[1] << "\t";
      else
	resp << altmsgid << "\t";
      if (overref.search (msg))
	resp << overref[1];
      for (i = 0; i < blk->len; i++)
	if (blk->data[i] == '\n')
	  line++; // xxx make this only count lines of body
      resp << "\t" << blk->len  << "\t" << line;
      // xxx filter out tabs
    } else
      warn << "msg parse error\n";
  }
}
#endif
