#include <rxx.h>

#include <usenet.h>
#include <group.h>

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
