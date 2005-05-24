#include <chord.h>
#include "dhash.h"
#include "dhash_common.h"
#include "dhashcli.h"

#include <dbfe.h>
#include <dhblock_srv.h>

#include <merkle_misc.h>

dhblock_srv::dhblock_srv (ptr<vnode> node,
		          str desc,
			  str dbname,
			  dbOptions opts) :
  node (node),
  desc (desc),
  synctimer (NULL),
  cli (NULL)
{
  // XXX share cli with dhash object??
  cli = New refcounted<dhashcli> (node);

  db = New refcounted<dbfe> ();

  if (int err = db->opendb (const_cast <char *> (dbname.cstr ()), opts))
  {
    warn << desc << ": " << dbname <<"\n";
    warn << "open returned: " << strerror (err) << "\n";
    exit (-1);
  }
}

dhblock_srv::~dhblock_srv ()
{
  stop ();
}

void
dhblock_srv::start (bool randomize)
{
  int delay = 0;
  if (!synctimer) {
    if (randomize)
      delay = random_getword () % dhash::synctm ();
    synctimer = delaycb (dhash::synctm () + delay,
	wrap (this, &dhblock_srv::sync_cb));
  }
}

void
dhblock_srv::stop ()
{
  if (synctimer) {
    timecb_remove (synctimer);
    synctimer = NULL;
  }
}

void
dhblock_srv::sync_cb ()
{
  // Probably only one of sync or checkpoint is needed.
  db->sync ();
  db->checkpoint ();

  synctimer = delaycb (dhash::synctm (), wrap (this, &dhblock_srv::sync_cb));
}

ptr<dbrec>
dhblock_srv::fetch (chordID k)
{
  ptr<dbrec> id = id2dbrec (k);
  return db->lookup (id);
}

bool
dhblock_srv::key_present (const blockID &n)
{
  ptr<dbrec> val = fetch (n.ID);
  return (val != NULL);
}

void
dhblock_srv::stats (vec<dstat> &s)
{
  // Default implementation adds no statistics
  return;
}


const strbuf &
dhblock_srv::key_info (const strbuf &sb)
{
  return sb;
}


void
dhblock_srv::offer (user_args *sbp, dhash_offer_arg *arg)
{
  sbp->replyref (NULL);
}

void
dhblock_srv::bsmupdate (user_args *sbp, dhash_bsmupdate_arg *arg)
{
  sbp->replyref (NULL);
}
