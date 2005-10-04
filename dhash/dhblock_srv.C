#include <chord.h>
#include "dhash.h"
#include "dhash_common.h"
#include "dhashcli.h"

#include <dbfe.h>
#include <dhblock_srv.h>

#include <merkle_misc.h>

dhblock_srv::dhblock_srv (ptr<vnode> node,
			  ptr<dhashcli> cli,
		          str desc,
			  str dbname,
			  str dbext) :
  node (node),
  desc (desc),
  synctimer (NULL),
  cli (cli),
  repair_outstanding (0)
{

  db = New refcounted<adb> (dbname, dbext);
  warn << "opened " << dbname << " with space " << dbext << "\n";
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

void
dhblock_srv::fetch (chordID k, cb_fetch cb)
{
  db->fetch (k, cb);
}

#ifdef DDD
bool
dhblock_srv::key_present (const blockID &n)
{
  ptr<dbrec> val = fetch (n.ID);
  return (val != NULL);
}
#endif

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


// Handling repair queuing:

void
dhblock_srv::repair_done ()
{
  repair_outstanding--;
  while ((repair_outstanding <= REPAIR_OUTSTANDING_MAX)
	 && (repair_q.size () > 0)) {
    repair_state *s = repair_q.first ();
    assert (s);
    repair_q.remove (s);
    repair (s->key, s->where);
    delete s;
  }
}

bool
dhblock_srv::repair (blockID k, ptr<location> to)
{

  assert (repair_outstanding >= 0);
  // throttle the block downloads
  if (repair_outstanding > REPAIR_OUTSTANDING_MAX) {
    if (repair_q[k.ID] == NULL) {
      repair_state *s = New repair_state (k, to);
      repair_q.insert (s);
    }
    return false;
  }

  // Be very careful about maintaining this counter.
  // Trust the subclass to call repair_done whenever a branch finishes.
  repair_outstanding++;
  return true;

}
