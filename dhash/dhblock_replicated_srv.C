#include <chord.h>
#include <dhash.h>
#include <dhash_common.h>

#include <configurator.h>
#include <location.h>
#include <dbfe.h>

#include "dhash_store.h"
#include "dhblock_replicated.h"
#include "dhblock_replicated_srv.h"

#include <merkle_misc.h>

#include <modlogger.h>
#define warning modlogger ("dhblock_replicated", modlogger::WARNING)
#define info    modlogger ("dhblock_replicated", modlogger::INFO)
#define trace   modlogger ("dhblock_replicated", modlogger::TRACE)

#ifdef DMALLOC
#include <dmalloc.h>
#endif


dhblock_replicated_srv::dhblock_replicated_srv (ptr<vnode> node,
						str desc,
						str dbname,
						str dbext,
						dhash_ctype ctype) :
  dhblock_srv (node, desc, dbname, dbext),
  ctype (ctype),
  nrpcsout (0),
  checkrep_tcb (NULL),
  checkrep_interval (300)
{
  bool ok = Configurator::only ().get_int ("dhash.repair_timer",
      checkrep_interval);
  assert (ok);
}

dhblock_replicated_srv::~dhblock_replicated_srv ()
{
  stop ();
}

void
dhblock_replicated_srv::store (chordID key, str d, cbi cb)
{

  db->fetch (key, wrap (this, &dhblock_replicated_srv::store_fetch_cb, 
			cb, d));
}


void 
dhblock_replicated_srv::delete_cb (chordID k, str d, cbi cb, int stat) 
{
  assert (stat == ADB_OK);
  info << "db write: " << node->my_ID ()
       << " U " << k << " " << d.len () << "\n";
  
  db->store (k, d, cb);

};

void
dhblock_replicated_srv::store_fetch_cb (cbi cb, str d,
					adb_status stat, 
					chordID key,
					str prev)
{
  if (stat == ADB_OK) {
    if (is_block_stale (prev, d)) {
      chordID p = node->my_pred ()->id ();
      chordID m = node->my_ID ();
      if (betweenrightincl (p, m, key))
	cb (DHASH_STALE);
    }
    else {
      info << "db delete: " << key << "\n";
      db->remove (key, wrap (this, &dhblock_replicated_srv::delete_cb, key, d, cb));
    }
  }

}


