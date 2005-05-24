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
					  dbOptions opts,
					  dhash_ctype ctype) :
  dhblock_srv (node, desc, dbname, opts),
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

dhash_stat
dhblock_replicated_srv::store (chordID key, ptr<dbrec> d)
{
  dhash_stat stat (DHASH_OK);

  ref<dbrec> k = id2dbrec (key);
  ptr<dbrec> prev = db->lookup (k);
  if (prev) {
    if (is_block_stale (prev, d)) {
      chordID p = node->my_pred ()->id ();
      chordID m = node->my_ID ();
      if (betweenrightincl (p, m, key))
	stat = DHASH_STALE;
    }
    else {
      info << "db delete: " << key << "\n";
      db->del (k);
    }
  }
  db->insert (k, d);
  info << "db write: " << node->my_ID ()
       << " U " << key << " " << d->len << "\n";
  return stat;
}

// ------------------------------------------------------------------
// Maintenance
void
dhblock_replicated_srv::start (bool randomize)
{
  dhblock_srv::start (randomize);

  u_long delay = checkrep_interval;
  if (randomize)
    delay = random_getword () % checkrep_interval;

  checkrep_tcb = delaycb (delay,
      wrap (this, &dhblock_replicated_srv::checkrep_timer));
}

void
dhblock_replicated_srv::stop ()
{
  dhblock_srv::stop ();
  if (checkrep_tcb) {
    timecb_remove (checkrep_tcb);
    checkrep_tcb = NULL;
  }
}


void
dhblock_replicated_srv::update_replica_list () 
{
  replicas = node->succs ();
  // trim down successors to just the replicas
  while (replicas.size () > dhblock_replicated::num_replica ())
    replicas.pop_back ();
}

void
dhblock_replicated_srv::checkrep_timer ()
{
  checkrep_tcb = NULL;
  update_replica_list ();
  chordID p = node->my_pred ()->id ();
  chordID m = node->my_ID ();

  if (nrpcsout == 0) {
    ptr<dbEnumeration> iter = db->enumerate ();
    ptr<dbPair> entry = iter->firstElement ();
    while (entry) {
      chordID n = dbrec2id (entry->key);
      if (betweenrightincl (p, m, n)) {
        // replicate a block if we are responsible for it
	for (unsigned j=0; j<replicas.size (); j++) {
	  // trace << "replicated: " << n << " to " << replicas[j]->id () << "\n";
	  nrpcsout++;
	  
	  ptr<dbrec> blk = db->lookup (id2dbrec (n));
	  str dhblk (blk->value, blk->len);
	  dhash_store::execute (node, replicas[j],
	      blockID (n, ctype),
	      dhblk,
	      wrap (this, &dhblock_replicated_srv::checkrep_sync_done),
	      DHASH_REPLICA);
	}
      } else {
        nrpcsout++;
        // otherwise, try to sync with the master node
	
        node->find_successor (n, 
	    wrap (this, &dhblock_replicated_srv::checkrep_lookup, n));
      }
      entry = iter->nextElement ();
    }
  }
  checkrep_tcb =
    delaycb (checkrep_interval,
	wrap (this, &dhblock_replicated_srv::checkrep_timer));
}


void
dhblock_replicated_srv::checkrep_sync_done (dhash_stat err, chordID k, bool present)
{
  nrpcsout--;
}

void
dhblock_replicated_srv::checkrep_lookup (chordID key, 
				vec<chord_node> hostsl, route r,
				chordstat err)
{
  nrpcsout --;
  if (!err) {
    nrpcsout++;
    // trace << "replicated: sync " << key << " to " << r.back()->id () << "\n";
    ptr<dbrec> blk = db->lookup (id2dbrec (key));
    str dhblk (blk->value, blk->len);
    // XXX Should use hostsl[0] instead of r.back??
    dhash_store::execute (node, r.back (),
	blockID (key, ctype),
	dhblk,
	wrap (this, &dhblock_replicated_srv::checkrep_sync_done),
	DHASH_REPLICA);
  }
}
