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


