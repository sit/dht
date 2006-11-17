#include <chord.h>
#include <location.h>

#include "dhash.h"
#include "dhash_common.h"

#include <dhblock_keyhash.h>
#include <dhblock_keyhash_srv.h>

#include <modlogger.h>
#define warning modlogger ("dhblock_keyhash", modlogger::WARNING)
#define info    modlogger ("dhblock_keyhash", modlogger::INFO)
#define trace   modlogger ("dhblock_keyhash", modlogger::TRACE)

#ifdef DMALLOC
#include <dmalloc.h>
#endif

dhblock_keyhash_srv::dhblock_keyhash_srv (ptr<vnode> node,
					  ptr<dhashcli> cli,
				          str desc,
					  str dbname,
					  str dbext, cbv donecb) :
  dhblock_replicated_srv (node, cli, desc, dbname, dbext, DHASH_KEYHASH, 
			  donecb)
{
}

void
dhblock_keyhash_srv::real_store (chordID key, str od, str nd, cb_dhstat cb)
{
  u_int32_t v1 = dhblock_keyhash::version (nd.cstr (), nd.len ());
  if (od) {
    u_int32_t v0 = dhblock_keyhash::version (od.cstr (), od.len ());
    if (v0 > v1) {
      chordID p = node->my_pred ()->id ();
      chordID m = node->my_ID ();
      if (betweenrightincl (p, m, key))
	cb (DHASH_STALE);
      else
	cb (DHASH_RETRY);
    } else {
      info << "db delete: " << key << "\n";
      db->remove (key, v0, 
		  wrap (this, &dhblock_keyhash_srv::delete_cb, key, 
			nd, v1, cb));
      db->update (key, node->my_location (), v0, false);
    }
  } else {
    info << "db write: " << node->my_ID ()
	 << " N " << key << " " << nd.len () << "\n";
    db_store (key, nd, v1, cb);
  }
}

void 
dhblock_keyhash_srv::delete_cb (chordID k, str d, u_int32_t v, cb_dhstat cb, adb_status stat) 
{
  assert (stat == ADB_OK);
  info << "db write: " << node->my_ID ()
       << " U " << k << " " << d.len () << "\n";
  
  db_store (k, d, v, cb);
  db->update (k, node->my_location (), v, true);
}

// XXX could ideally try to find the newest version over
//     all replicas and move that everywhere.
//     This ought to do that eventually...
void
dhblock_keyhash_srv::real_repair (blockID key, ptr<location> me, u_int32_t *myaux, ptr<location> them, u_int32_t *theiraux)
{
  // keyhash aux is the version number of the block.
  ptr<repair_job> job;
  if (!myaux) {
    // We're missing, so fetch it.
    job = New refcounted<rjrep> (key, me, mkref (this));
    repair_add (job);
  } else {
    // They're missing so push it.
    // Otherwise, move towards the newer one.
    if (!theiraux) {
      job = New refcounted<rjrep> (key, them, mkref (this));
      repair_add (job);
    } else if (*theiraux < *myaux) {
      job = New refcounted<rjrep> (key, them, mkref (this));
      repair_add (job);
    } else if (*theiraux > *myaux) {
      job = New refcounted<rjrep> (key, me, mkref (this));
      repair_add (job);
    }
  }
}
