#include <chord.h>
#include "dhash.h"
#include "dhash_common.h"
#include "dhashcli.h"

#include <libadb.h>

#include <dhblock_srv.h>

#include <modlogger.h>
#define trace   modlogger ("dhblock_srv", modlogger::TRACE)

dhblock_srv::dhblock_srv (ptr<vnode> node,
			  ptr<dhashcli> cli,
		          str desc,
			  str dbpath,
			  str dbname,
			  bool hasaux,
			  cbv dcb) :
  repair_tcb (NULL),
  db (NULL),
  node (node),
  desc (desc),
  cli (cli),
  donecb (dcb)
{
  db = New refcounted<adb> (dbpath, dbname, hasaux);
  warn << "opened " << dbpath << " with space " << dbname 
       << (hasaux ? " (hasaux)\n" : "\n");
}

dhblock_srv::~dhblock_srv ()
{
  stop ();
}

// These 2 functions exist just so that we can have a generic adbcb.
// Unfortunately, we can't wrap superclass functions in subclasses.
void 
dhblock_srv::db_store (chordID k, str d, cb_dhstat cb)
{
  db->store (k, d, wrap (this, &dhblock_srv::adbcb, cb));
}
void 
dhblock_srv::db_store (chordID k, str d, u_int32_t aux, cb_dhstat cb)
{
  db->store (k, d, aux, wrap (this, &dhblock_srv::adbcb, cb));
}

void
dhblock_srv::adbcb (cb_dhstat cb, adb_status astat)
{
  switch (astat) {
  case ADB_ERR:
    cb (DHASH_DBERR);
    break;
  case ADB_OK:
    cb (DHASH_OK);
    break;
  default:
    // Don't expect adb::store and adb::remove to have other return codes.
    fatal << "Unexpected adb_status in dhblock_srv::adbcb: " << astat << "\n";
  }
}

void
dhblock_srv::start (bool randomize)
{
  int delay = 0;
  if (!repair_tcb) {
    if (randomize)
      delay = random_getword () % dhash::reptm ();
    repair_tcb = delaycb (dhash::reptm () + delay,
	wrap (this, &dhblock_srv::repair_timer));
  }
}

void
dhblock_srv::stop ()
{
  if (repair_tcb) {
    // Outstanding repairs will continue
    timecb_remove (repair_tcb);
    repair_tcb = NULL;
  }
}

void
dhblock_srv::fetch (chordID k, cb_fetch cb)
{
  db->fetch (k, cb);
}

void
dhblock_srv::stats (vec<dstat> &s)
{
  // Default implementation adds no statistics
  return;
}

void
dhblock_srv::offer (user_args *sbp, dhash_offer_arg *arg)
{
  sbp->replyref (NULL);
}

//
// Repair management
// 
//
void
repair_job::setdonecb (cbv cb, u_int32_t to)
{
  donecb = cb;
  timeout = to;
}

void
repair_job::start ()
{
  // if (to)
  //   delaycb (to, 0, XXXgiveup);
  execute ();
}

bool
dhblock_srv::repair_add (ptr<repair_job> job)
{
  if (!job)
    return false;
  trace << "dhblock_srv::repair_add for " << job->key << "\n";
  if (repairs_queued[job->key] || repairs_inprogress[job->key]) 
    return false;
  repair_q.push_back (job);
  repairs_queued.insert (job->key);

  repair_flush_q ();

  return true;
}

u_int32_t
dhblock_srv::repair_qlength ()
{
  return repairs_queued.size ();
}

void
dhblock_srv::repair_timer ()
{
  if (repairs_queued.size () < REPAIR_QUEUE_MAX)
    generate_repair_jobs ();
  // Generation will finish asynchronously
  // REPAIR_QUEUE_MAX is a soft cap to try and
  // keep us from generating too many repairs; subclasses
  // are free to call repair_add to enqueue jobs whenever
  // they like.

  repair_flush_q ();

  repair_tcb = delaycb (dhash::reptm (),
      wrap (this, &dhblock_srv::repair_timer));
}

void
dhblock_srv::repair_done (blockID key)
{
  repairs_inprogress.remove (key);
  trace << "completed repair of " << key << "; "
	<< repairs_inprogress.size () << " in progress, "
	<< repairs_queued.size () << " in queue.\n";
  repair_flush_q ();
}

void
dhblock_srv::repair_flush_q ()
{
  while ((repairs_inprogress.size () < REPAIR_OUTSTANDING_MAX)
         && (repair_q.size () > 0)) {
    ptr<repair_job> job = repair_q.pop_front ();
    repairs_queued.remove (job->key);
    assert (!repairs_inprogress[job->key]);
    repairs_inprogress.insert (job->key);
    job->setdonecb (wrap (this, &dhblock_srv::repair_done, job->key));
    job->start ();
    trace << "dhblock_srv::repair_flush_q: started repair of " << job->key 
	  << "\n";
  }
  assert (repair_q.size () == repairs_queued.size ());
}
