#include <sys/time.h>
#include <assert.h>
#include <chord.h>
#include <chord_util.h>

#define MAX_INT 0x7fffffff

//note that a search callback only "fires" once
cb_ID
p2p::registerSearchCallback(cbsearch_t cb) 
{
  warn << "registered a search callback\n";
  searchcb_entry *scb = New searchcb_entry (cb);
  scb->cb = cb;
  searchCallbacks.insert_head(scb);
}

void
p2p::testSearchCallbacks(sfs_ID id, sfs_ID target, cbtest_t cb) 
{
  warn << "testing callbacks " << id << " " << target << "\n";
  tscb(id, target, searchCallbacks.first, cb);
}

void
p2p::tscb (sfs_ID id, sfs_ID x, searchcb_entry *scb, cbtest_t cb) {
  if (scb == NULL) 
    cb (0);
  else
    (scb->cb (id, x, wrap(this, &p2p::tscb_cb, id, x, scb, cb)));
}

void
p2p::tscb_cb (sfs_ID id, sfs_ID x, 
	      searchcb_entry *scb, cbtest_t cb, int result) {

  searchcb_entry *next = searchCallbacks.next (scb);
  searchCallbacks.remove (scb);
  if (result) 
    cb (1);
  else 
    tscb(id, x, next, cb);
}

void
p2p::registerActionCallback(cbaction_t cb) 
{
  actionCallbacks.push_back(cb);
}  

void
p2p::doActionCallbacks(sfs_ID id, char action) 
{
  //  warn << "\n\n\n\n ACTION CALLBACKS " << actionCallbacks.size() << "\n";
  for (unsigned int i=0; i < actionCallbacks.size (); i++)
    actionCallbacks[i] (id, action);
}
