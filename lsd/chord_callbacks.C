#include <sys/time.h>
#include <assert.h>
#include <chord.h>
#include <chord_util.h>

#define MAX_INT 0x7fffffff

cb_ID
p2p::registerSearchCallback(cbsearch_t cb) 
{
  warn << "registered a search callback\n";
  searchCallbacks.push_back(cb); 
}

void
p2p::testSearchCallbacks(sfs_ID id, sfs_ID target, cbtest_t cb) 
{
  warn << "testing callbacks " << id << " " << target << "\n";
  tscb(id, target, 0, cb);
}

void
p2p::tscb (sfs_ID id, sfs_ID x, int i, cbtest_t cb) {
  if (searchCallbacks.size () == i) 
    cb (0);
  else
    (searchCallbacks[i] (id, x, wrap(this, &p2p::tscb_cb, id, x, i, cb)));
}

void
p2p::tscb_cb (sfs_ID id, sfs_ID x, 
	      int i, cbtest_t cb, int result) {
  if (result) 
    cb (1);
  else 
    tscb(id, x, i + 1, cb);
}

void
p2p::registerActionCallback(cbaction_t cb) 
{
  actionCallbacks.push_back(cb);
}  

void
p2p::doActionCallbacks(sfs_ID id, char action) 
{
  for (int i=0; i < actionCallbacks.size (); i++)
    actionCallbacks[i] (id, action);
}
