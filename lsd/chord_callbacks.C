#include <sys/time.h>
#include <assert.h>
#include <chord.h>
#include <chord_util.h>

#define MAX_INT 0x7fffffff

void
p2p::registerSearchCallback(cbsearch_t cb) 
{
  searchCallbacks.push_back(cb); 
}

bool
p2p::testSearchCallbacks(sfs_ID id, sfs_ID x) 
{
  warn << "testing callbacks " << id << " " << x << "\n";
  int ret = -1;
  for (int i=0; i < searchCallbacks.size (); i++)
    if ( (searchCallbacks[i] (id, x)) && (ret != 0) ) ret = 1;

  return (ret > 0);
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
