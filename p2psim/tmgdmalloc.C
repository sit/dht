#define TMG_DMALLOC
#include "tmgdmalloc.h"


// maps class names to __tmg_dmalloc_entries
hash_map<string, __tmg_dmalloc_entry*> __tmg_dmalloc_map;

// maps pointers to class names
hash_map<void*, string> __tmg_dmalloc_typemap;

void
__tmg_dmalloc_del(void *p)
{
  // find class name for this pointer and remove entry
  assert(__tmg_dmalloc_typemap.find(p) != __tmg_dmalloc_typemap.end());
  string name = __tmg_dmalloc_typemap[p];
  __tmg_dmalloc_typemap.erase(p);

  // find class type name
  if(__tmg_dmalloc_map.find(name) == __tmg_dmalloc_map.end()) {
    assert(false);
    return;
  }

  // lower number of references and remove file/line entry
  __tmg_dmalloc_entry *ne = 0;
  ne = __tmg_dmalloc_map[name];
  if(!(--ne->nobj)) {
    delete __tmg_dmalloc_map[name];
    __tmg_dmalloc_map.erase(name);
  }
  ne->where.erase(p);
};



void
__tmg_dmalloc_dump()
{
  cout << "--- NORMAL DUMP ----------\n";
  for(hash_map<string, __tmg_dmalloc_entry*>::const_iterator i = __tmg_dmalloc_map.begin();
      i != __tmg_dmalloc_map.end();
      ++i)
  {
    cout << "Class: " << i->first << endl;
    cout << "instances: " << i->second->nobj << endl;
  }
}


void
__tmg_dmalloc_large_dump()
{
  cout << "--- LARGE DUMP -----------\n";
  for(hash_map<string, __tmg_dmalloc_entry*>::const_iterator i = __tmg_dmalloc_map.begin();
      i != __tmg_dmalloc_map.end();
      ++i)
  {
    for(hash_map<void*, pair<string, unsigned> >::const_iterator j = i->second->where.begin();
      j != i->second->where.end();
      ++j)
    {
      cout << "Class: " << i->first << endl;
      cout << "instances: " << i->second->nobj << endl;
      cout << j->second.first << ":" << j->second.second << endl;
    }
  }
}

