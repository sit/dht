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
  ne->where.erase(p);
  if(!(--ne->nobj)) {
    __tmg_dmalloc_map.erase(name);
    delete ne;
  }
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
    printf("Total of class %s: %d\n", i->first.c_str(), i->second->nobj);
    for(hash_map<void*, pair<string, unsigned> >::const_iterator j = i->second->where.begin();
      j != i->second->where.end();
      ++j)
    {
      printf("Class %s -> %p ", i->first.c_str(), j->first);
      printf("alloc in %s:%d\n", j->second.first.c_str(), j->second.second);
    }
  }
}

