#ifndef __TMG_DMALLOC
#define __TMG_DMALLOC

//
//  Allows you to use
//      New(obj, param1, param2, ...);
//  and
//      Delete(obj);
//
//
//  NB: THIS WON'T WORK!
//
//  for(...)
//    Delete()
//
//  for(...)
//    New();
// 
//  It has to be:
//
//  for(...) {
//    Delete()
//  }
//
//  for(...) {
//    New();
//  }
//


#include <typeinfo>
#include "p2psim_hashmap.h"
#include <stdio.h>
using namespace std;

#ifndef TMG_DMALLOC
# define New(X, args...) new X(args)
# define Delete(x) delete x;


#else
// one entry for each class type in __tmg_dmalloc_map
struct __tmg_dmalloc_entry {
  unsigned nobj;
  hash_map<void*, pair<string, unsigned> > where;
};

void __tmg_dmalloc_del(void *);
void __tmg_dmalloc_dump();
void __tmg_dmalloc_large_dump();
extern hash_map<string, __tmg_dmalloc_entry*> __tmg_dmalloc_map;
extern hash_map<void*, string> __tmg_dmalloc_typemap;

# define New(X, args...) __tmg_dmalloc_new<X>(new X(args), string(__FILE__), __LINE__);
# define Delete(x) __tmg_dmalloc_del((void*)x); delete x;


template<class C>
inline
C*
__tmg_dmalloc_new(C *p, string f, unsigned l)
{
  __tmg_dmalloc_entry *ne = 0;
  string name = typeid(C).name();

  // store in __tmg_dmalloc_map
  if(__tmg_dmalloc_map.find(name) == __tmg_dmalloc_map.end()) {
    ne = new __tmg_dmalloc_entry;
    ne->nobj = 0;
    ne->where.clear();
    __tmg_dmalloc_map[name] = ne;
  } else {
    ne = __tmg_dmalloc_map[name];
  }

  ne->nobj++;
  assert(ne->where.find(p) == ne->where.end());
  ne->where[p] = make_pair(f, l);

  // store in __tmg_dmalloc_typemap
  assert(__tmg_dmalloc_typemap.find((void*)p) == __tmg_dmalloc_typemap.end());
  __tmg_dmalloc_typemap[(void*)p] = name;

  return p;
}



#endif // TMG_DMALLOC
#endif //  __TMG_DMALLOC
