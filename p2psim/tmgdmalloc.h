#ifndef __TMG_DMALLOC
#define __TMG_DMALLOC

#include <typeinfo>

//
//  Allows you to use
//      New(obj, param1, param2, ...);
//  and
//      Delete(obj);
//
// 
//


#ifndef TMG_DMALLOC
# define New(X, args...) new X(args)
# define Delete(x) delete x;


#else
# define New(X, args...) __tmg_dmalloc_new<X>(new X(args), string(__FILE__), __LINE__);
# define Delete(x) __tmg_dmalloc_new((void*)x); delete x;

// one entry for each class type in __tmg_dmalloc_map
struct __tmg_dmalloc_entry {
  unsigned nobj;
  hash_map<void*, pair<string, unsigned> > where;
};

// maps class names to __tmg_dmalloc_entries
hash_map<string, __tmg_dmalloc_entry*> __tmg_dmalloc_map;

// maps pointers to class names
hash_map<void*, string> __tmg_dmalloc_typemap;

void* __tmg_dmalloc_p = 0;


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
  ne->where[p] = make_pair(f, l);

  // store in __tmg_dmalloc_typemap
  __tmg_dmalloc_typemap[(void*)p] = name;

  return p;
}


inline void
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
  cout << "-------------\n";
  for(hash_map<string, __tmg_dmalloc_entry*>::const_iterator i = __tmg_dmalloc_map.begin();
      i != __tmg_dmalloc_map.end();
      ++i)
  {
    cout << "Class: " << i->first << endl;
    cout << "instances: " << i->second->nobj << endl;
  }
  cout << "-------------\n";
}


void
__tmg_dmalloc_large_dump()
{
  cout << "-------------\n";
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


#endif // defined TMG_DMALLOC
#endif //  __TMG_DMALLOC
