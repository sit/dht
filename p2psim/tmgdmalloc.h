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

struct __TMG_c2info;

// maps typeid class name to a __TMG_filemap
typedef hash_map<string, __TMG_c2info*> __TMG_classmap;

// maps a line number to a __TMG_classmap
typedef hash_map<unsigned, unsigned> __TMG_linemap;

// maps a file name to a __TMG_linemap
typedef hash_map<string, __TMG_linemap*> __TMG_filemap;


struct __TMG_c2info {
  __TMG_c2info() { allocs = 0; files = new __TMG_filemap; assert(files); }
  ~__TMG_c2info() { delete files; }
  unsigned allocs;
  __TMG_filemap *files;
};


// maps pointers to all the info one would want about it
struct __TMG_p2info {
  __TMG_p2info(string f, unsigned l, string c) :
    file(f), line(l), cname(c) {}
  string file;
  unsigned line;
  string cname;
};
typedef hash_map<void*, __TMG_p2info*> __TMG_p2infomap;

void __tmg_dmalloc_del(void *);
void __tmg_dmalloc_stats();
void __tmg_dmalloc_dump(void*);
extern __TMG_classmap __tmg_cmap;
extern __TMG_p2infomap __tmg_p2infomap;
extern unsigned __tmg_totalallocs;

template<class C>
inline
C*
__tmg_dmalloc_new(C *p, string fname, unsigned lineno)
{
  string cname = typeid(C).name();

  __TMG_c2info *c2i = 0;
  __TMG_linemap *linemap = 0;

  // find the class entry and update the number of allocs
  if(__tmg_cmap.find(cname) == __tmg_cmap.end()) {
    c2i = new __TMG_c2info();
    assert(c2i);
    __tmg_cmap[cname] = c2i;
  } else {
    c2i = __tmg_cmap[cname];
  }
  c2i->allocs++;

  // in correct file/linenumber entry do update
  if(c2i->files->find(fname) == c2i->files->end()) {
    linemap = new __TMG_linemap;
    assert(linemap);
    (*c2i->files)[fname] = linemap;
  } else {
    linemap = (*c2i->files)[fname];
  }
  (*linemap)[lineno]++;

  // now take care of the entry to map pointer to the info
  assert(__tmg_p2infomap.find(p) == __tmg_p2infomap.end());
  __TMG_p2info *p2i = new __TMG_p2info(fname, lineno, cname);
  assert(p2i);
  __tmg_p2infomap[p] = p2i;

  __tmg_totalallocs++;
  return p;
}

#ifndef TMG_DMALLOC
# define New(X, args...) new X(args)
# define Delete(x) delete x;
#else
# define New(X, args...) __tmg_dmalloc_new<X>(new X(args), string(__FILE__), __LINE__);
# define Delete(x) __tmg_dmalloc_del((void*)x); delete x;
#endif

#endif //  __TMG_DMALLOC
