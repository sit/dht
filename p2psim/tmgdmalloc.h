#ifndef __TMG_DMALLOC_H
#define __TMG_DMALLOC_H

//
// Usage:
//
// ./configure --with-tmgdmalloc
// #include "tmgdmalloc.h"
//
// Then use New and Delete, rather than new and delete.
//
// __tmg_dmalloc_stats will tell you the location of un-delete-d objects.
//

#include "config.h"

#ifdef HAVE_EXT_HASH_MAP
# include <ext/hash_map>
using namespace __gnu_cxx;
namespace __gnu_cxx {
#else
# include <hash_map>
using namespace std;
namespace std {
#endif
  template<> struct hash<void*> {
    size_t operator()(void *x) const {
      return (size_t) x;
    }
  };
}

void __tmg_dmalloc_new(void *, size_t, const char*, unsigned);
void __tmg_dmalloc_del(void *);
void __tmg_dmalloc_stats();
void __tmg_dmalloc_dump(void*);

#ifdef WITH_TMGDMALLOC
void* operator new(size_t, const char*, unsigned);
void* operator new[](size_t, const char*, unsigned);
void  operator delete(void *p);
void  operator delete[](void *p);
# define New new(__FILE__, __LINE__)
#else
# define New new
#endif // WITH_TMGDMALLOC

#endif //  __TMG_DMALLOC_H
