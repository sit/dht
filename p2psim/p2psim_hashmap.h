#ifndef __P2PSIM_HASHMAP
#define __P2PSIM_HASHMAP

#include "config.h"

#include <string>
#ifdef HAVE_EXT_HASH_MAP
# include <ext/hash_map>
#else
# include <hash_map>
#endif

#ifdef HAVE_EXT_HASH_MAP
using namespace __gnu_cxx;
#else
using namespace std;
#endif

#ifndef HAVE_EXT_HASH_MAP
namespace std {
#else
namespace __gnu_cxx {
#endif
  template<> struct hash<std::string> {
    size_t operator()( const std::string& x ) const {
      return hash<const char*>()(x.c_str());
    }
  };

  template<> struct hash<void*> {
    size_t operator()( void*x ) const {
      return hash<const char*>()((const char*)x);
    }
  };
}

#endif // __P2PSIM_HASHMAP
