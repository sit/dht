#ifndef __P2PSIM_HASHMAP
#define __P2PSIM_HASHMAP

#include "config.h"
#include <string>

#ifdef HAVE_EXT_HASH_MAP
# include <ext/hash_map>
using namespace __gnu_cxx;
namespace __gnu_cxx {
#else
# include <hash_map>
using namespace std;
namespace std {
#endif
  template<> struct hash<std::string> {
    size_t operator()(const std::string &x) const {
      return hash<const char*>()(x.c_str());
    }
  };
}


#endif // __P2PSIM_HASHMAP
