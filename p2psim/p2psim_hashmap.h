#ifndef __P2PSIM_HASHMAP
#define __P2PSIM_HASHMAP

#include <string>
#include <hash_map>
using namespace std;
namespace std {
  template<> struct hash<std::string> {
    size_t operator()( const std::string& x ) const {
      return hash<const char*>()(x.c_str());
    }
  };
}

#endif // __P2PSIM_HASHMAP
