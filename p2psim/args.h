#ifndef __ARGS_H
#define __ARGS_H

#include <string>
#include "p2psim_hashmap.h"
using namespace std;

// arguments
class Args : public hash_map<string,string> {
public:
  template<class T>
  T nget(string s, T defaultv = (T)0, unsigned base = 16) {
    if (this->find(s) == this->end()) return defaultv;
    return (T) strtoull((*this)[s].c_str(), NULL, base);
  }
};

#endif // __ARGS_H
