#ifndef __ARGS_H
#define __ARGS_H


#include "p2psim_hashmap.h"
#include "parse.h"
using namespace std;

// arguments
class Args : public hash_map<string,string> {
public:
  Args() {}

  Args(vector<string> *v, int offset = 0) {
    for(unsigned int i=offset; i<v->size(); i++) {
      vector<string> arg = split((*v)[i], "=");
      insert(make_pair(arg[0], arg[1]));
    }
  }

  template<class T>
  T nget(string s, T defaultv = (T)0, unsigned base = 16) {
    if (this->find(s) == this->end()) return defaultv;
    return (T) strtoull((*this)[s].c_str(), NULL, base);
  }
};

#endif // __ARGS_H
