#ifndef __ARGS_H
#define __ARGS_H

#include <map>
#include <string>
using namespace std;

// arguments
class Args : public map<string,string> {
public:
  template<class T>
  T nget(string s, unsigned base = 16) {
    return (T) strtoull((*this)[s].c_str(), NULL, base);
  }
};

#endif // __ARGS_H
