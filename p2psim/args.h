#ifndef __ARGS_H
#define __ARGS_H

#include <map>
#include <string>
using namespace std;

// to make life a bit easier using the Args object

class Args : public map<string,string> {
public:
  template<class T>
  T nget(string s) {
    return (T) atoi((*this)[s].c_str());
  }
};

#endif // __ARGS_H
