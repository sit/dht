#ifndef __ARGS_H
#define __ARGS_H

#include <map>
#include <string>
using namespace std;

// to make life a bit easier using the Args object

class Args : public map<string,string> {
public:
  unsigned uget(string s) {
    return (unsigned) atoi((*this)[s].c_str());
  }
  int iget(string s) {
    return (int) atoi((*this)[s].c_str());
  }
};

#endif // __ARGS_H
