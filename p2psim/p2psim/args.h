/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu)
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

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
  double fget(string s, double defaultv=0.0) {
    if (this->find(s) == this->end()) return defaultv;
    return atof((*this)[s].c_str());
  }
  string sget(string s, string defaultv="?") {
    if (this->find(s)==this->end()) return defaultv;
    return ((*this)[s]);
  }
};

#endif // __ARGS_H
