/*
 * testslave.{C,h} --- a slave that pipes DHash RPCs to lsd
 *
 * Copyright (C) 2002  Thomer M. Gil (thomer@lcs.mit.edu)
 *   		       Massachusetts Institute of Technology
 * 
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef __TESTCLIENT_H
#define __TESTCLIENT_H 1

#include "test.h"
#include <dhash.h>

class testslave { public:
  testslave(int, char *argv[]);
  ~testslave();



private:
  void tcppipe(const int, const int);
  void udppipe(const int, const u_int16_t, const int tofd = -1);
  void accept_master();
  void start_lsd(const char *, const char *, const char *, const str);
  void start_lsd2(const char *, const u_int16_t);

  int _master_listener; // incoming connection from master
  int _masterfd;  // master --> slave (dhash client protocol)
  int _lsdsockfd; // slave --> lsd (dhash client protocol)

  int _lsd_listener; // incoming data from remote lsd's
};

#endif // __TESTCLIENT_H
