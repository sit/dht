#ifndef _DIRPAGE_H_
#define _DIRPAGE_H_
/*
 *
 * Copyright (C) 2002  James Robertson (jsr@mit.edu),
 *   		       Massachusetts Institute of Technology
 * 
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
 *
 */

#include "sfsmisc.h"
class cs_output;
class dir;
class melody_file;
class cs_client;

class dirpage {
  cs_output *out;
  dir *d;
  melody_file *cc;
  str hosturl;
  bool started, fileout_started;
  int offset, size;

  void listdir(str path);
  void add_dir_cb(str dir, str parent);
  void add_file_cb(str dir, str parent, int size, bigint filehash);
  void fileout(const char *buf, int len, int b_offset);
  void fileout_head (int size);
  void redirect(str path);

public:
  dirpage(melody_file *acc, str ahosturl, cs_client *cs);
  ~dirpage();
  void start(cs_output *aout);
  void output(str path, str referrer);
  void add_dir(str dir, str parent);
  void add_file(str file, str parent, int size, bigint filehash);
};

#endif
