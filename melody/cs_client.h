#ifndef _CS_CLIENT_H_
#define _CS_CLIENT_H_
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

class melody_conn;

#include "async.h"
#include "http.h"
#include "cs_output.h"
#include "dirpage.h"

class cs_client {
  int s;
  melody_file *f;
  timecb_t *timeout;
  suio req;
  httpreq reqheaders;
  int offset, size;
  cs_output *out;
  dirpage *dir;
  bool sleeping;
  callback<void>::ptr accept_more;
  str endtag;

public:
  static int num_active;

  cs_client(int cfd, callback<void>::ptr am);
  ~cs_client();
  tailq_entry <cs_client> sleep_link;
  void readcb_wakeup();

private:
  void xfer_done(str status);
  void requestcb();
  void died();
  void readc(const char *buf, int len, int b_offset);
  void readcb();
  void writec();
  void input(suio *req, str referrer);
};

#endif
