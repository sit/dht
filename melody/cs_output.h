#ifndef _CS_OUTPUT_H_
#define _CS_OUTPUT_H_
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

#include "async.h"
#include "list.h"

class data_sender {
 public:
  virtual void wakeup() = 0;
  tailq_entry<data_sender> sleep_link;
  virtual ~data_sender() {};
};

class cs_output {
  int s;
  timecb_t *timeout;
  suio out;
  bool nomore;
  callback<void>::ptr ccd, dpcb;
  data_sender *cs;
  tailq < data_sender, &data_sender::sleep_link > sleeping;
public:
  int bytes_out;
  bool take(const char *buf, int len, data_sender *c);
  void take(const char *buf, int len);
  void take(const char *buf);
  void cb(void);
  void done(void);
  void died(void);
  ~cs_output();
  cs_output(int as, callback<void>::ptr foo, data_sender *cs, callback<void>::ptr adpcb);
};

#endif
