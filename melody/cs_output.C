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

#include "cs_output.h"

cs_output::cs_output(int as, callback<void>::ptr foo, cs_client *acs)
{
  s = as;
  nomore = false;
  closed = false;
  timeout = NULL;
  ccd = foo;
  cs = acs;
  bytes_out = 0;
}

void 
cs_output::take(const char *buf, int len)
{
  out.copy(buf, len);
  fdcb(s, selwrite, wrap(this, &cs_output::cb));
  if(!timeout) {
    timeout = delaycb(10, 0, wrap(this, &cs_output::died));
  }
}

void 
cs_output::take(const char *buf)
{
  take(buf, strlen(buf));
}

void 
cs_output::cb(void)
{
  bool moreto = true;
  int tmp = out.resid();
  timecb_remove(timeout);
  timeout = NULL;

  int res = out.output(s);
  if(res == 0)
    warn << (int)cs << " sEAGAIN\n";
  if(res == -1) {
    perror(progname);
    fdcb(s, selwrite, NULL);
    closed = true; // FIXME this is hackish. need better way to report error back to block layer and cancel retrieval
    moreto = false;
  } else
    bytes_out += tmp - out.resid();

  if(out.resid() == 0) {
    fdcb(s, selwrite, NULL);
    moreto = false;
    if(nomore) {
warn << (int)cs << " cs_output::cb shutdown WR\n";
warn << (int)cs << " wrote " << bytes_out << " bytes\n";
      shutdown(s, SHUT_WR);
      warn << (int)cs << " cs_ouput done\n";
      warn << (int)cs << " deleting " << (int)this << "\n";
      (*ccd)();
      delete(this);
    }
  }

  if(moreto) {
    timeout = delaycb(10, 0, wrap(this, &cs_output::died));
  }
}

void
cs_output::done(void)
{
  nomore = true;
}

void
cs_output::died()
{
  timeout = NULL;
  warn << (int)cs << " died deleting " << (int)this << "\n";
  (*ccd)();
  delete this;
}

cs_output::~cs_output()
{
  if(timeout) {
    warn << (int)cs << " ahhh! had to remove timeout???\n";
    timecb_remove(timeout);
  }
  fdcb(s, selread, NULL);
  close(s);
}
