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

#include "dir.h"
#include "cs_output.h"
#include "dirpage.h"

dirpage::dirpage(melody_file *acc, str ahosturl, cs_client *cs)
{
  out = NULL;
  d = New dir(acc, wrap(this, &dirpage::fileout), wrap(this, &dirpage::fileout_head), cs);
  cc = acc;
  hosturl = ahosturl;
  started = false;
  fileout_started = false;
  offset = 0;
}

dirpage::~dirpage()
{
}

void
dirpage::start(cs_output *aout)
{
  out = aout;
}

void
dirpage::output(str path, str referrer)
{
  if(started) {
    warn << (int)d->cs << " reused dirpage error o\n";
    return;
  }
  started = true;

  if(!strncmp(path.cstr(), "/add_dir", 8))
    // add dir
    add_dir(path.cstr()+17, referrer); // FIXME math is hard
  else if(!strncmp(path.cstr(), "/exit", 5)) {
    delete(out);
    delete(d);
    delete(this);
    exit(1);
  } else
    d->opendir(path, wrap(this, &dirpage::listdir, path));
}

char *dp="\r\n<HTML><HEAD><TITLE>Melody directory</TITLE></HEAD><BODY><H3>";
char *dp2="</H3><P><FORM action=\"http://";
char *dp2a="/add_dir\" method=\"get\"><P>add dir: <INPUT type=\"text\" name=\"add_dir\"><INPUT type=\"submit\" value=\"Add\"></FORM><FORM action=\"http://";
char *dp2b="/dstore\" enctype=\"multipart/form-data\" method=\"post\"><P>add file: <INPUT type=\"file\" name=\"file\"><INPUT type=\"submit\" value=\"Store\"></FORM><TABLE border=\"0\">";
char *dp3="<TR><TD><a href=\"";
char *dp4="\">";
char *dp5="</a>";
char *dp6="</TABLE></BODY></HTML>";
char *dperr="</H3><P>does not exist. would you like to add?";
char *dphead="HTTP/1.1 200 OK\r\nServer: melody\r\n";
char *dpfile="Content-Type: audio/x-mp3\r\nContent-Length: ";

void
dirpage::listdir(str path) {
  warn << (int)d->cs << " dir " << path << "\n";
  if(path[path.len()-1] != '/') {  // redirect for dir urls with no trailing slash
    strbuf tmp = path;
    tmp << "/";
    redirect(tmp);
    delete d;
    return;
  }

  out->take(dphead);
  out->take(dp);
  out->take(path);
  if(d->exists()) {
    out->take(dp2);
    out->take(hosturl);
    out->take(dp2a);
    out->take(hosturl);
    out->take(dp2b);

    while(d->more()) {
      strbuf tmp, tmp2, size;
      struct dir_record dr;

      d->readdir(&dr);
	
      size << "<TD>" << dr.size;
      tmp << dr.entry;
      warn << (int)d->cs << " " << tmp << "\n";
      tmp2 << tmp;
      if(dr.type == 0)
	tmp2 << "/";

      out->take(dp3);
      out->take((str)tmp2);
      out->take(dp4);
      out->take((str)tmp);
      out->take((str)size);
      out->take(dp5);
    }
  } else
    out->take(dperr);
  out->take(dp6);
  out->done();
  delete d;
  delete this;
}

void
dirpage::fileout_head (int asize)
{
  char foo[1024]; // ridiculusly large
  size = asize;
  sprintf(&foo, "%d\r\n\r\n", size); // FIXME return == null?
warn << (int)d->cs << " fileout_head size " << size << "\n";
  out->take(dphead);
  out->take(dpfile);
  out->take(foo);
  cc->next();
}

void
dirpage::fileout (const char *buf, int len, int b_offset)
{
warn << (int)d->cs << " fileout " << b_offset << " " << len << "\n";
  if(b_offset != offset) // FIXME async buffering
    warn << (int)d->cs << " oops, sent blocks out of order" << " " << b_offset << " " << offset << "\n";
  out->take(buf, len);
  offset += len;
  if(offset == size) {
    out->done();
    warn << (int)d->cs << " fileout done\n";
  }
  if(!out->closed)
    cc->next();
}

void
dirpage::add_dir(str dir, str parent)
{
  warn << (int)d->cs << " adding " << dir << " to " << parent << "\n";
  d->opendir(parent, wrap(this, &dirpage::add_dir_cb, dir, parent));
}

void
dirpage::add_dir_cb(str dir, str parent)
{
  warn << (int)d->cs << " added " << dir << " to " << parent << "\n";
  if(d->exists())
    d->add_dir(dir, parent, wrap(this, &dirpage::redirect, parent));
  else {
    out->done();
    delete(this);
  }   
}

void
dirpage::add_file(str dir, str parent, int size, bigint filehash)
{
  if(started) {
    warn << (int)d->cs << " reused dirpage error af\n";
    return;
  }
  started = true;

  d->opendir(parent, wrap(this, &dirpage::add_file_cb, dir, parent, size, filehash));
}

void
dirpage::add_file_cb(str dir, str parent, int size, bigint filehash)
{
  if(d->exists())
    d->add_file(dir, parent, filehash, size, wrap(this, &dirpage::redirect, parent));
  else {
    out->done();
    delete(this);
  }   
}

void
dirpage::redirect(str path)
{
warn << (int)d->cs << " redirecting to " << path << "\n";

  char *tmp = "HTTP/1.0 301 \r\nLocation: http://";
  out->take(tmp);
  out->take(hosturl);
  out->take(path);
  out->take("\r\n", 2);
  out->done();
  delete(this);
}
