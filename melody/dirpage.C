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
#include "stdlib.h"
#include "rxx.h"

char bad[4096];
void tourl(const char *url, char *out) {
  while(*url) {
    if(((*url >= 47) && (*url <= 57)) || /* / and numbers */
       ((*url >= 65) && (*url <= 90)) || /* A-Z */
       ((*url >= 97) && (*url <= 122))) /* a-z */
      *out = *url;
    else {
      sprintf(out, "%%%02x", *url);
      out += 2;
    }
    url++;
    out++;
  }
  *out = 0;
}

dirpage::dirpage(ptr<melody_file>acc, str ahosturl, cs_client *cs)
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
  out->done();
  delete(d);
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

  d->opendir(path, wrap(this, &dirpage::listdir, path), false);
}

char *dp="\r\n<HTML><HEAD><TITLE>Melody directory</TITLE></HEAD><BODY bgcolor=#ffffff><H3>";
char *dp1="</H3><P>Generated at ";
char *dp2="<P><FORM action=\"http://";
char *dp2a="/add_dir\" method=\"post\"><P>Create dir: <INPUT type=\"text\" name=\"add_dir\"><INPUT type=\"submit\" value=\"Create\"></FORM><FORM action=\"http://";
char *dp2b="/dstore\" enctype=\"multipart/form-data\" method=\"post\"><P>add file: <INPUT type=\"file\" name=\"file\"><INPUT type=\"submit\" value=\"Store\"></FORM><P><TABLE border=\"0\"><TR><TD><font color=#606060>name<TD><font color=#606060>size<TD><font color=#606060>creation time<TD><font color=#606060>key";
char *dp3="<TR><TD><a href=\"";
char *dp3a="<TR bgcolor=#d0ffe0><TD><a href=\"";
char *dp4="\">";
char *dp5="</a>";
char *dp6="</TABLE></BODY></HTML>";
char *dperr="</H3><P>does not exist. would you like to add?";
char *dphead="HTTP/1.1 200 OK\r\nServer: melody\r\nPragma: no-cache\r\n";
char *dpfile="Content-Type: audio/x-mp3\r\nContent-Length: ";
char *dphtml="Content-Type: text/html\r\nContent-Length: ";
char *dptext="Content-Type: text/plain\r\nContent-Length: ";

class dirline {
public:
  struct dir_record dr;
  strbuf t;
  str t2;
  str hosturl;
  bool start;

  dirline::dirline(struct dir_record *adr, str host) {
    dr = *adr;
    hosturl = host;
    start = true;
  }

  static int comp (const void *ad1, const void *ad2) {
    dirline *d1 = (dirline *)ad1, *d2 = (dirline *)ad2;
    if(d1->dr.type < d2->dr.type)
      return -1;
    else if(d1->dr.type > d2->dr.type)
      return 1;
    else if(strncmp(d1->dr.entry, d2->dr.entry, 256) < 0)
      return -1;
    else
      return 1;
  }

  void sizeprint(int size, strbuf *sout, bool color) {
    int newsize = size/1000;
    char tmp[4];
    if(newsize)
      sizeprint(newsize, sout, !color);
    // yea, recursion
    if(color)
      *sout << "<TD bgcolor=#e8e0ff>";
    else
      *sout << "<TD bgcolor=#ffffff>";
    if(start) {
      sprintf(tmp, "%3d", (size%1000));
      start = false;
    } else
      sprintf(tmp, "%03d", (size%1000));
    *sout << tmp;
  }

  const char* cstr() {
    strbuf tmp, tmp2, size;
    bigint btmp;

    mpz_set_rawmag_be(&btmp, dr.key, sha1::hashsize);
    size << "<TD><P align=right><TABLE><TR>";
    tmp << dr.entry;
    tmp2 << tmp;
    if((str)tmp != "..") {
      str foo = tmp2;
      tourl(foo, bad);
      tmp2.tosuio()->clear();
      tmp2.tosuio()->copy(bad, strlen(bad));
      tmp2 << ";" << btmp;
    }
    if(dr.type == 0) {
      tmp2 << "/";
      t << dp3a;
    } else {
      t << dp3;
      sizeprint(dr.size, &size, true);
    }
    size << "</TABLE>";
    t << tmp2 << dp4 << tmp << size 
      << "<TD>" << ctime((time_t *)&dr.ctime.tv_sec) 
      << "<TD>" << btmp
      << dp5;

    t2 = t;
    return t2.cstr();
  }
};

static rxx pathrx ("^/+([^/;]+);[\\dabcdef]+", "i");
void
dirpage::listdir(str path2) {
  vec<dirline> lines;
  warn << (int)d->cs << " dir " << path2 << "\n";
  if(path2[path2.len()-1] != '/') {  // redirect for dir urls with no trailing slash
    strbuf tmp = path2;
    tmp << "/";
    redirect(tmp);
    return;
  }
  strbuf tmpath = path2, paths;
  paths << "/";
  while(pathrx.search(tmpath)) {
    paths << pathrx[1] << "/";
    warn << (int)d->cs << " asdf " << pathrx[1] << "\n";
    tmpath.tosuio()->rembytes(pathrx.end(0));
  }
  str path = paths;

  out->take(dphead);
  out->take(dp);
  out->take(path);
  if(d->exists()) {
    out->take(dp1);
    struct timeval tv;
    gettimeofday(&tv, NULL);
    out->take(ctime((time_t *)&tv.tv_sec));

    out->take(dp2);
    out->take(hosturl);
    out->take(dp2a);
    out->take(hosturl);
    out->take(dp2b);

    while(d->more()) {
      struct dir_record dr;
      d->readdir(&dr);
      lines.push_back(dirline(&dr, hosturl));
    }
    qsort(lines.base(), lines.size(), sizeof(dirline), &dirline::comp);
    for(unsigned int i=0; i<lines.size(); i++)
      out->take(lines[i].cstr());
  } else
    out->take(dperr);
  out->take(dp6);
  delete(this);
}

static rxx extenrx (".+\\.(.+)", "i");

void
dirpage::fileout_head (int asize, str filename)
{
  char foo[1024]; // ridiculusly large
  size = asize;
  sprintf(foo, "%d\r\n\r\n", size); // FIXME return == null?
warn << (int)d->cs << " fileout_head size " << size << "\n";
  out->take(dphead);
  warn << (int)d->cs << " wooooo1 " << filename << "\n";
  if(extenrx.search(filename)) {
    warn << (int)d->cs << " wooooo " << extenrx[1] << "\n";
    if(extenrx[1] == "html")
      out->take(dphtml);
    else if(extenrx[1] == "c")
      out->take(dptext);
    else
      out->take(dpfile);
  } else
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
  bool more = out->take(buf, len, d->cs);
  offset += len;
  if(offset == size) {
    out->done();
    warn << (int)d->cs << " fileout done\n";
    delete(this);
    return;
  }
  if(more)
    cc->next();
}

void
dirpage::fileout_stop()
{
  // tell block layer to stop
  cc->readstop();
  delete(this);
}

void
dirpage::add_dir(str dir, str parent)
{
  warn << (int)d->cs << " adding " << dir << " to " << parent << "\n";
  d->opendir(parent, wrap(this, &dirpage::add_dir_cb, dir, parent), true);
}

void
dirpage::add_dir_cb(str dir, str parent)
{
  warn << (int)d->cs << " added " << dir << " to " << parent << "\n";
  if(d->exists())
    d->add_dir(dir, parent, wrap(this, &dirpage::redirect));
  else {
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

  d->opendir(parent, wrap(this, &dirpage::add_file_cb, dir, parent, size, filehash), true);
}

void
dirpage::add_file_cb(str dir, str parent, int size, bigint filehash)
{
  if(d->exists())
    d->add_file(dir, parent, filehash, size, wrap(this, &dirpage::redirect));
  else {
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
  tourl(path, bad);
  out->take(bad);
  out->take("\r\nPragma: no-cache\r\n");
  delete(this);
}
