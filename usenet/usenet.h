// -*-c++-*-
/* $Id: usenet.h,v 1.10 2005/07/15 04:07:34 sit Exp $ */

/*
 *
 * Copyright (C) 2004, 2003 Emil Sit (sit@mit.edu)
 * Copyright (C) 2003 James Robertson (jsr@mit.edu)
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

#ifndef USENET
#define USENET

/* usenet.C */
struct dbfe;
struct dhashclient;
extern ptr<dbfe> group_db, header_db;
extern dhashclient *dhash;
extern unsigned int nrpcout;

str collect_stats ();

/* newspeer.C (partial) */
void feed_article (str id, const vec<str> &groups);
struct peerinfo;

/* config.C */
extern str config_file;
struct options {
  options ();
  ~options ();

  unsigned int client_timeout;
  u_int16_t listen_port;
  unsigned int peer_max_queue;
  unsigned int peer_timeout;
  bool create_unknown_groups;
  unsigned int sync_interval;
  unsigned int max_parallel;

  vec<ptr<peerinfo> > peers;

private:
  str parse_peer (vec<str> &av);
  friend bool parseconfig (options *op, str cf);
};

extern options *opt;

#endif /* USENET */
