/* $Id: config.C,v 1.4 2005/02/22 14:29:06 sit Exp $ */

/*
 *
 * Copyright (C) 2004 Emil Sit (sit@mit.edu)
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

/* This file modeled on Mail Avenger's asmtpd/config.C */

#include <async.h>
#include <parseopt.h>
#include <rxx.h>
#include "usenet.h"
#include "newspeer.h"

options *opt (New options);
str config_file ("/dev/null");

options::options ()
  : client_timeout (300),
    listen_port (11999),
    peer_max_queue (1024),
    peer_timeout (300),
    create_unknown_groups (false),
    sync_interval (5)
{
}

options::~options ()
{
}

str
options::parse_peer (vec<str> &av)
{
  if (av.size () < 2)
    return "usage: Peer hostname[:port] [pattern] [pattern ...]";

  // parse av[1] for hostname and port
  str host;
  u_int16_t port = 119;
  {
    vec<str> out;
    int n = split (&out, rxx(":"), av[1]);
    assert (n >= 1);
    host = out[0];
    if (n > 1)
      if (!convertint (out[1], &port))
	return "Bad port!";
  }

  // warnx << "New peer: " << host << ":" << port << "\n";
  for (size_t i = 0; i < peers.size (); i++)
    if (peers[i]->hostname == host && peers[i]->port == port)
      return "duplicate peer!";

  ptr<peerinfo> np = New refcounted<peerinfo> (host, port);
  if (!np)
    return NULL;
  peers.push_back (np);

  if (av.size () < 3) {
    np->add_pattern ("*");
    return NULL;
  }

  for (size_t i = 2; i < av.size (); i++) {
    if (!np->add_pattern (av[i]))
      return strbuf () << "invalid pattern '" << av[i] << "'";
  }
  return NULL;
}

bool
parseconfig (options *op, str cf)
{
  parseargs pa (cf);
  bool errors = false;
#define WARN warn << cf << ":" << line << ": "

#if 0
  // Single line directives
  conftab ct;
  ct
    .add ("ClientTimeout", &op->client_timeout, 0, 60 * 60)
    .add ("ListenPort", &op->listen_port, 0, 65536)
    .add ("PeerMaxQueue", &op->peer_max_queue, 0, 1000000)
    .add ("PeerTimeout", &op->peer_timeout, 0, 60 * 60)
    .add ("CreateUnknownGroups", &op->create_unknown_groups)
    .add ("SyncInterval", &op->sync_interval, 0, 5)
    ;
#endif /* 0 */
  
  int line;
  vec<str> av;
  while (pa.getline (&av, &line)) {
#if 0    
    if (ct.match (av, cf, line, &errors)) {
      continue;
#else
    if (!strcasecmp ("Peer", av[0])) {
      str err = op->parse_peer (av);
      if (err) {
	errors = true;
	WARN << err << "\n";
      }
#endif /* 0 */
#if 1      
    } else if (!strcasecmp("ClientTimeout", av[0])) {
      if (!convertint (av[1], &op->client_timeout) ||
	  op->client_timeout < 0) {
	errors = true;
	WARN << "usage: ClientTimeout seconds\n";
      }
    } else if (!strcasecmp("ListenPort", av[0])) {
      if (!convertint (av[1], &op->listen_port)) {
	errors = true;
	WARN << "usage: ListenPort port\n";
      }
    } else if (!strcasecmp("PeerMaxQueue", av[0])) {
      if (!convertint (av[1], &op->peer_max_queue) ||
	  op->peer_max_queue < 0) {
	errors = true;
	WARN << "usage: PeerMaxQueue numarts\n";
      }
    } else if (!strcasecmp("PeerTimeout", av[0])) {
      if (!convertint (av[1], &op->peer_timeout) ||
	  op->client_timeout < 0) {
	errors = true;
	WARN << "usage: ClientTimeout seconds\n";
      }
    } else if (!strcasecmp("CreateUnknownGroups", av[0])) {
      if (!strcasecmp("true", av[1]) ||
	  !strcasecmp("1", av[1])) {
	op->create_unknown_groups = true;
      } else if (!strcasecmp("false", av[1]) ||
	  !strcasecmp("0", av[1])) {
	op->create_unknown_groups = false;
      } else {
	errors = true;
	WARN << "usage: CreateUnknownGroups true|false\n";
      }
    } else if (!strcasecmp("SyncInterval", av[0])) {
      if (!convertint (av[1], &op->sync_interval) ||
	  op->sync_interval < 0) {
	errors = true;
	WARN << "usage: SyncInterval seconds\n";
      }
#endif /* 1 */	     
    } else {
      warn << cf << ":" << line << ": unknown directive " << av[0] << "\n";
      errors = true;
    }
  }

  return !errors;
}
