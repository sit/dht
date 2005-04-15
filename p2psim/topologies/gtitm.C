/*
 * Copyright (c) 2003-2005 Frank Dabek
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

#include "p2psim/topology.h"
#include "p2psim/network.h"
#include "p2psim/parse.h"
#include <iostream>
#include "protocols/protocolfactory.h"
#ifdef HAVE_LIBGB

#include "gtitm.h"

gtitm::gtitm(vector<string> *v)
{

  assert (v->size () > 0);
  strcpy((char *)_filename,((*v)[1]).c_str ());
  _num = atoi (((*v)[0]).c_str ());
  if (v->size () >= 3)
    strcpy((char *)_filename_alt,((*v)[2]).c_str ());
}

gtitm::~gtitm()
{
}

Time
gtitm::latency(IPAddress ip1x, IPAddress ip2x, bool ignored)
{
  Vertex *a, *b;

  IPAddress ip1 = Network::Instance()->first_ip(ip1x);
  IPAddress ip2 = Network::Instance()->first_ip(ip2x);

  ((int)ip1)--;
  ((int)ip2)--;
  assert ((int)ip1 < _num && (int)ip2 < _num);
  a = &g->vertices[ip1];
  b = &g->vertices[ip2];
  long key = ip1*_num + ip2;
  if (memo[key]) return memo[key];

  Time ret = 1000*dijkstra (a, b, g, NULL);
  memo[key] = ret;

  return ret;
}


void
gtitm::swap ()
{
  char tmp[64];
  strcpy (tmp, _filename);
  strcpy (_filename, _filename_alt);
  strcpy (_filename_alt, tmp);
  g = restore_graph ((char *)_filename);
  if (!g) {
    cerr << "(swap) unable to parse graph in file: " << _filename << "\n";
    exit (-1);
  }
  if (_num > g->n) {
    cerr << "gtitm (swap): requested " << _num 
	 << " hosts, but only " << g->n 
	 << " are in the file\n";
    exit (-1);
  }
  memo.clear ();
}

void
gtitm::parse(ifstream &ifs)
{
  string line;
  int ip_addr = 0;
  
  g = restore_graph ((char *)_filename);
  if (!g) {
    cerr << "unable to parse graph in file: " << _filename << "\n";
    exit (-1);
  }
  if (_num > g->n) {
    cerr << "gtitm: requested " << _num 
	 << " hosts, but only " << g->n 
	 << " are in the file\n";
    exit (-1);
  }

  //TODO: bind nodes to vertices in an interesting way

  for (int i = 1; i <= _num; i++) {
    ip_addr = i;
    Node *p = ProtocolFactory::Instance()->create(ip_addr);
    // this assert doesn't work since the firstip stuff was added 
    // assert(!Network::Instance()->getnode(ip_addr));
    send(Network::Instance()->nodechan(), &p);

  }
}

#endif
