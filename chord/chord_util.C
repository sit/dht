/*
 *
 * Copyright (C) 2000 Frans Kaashoek (kaashoek@lcs.mit.edu)
 * Copyright (C) 2001 Frans Kaashoek (kaashoek@lcs.mit.edu) and 
 *                    Frank Dabek (fdabek@lcs.mit.edu).
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

#include <assert.h>
#include "chord.h"
#include "chord_util.h"
#include "misc_utils.h"
#include <sha1.h>
#include <transport_prot.h>

vec<float>
convert_coords (dorpc_arg *arg)
{
  vec<float> coords;
  for (unsigned int i = 0; i < arg->src_coords.size (); i++)
    coords.push_back ((float)arg->src_coords[i]);
  return coords;
}

void
convert_coords (dorpc_res *res, vec<float> &out)
{
  for (unsigned int i = 0; i < res->resok->src_coords.size (); i++) {
    warn << "converting: " << res->resok->src_coords[i] << "\n";
    float c = ((float)res->resok->src_coords[i]);
    printf ("c:  %f\n", c);
    out.push_back (c);
  }
}

chordID
make_chordID (str addr, int port, int index)
{
  chordID ID;
  // XXX we probably should marshall this!
  str ids = strbuf () << addr << "." << port << "." << index;
  char id[sha1::hashsize];
  sha1_hash (id, ids, ids.len());
  mpz_set_rawmag_be (&ID, id, sizeof (id));  // For big endian
  return ID;
}

chordID
make_chordID (const chord_node_wire &n)
{
  chord_node z = make_chord_node (n);
  return z.x;
}

bool
is_authenticID (const chordID &x, chord_hostname n, int p, int vnode)
{  
  // xxx presumably there's really a smaller actual range
  //     of valid ports.
  if (p < 0 || p > 65535)
    return false;

  // max vnode is a system-wide default.
  if (vnode > chord::max_vnodes)
    return false;

  chordID ID = make_chordID (n, p, vnode);
  return (ID == x);
}
