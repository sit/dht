#include <async.h>
#include "ida.h"

// This test a bunch of internal functions.  External callers should
// not use these functions at all.
void
test_packunpack ()
{
  vec<u_long> vin;
  vec<u_long> vout;
  vin.push_back (65538);
  u_long x = 42135;
  u_long y = 1;
  for (u_long i = 0; i < 65537; i++)
    vin.push_back ((x*y % 65537));
  
  str encstr = Ida::pack (vin);
  
  bool ok = Ida::unpack (encstr, vout);
  if (!ok)
    exit (1);
  
  if (vin.size () != vout.size ()) {
    warnx << "vin.size () = " << vin.size () << " "
	  << "vout.size () = " << vout.size () << "\n";
    exit (1);
  }

  bool bad = false;
  for (u_long i = 0; i < vin.size (); i++) {
    if (vin[i] != vout[i]) {
      warnx << i << " " << vin[i] << " " << vout[i] << "\n";
      bad = true;
    }
  }
  if (bad)
    exit (1);
}

bool
test_block (str b)
{
  vec<str> frags;
  for (int i = 0; i < 8; i++) {
    str f = Ida::gen_frag (8, b);
    frags.push_back (f);
  }

  strbuf block2;
  bool ok = Ida::reconstruct (frags, block2);
  if (!ok) {
    warnx << "reconstruction failed\n";
    exit (1);
  }
  
  str b2 (block2);
  assert (b2.len() == 8192);
  ok = true;
  for (int i = 0; i < 8192; i++) {
    if (b[i] != b2[i]) {
      warnx << i << " " << b[i] << " " << b2[i] << "\n";
      ok = false;
    }
  }

  return ok;
}

bool
test_single (char c)
{
  strbuf block;
  for (int i = 0; i < 8192; i++)
    block.fmt ("%c", c);
  
  return test_block (block);
}

int
main (int argc, char *argv[])
{
  bool ok;

  warnx << "Testing pack unpack\n";
  test_packunpack ();

  warnx << "Testing 8k single character blocks\n";
  for (int i = 0; i < 256; i++) {
    ok = test_single (i);
    if (!ok)
      exit (1);
  }

  warnx << "Testing 8k cyclical block\n";
  strbuf block;
  for (int i = 0; i < 8192; i++) {
    char c = i % 256;
    block.fmt ("%c", c);
  }
  ok = test_block (block);
  if (!ok)
    exit (1);
}
