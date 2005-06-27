#include "dhash_common.h"
#include "dhblock_chash.h"

#include <ida.h>
#include <modlogger.h>
#include <crypt.h>
#include <configurator.h>

#define warning modlogger ("dhblock_chash", modlogger::WARNING)
#define info  modlogger ("dhblock_chash", modlogger::INFO)
#define trace modlogger ("dhblock_chash", modlogger::TRACE)

u_int
dhblock_chash::num_dfrags ()
{
  static bool initialized = false;
  static int v = 0;
  if (!initialized) {
    initialized = Configurator::only ().get_int ("dhash.dfrags", v);
    assert (initialized);
  }
  return v;
}

u_int
dhblock_chash::num_efrags ()
{
  static bool initialized = false;
  static int v = 0;
  if (!initialized) {
    initialized = Configurator::only ().get_int ("dhash.efrags", v);
    assert (initialized);
  }
  return v;
}

u_int
dhblock_chash::num_fetch ()
{
  static bool initialized = false;
  static int v = 0;
  if (!initialized) {
    initialized = Configurator::only ().get_int ("chord.nsucc", v);
    assert (initialized);
  }
  return v;
}

int
dhblock_chash::process_download (blockID key, str frag) 
{

  //check to see if this is a duplicate fragment
  for (size_t j = 0; j < frags.size (); j++) {
    if (frags[j] == frag) {
      warning << "retrieve (" << key
	      << "): duplicate fragment retrieved from successor; "
	      << "same as fragment " << j << "\n";
      return -1; // XXX real error codes?
    }
  }
  frags.push_back (frag);

  strbuf newblock;
  if (!Ida::reconstruct (frags, newblock)) {
    if (frags.size () >= num_dfrags ()) {
      warning << "reconstruction failed.\n";
      return -1; 
    }
    return 0;
  } else {
    str tmp (newblock);
    if (!verify (key.ID, tmp)) {
      if (frags.size () >= num_dfrags ()) {
	warning << "retrieve (" << key
		<< "): verify failed.\n";
	return -1;
      }
    }
    done_flag = true;
    result_str = tmp;
    return 0;
  }
}

str
dhblock_chash::produce_block_data () { 
  assert (done_flag);
  return result_str; 
}
  


str
dhblock_chash::generate_fragment (ptr<dhash_block> block, int n)
{
  u_long m = Ida::optimal_dfrag (block->data.len (), dhash_mtu ());
  if (m > num_dfrags ())
    m = num_dfrags ();

  str frag = Ida::gen_frag (m, block->data);
  return frag;
}

bool
dhblock_chash::verify (chordID key, str data)
{
  char hashbytes[sha1::hashsize];
  sha1_hash (hashbytes, data.cstr (), data.len ());
  chordID ID;
  mpz_set_rawmag_be (&ID, hashbytes, sizeof (hashbytes));  // For big endian
  return (ID == key);
}

// identify function for content hash: marshalling is a nop
vec<str>
dhblock_chash::get_payload (str data)
{
  vec<str> ret;
  ret.push_back (data);
  return ret;
}

str
dhblock_chash::marshal_block (str data)
{
  return data;
}
