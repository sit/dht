#include "dhash_common.h"
#include "mud_obj.h"

void
mud_ID_put (char *buf, chordID id)
{
  bzero (buf, ID_SIZE);
  mpz_get_rawmag_be (buf, ID_SIZE, &id);
}

void
mud_ID_get (chordID *id, char *buf)
{
  mpz_set_rawmag_be (id, (const char *) buf, ID_SIZE);
}

