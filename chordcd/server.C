#include "chordcd.h"
#include "rxx.h"

bool
chord_server::setrootfh (str root) 
{
  //                       chord:ade58209f
  static rxx path_regexp ("chord:([0-9a-z]*)$"); 

  if (!path_regexp.search (root)) return false;
  str rootfhstr = path_regexp[1];
  str dearm = dearmor64A (rootfhstr);
  mpz_set_rawmag_be (&rootfh, dearm.cstr (), dearm.len ());
  warn << "root file handle is " << rootfh << "\n";
  //fetch the root file handle too..
  return true;
}


void
chord_server::dispatch (nfscall *sbp)
{
  ref<nfs_fh3> nfh = New refcounted<nfs_fh3> ();
  
  switch(sbp->proc()) {
  case NFSPROC3_READLINK:
  case NFSPROC3_GETATTR: 
  case NFSPROC3_FSSTAT:
  case NFSPROC3_FSINFO:
    *nfh = *sbp->getfh3arg ();
    break;
  case NFSPROC3_LOOKUP: 
    *nfh = sbp->template getarg<diropargs3> ()->dir;
    break;
  case NFSPROC3_ACCESS:
    *nfh = sbp->template getarg<access3args> ()->object;
    break;
  case NFSPROC3_READ:
    *nfh = sbp->template getarg<read3args> ()->file;
    break;
  case NFSPROC3_READDIR:
    *nfh = sbp->template getarg<readdir3args> ()->dir;
    break;
  case NFSPROC3_READDIRPLUS:
    *nfh = sbp->template getarg<readdirplus3args> ()->dir;
    break;

  case NFSPROC3_NULL:
    {
      sbp->reply (NULL);
      return;
    }
    break;
  case NFSPROC_CLOSE:
    {
    }
    break;
  default:
    {
      sbp->error (NFS3ERR_ROFS);
      return;
    }
    break;
  }

  
}


void
chord_server::getdata (chordID ID, str data) {
  
}
