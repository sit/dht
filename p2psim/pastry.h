#ifndef __PASTRY_H
#define __PASTRY_H

#include "protocol.h"
#include "node.h"
#include <openssl/bn.h>

class Pastry : public Protocol {
public:
  typedef BIGNUM* NodeID;
  const unsigned idlength;

  Pastry(Node*);
  ~Pastry();

  virtual void join(Args*);
  virtual void leave(Args*);
  virtual void crash(Args*);
  virtual void insert_doc(Args*);
  virtual void lookup_doc(Args*);

private:
  NodeID _id;
};

#endif // __PASTRY_H
