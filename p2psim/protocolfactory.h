#ifndef __PROT_FACTORY_H
#define __PROT_FACTORY_H

#include "node.h"
#include <typeinfo>
// #include <map>

using namespace std;

class ProtocolFactory {
public:
  static ProtocolFactory* Instance();
  static void DeleteInstance();
  Protocol *create(string, Node*);

private:
  static ProtocolFactory *_instance;
  ProtocolFactory();
  ~ProtocolFactory();
};

#endif // __PROT_FACTORY_H
