#include "nodefactory.h"
#include "vivaldinode.h"
#include "node.h"
#include <string>
#include <iostream>
#include <typeinfo>
using namespace std;

NodeFactory *NodeFactory::_instance = 0;

NodeFactory*
NodeFactory::Instance()
{
  if(!_instance)
    _instance = new NodeFactory();
  return _instance;
}

void
NodeFactory::DeleteInstance()
{
  if(!_instance)
    return;
  delete _instance;
}


NodeFactory::NodeFactory()
{
}

NodeFactory::~NodeFactory()
{
}


Node *
NodeFactory::create(string type, IPAddress ip)
{
  Node *n = 0;
  if(type == "Node")
    n = new Node(ip);
  if(type == "VivaldiNode")
    n = new VivaldiNode(ip);
  _nodenames[typeid(*n).name()] = type;
  return n;
}


string
NodeFactory::name(Node *n)
{
  return _nodenames[typeid(*n).name()];
}
