#include "nodefactory.h"
#include "vivaldinode.h"
#include "node.h"
#include <string>
#include <iostream>
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
  Node *e = 0;
  if(type == "Node")
    e = new Node(ip);
  else if(type == "VivaldiNode")
    e = new VivaldiNode(ip);
  return e;
}
