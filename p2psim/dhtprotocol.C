#include <assert.h>
#include <lib9.h>
#include <thread.h>
#include <iostream>
#include <stdio.h>
using namespace std;

#include "dhtprotocol.h"
#include "node.h"
#include "p2psim.h"

DHTProtocol::DHTProtocol(Node *n) : Protocol(n)
{
}

DHTProtocol::~DHTProtocol()
{
}
