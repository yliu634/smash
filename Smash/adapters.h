#pragma once

#include "socket_node.h"
#include "../common.h"

// at each node, there is a loop accepting TCP messages. for mapping updates at lookup/data placement at storage/data register at master
// at other nodes, they communicate with others via these adapters

class MasterAdapter {

};

class LookUpAdapter {

};

class StorageAdapter {

};

class ClientAdapter {

};
