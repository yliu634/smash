#include "lookup_fn.h"
#include "storage_node.h"
#include "master_fn.h"
#include "client.h"
#include "name_server.h"

const char *_NodeTypeNames[] = {
    "MasterNode",
    "LookupNode",
    "StorageNode",
    "NameNode"
};
const char **NodeTypeNames = _NodeTypeNames;

const char *_MessageTypeNames[] = {
    "MasterRegister",
    "LookupRegister",
    "StorageRegister",
    "RegisterReply",
    "MasterRegisterInfo",
    "LookupRegisterInfo",
    "StorageRegisterInfo",
    "PullRegisterInfo",
    "LoadInfo",
    "Borrow",
    "Granted",
    "Insert",
    "Remove",
    "Copy",
    "Move",
    "Update",
    "UpdateOthello",
    "UpdateLudo",
    "Return",
    "Read",
    "Locate",
    "Leave",
    "Hot",
    "Size",
    "DumpKeys",
    "Log",
    "ReadReply"
};
const char**MessageTypeNames = _MessageTypeNames;

[[noreturn]] int ns_main(int argc, char **argv) {
  NameServer node;
  while (true) {
    this_thread::sleep_for(1s);
  }
}


[[noreturn]] int lookup_main(int argc, char **argv) {
  Lookup node;
  node.sendRegisterMsg();
  
  while (true) {
    this_thread::sleep_for(1s);
  }
}

[[noreturn]] int master_main(int argc, char **argv) {
  Master node;
  node.sendRegisterMsg();
  
  while (true) {
    this_thread::sleep_for(1s);
  }
}

[[noreturn]] int storage_main(int argc, char **argv) {
  Storage node(atoi(argv[2]), argv[3]);
  node.sendRegisterMsg();
  
  while (true) {
    this_thread::sleep_for(1s);
  }
}
