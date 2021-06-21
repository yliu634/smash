#include "common.h"
#include "Smash/node.h"
#include "Smash/lookup_fn.h"
#include "Smash/storage_node.h"
#include "Smash/master_fn.h"
#include "Smash/client.h"
#include "Smash/name_server.h"
#include "Smash/commander.h"
#include "ycsb_workload.h"

void simulateClient() {
  Client client;
  this_thread::sleep_for(1s);
  client.requestTopo();
  
  vector<char> buffer;
  buffer.resize(blockSize);
  
  for (int i = 0; i < blockSize; ++i) {
    buffer[i] = i;
  }
  
  for (int i = 0; i < 10; ++i) {
    buffer[0] = i;
    client.Insert("k" + to_string(i), buffer.data());
    this_thread::sleep_for(1s);
    
    vector<char> tmp = client.Read("k" + to_string(i));
    if (tmp[0] != i) debug_break();
    
    if (i >= 3) {
      tmp = client.Read("k" + to_string(i - 2));
      if (tmp[0] != i - 2) debug_break();
      
      client.Remove("k" + to_string(i - 3));
    }
  }

//  for (int i = 7; i < 10; ++i) {
//    client.Remove("k" + to_string(i));
//  }
  cout << "done! " << endl;
}

int main(int argc, char **argv) {
  commonInit();
  if (argc <= 1) return -1;
  
  if (argv[1] == string("ns")) {
    ns_main(argc, argv);
  } else if (argv[1] == string("master")) {
    master_main(argc, argv);
  } else if (argv[1] == string("lookup")) {
    lookup_main(argc, argv);
  } else if (argv[1] == string("storage")) {
    if (argc <= 3) return -1;
    storage_main(argc, argv);
  } else if (argv[1] == string("client")) {
    simulateClient();
  } else if (argv[1] == string("loadC")) {  // ycsb-like interface
    int records = argc == 4 ? 1000 : atoi(argv[4]);
    YCSB y(argv[2], atoi(argv[3]), records);
    y.loadCeph();
  } else if (argv[1] == string("loadS")) {  // ycsb-like interface
    int records = argc == 4 ? 1000 : atoi(argv[4]);
    YCSB y(argv[2], atoi(argv[3]), records);
    y.loadSmash();
  } else if (argv[1] == string("runC")) {  // ycsb-like interface
    int records = argc == 4 ? 1000 : atoi(argv[4]);
    YCSB y(argv[2], atoi(argv[3]), records);
    y.runCeph();
  } else if (argv[1] == string("runS")) {  // ycsb-like interface
    int records = argc == 4 ? 1000 : atoi(argv[4]);
    YCSB y(argv[2], atoi(argv[3]), records);
    y.runSmash();
  } else if (argv[1] == string("cmd")) {
    Commander c;
    c.startRoutine();
  } else if (argv[1] == string("all")) {
    NameServer ns;
    
    vector<Master *> masters;  // no duplication for simulation
    vector<Lookup *> lookups;
    vector<Storage *> storages;
    
    for (int i = 0; i < ns.nMasters; ++i) {
      auto *node = new Master;
      node->sendRegisterMsg();
      masters.push_back(node);
    }
    
    for (int i = 0; i < ns.nLookups; ++i) {
      auto *node = new Lookup;
      node->sendRegisterMsg();
      lookups.push_back(node);
    }
    
    for (int i = 0; i < ns.nStorages; ++i) {
      auto *node = new Storage(10, "dist/store-" + to_string(i));
      node->sendRegisterMsg();
      storages.push_back(node);
    }
    
    thread([argc, argv]() {
      prctl(PR_SET_NAME, "client main", 0, 0, 0);
      simulateClient();
    }).detach();
    
    while (true) {
      this_thread::sleep_for(1s);
    }
  }
  
  return 0;
}
