#pragma once

#include "../common.h"
#include "socket_node.h"
#include <boost/asio/ip/host_name.hpp>

typedef string K;
const uint blockSize = 4 * 1024 * 1024;

const int MAX_ADD_LEN = 100;

struct MasterNodeInfo {
  bool in = true;
  char addrPort[MAX_ADD_LEN];
};

struct LookupNodeInfo {
  bool in = true;
  char addrPort[MAX_ADD_LEN];
  bool supportFilter;
};

struct StorageNodeInfo {
  bool in = true;
  char addrPort[MAX_ADD_LEN];
  uint32_t capacity; // unit: 4MB block
};

struct Location {
  uint32_t dId = -1, blkId = 0;
  
  bool operator==(const Location &other) const {
    return dId == other.dId && blkId == other.blkId;
  }
  
  bool operator!=(const Location &other) const {
    return !(*this == other);
  }
};

struct Locations {
  Location locs[3];   // TODO the number "3" is compile-time constant. So nReplicas should be 3
  
  bool operator==(const Locations &other) const {
    return locs[0] == other.locs[0] && locs[1] == other.locs[1] && locs[2] == other.locs[2];
  }
  
  bool operator!=(const Locations &other) const {
    return !(*this == other);
  }
};

class Node : public SocketNode {
public:
  int my_write(const string &addrPort, uint type, const vector<char> &data) {
    return SocketNode::my_write(addrPort, type, thisId, data);
  }
  
  int my_write(const string &addrPort, uint type, const vector<u_char> &data) {
    return SocketNode::my_write(addrPort, type, thisId, data);
  }
  
  int my_write(const string &addrPort, uint type, const string &data) {
    return SocketNode::my_write(addrPort, type, thisId, data);
  }
  
  int my_write(const string &addrPort, uint32_t type, const void *data, uint32_t length) {
    return SocketNode::my_write(addrPort, type, thisId, data, length);
  }
  
  int my_write(int fd, uint type, const vector<char> &data) {
    return SocketNode::my_write(fd, type, thisId, data);
  }
  
  int my_write(int fd, uint type, const vector<u_char> &data) {
    return SocketNode::my_write(fd, type, thisId, data);
  }
  
  int my_write(int fd, uint type, const string &data) {
    return SocketNode::my_write(fd, type, thisId, data);
  }
  
  // format: 4B type, 4B length, and then the msg under that length
  int my_write(int fd, uint32_t type, const void *data, uint32_t length) {
    return SocketNode::my_write(fd, type, thisId, data, length);
  }
  
  uint nShards = -1;
  uint nReplicas = -1;
  
  uint nMasters = -1;
  uint nLookups = -1;
  uint nStorages = -1;
  vector<MasterNodeInfo> masters;  // no duplication for simulation
  vector<LookupNodeInfo> lookups;
  vector<StorageNodeInfo> storages;
  
  string nameServer;
  
  virtual int thisType() { return -1; }
  
  inline Node(string name, uint16_t port = 0) : SocketNode(name, port) {
    ifstream f("../config.txt");
    f >> nameServer;
    
    f >> nReplicas;
    nReplicas = 3;  // TODO make it really configurable
    
    f >> nMasters;
    f >> nLookups;
    f >> nStorages;
    
    nShards = nMasters;
    
    f.close();
  }
  
  vector<vector<uint>> lookupFunctionsForShard;
  
  // after registration, we know the whole system, and initialize accordingly
  virtual void startRoutine() {
    alive = true;
    lookupFunctionsForShard.resize(nShards);
    
    for (int i = 0; i < nLookups; ++i) {
      lookupFunctionsForShard[i * nShards / nLookups].push_back(i);
    }
  }
  
  virtual ~Node() {
    stop();
  }
  
  inline uint getShard(const K &k) {  // also getting the master managing the shard
    return Hasher32<K>(0xe2211)(k) % nShards;
  }
  
  uint getAnyLookupNode(const K &k) {
    vector<uint> nodes = getLookupNodes(k);
    return nodes[rand() % nodes.size()];
  }
  
  vector<uint> getLookupNodes(const K &k) {
    return getLookupNodes(getShard(k));
  }
  
  vector<uint> getLookupNodes(uint shard) {
    return lookupFunctionsForShard[shard];
  }
  
  virtual void sendRegisterMsg() {
    // log(string("Register as type ") + NodeTypeNames[thisType()]);
    my_write(nameServer, thisType(), &port, 2);
  }
  
  int getId() const {
    string host = boost::asio::ip::host_name();
    
    int id = -1;
    if (host.find("machine") != 0) {
      try {
        string ids = host.substr(string("machine").length());
        id = atoi(ids.c_str());
      } catch (exception &e) {
        id = -1;
      }
    }
    return id;
  }
  
  bool alive = true;
  
  void stop() {
    alive = false;
  }
  
  bool onMessage(int msgType, int id, const int fd, const string &ip, vector<char> &msg) override {
    ostringstream oss;
    oss << "Received command: " << MessageTypeNames[min(msgType, (int) ReadReply)] << ", " << id << ", " << ip << ", my name: " << name;
// log(oss.str());
    
    if (msgType == RegisterReply) {
      thisId = atoi(msg.data());
    } else if (msgType == MasterRegisterInfo) {
      masters.resize(nMasters);
      memcpy(masters.data(), msg.data(), nMasters * sizeof(MasterNodeInfo));  // server flaw in production. good for experiments
    } else if (msgType == LookupRegisterInfo) {
      lookups.resize(nLookups);
      memcpy(lookups.data(), msg.data(), nLookups * sizeof(LookupNodeInfo));
    } else if (msgType == StorageRegisterInfo) {
      storages.resize(nStorages);
      memcpy(storages.data(), msg.data(), nStorages * sizeof(StorageNodeInfo));
      
      startRoutine();
    } else {
      return false;
    }
    
    return true;
  }
};


[[noreturn]] int ns_main(int argc, char **argv);

[[noreturn]] int lookup_main(int argc, char **argv);

[[noreturn]] int master_main(int argc, char **argv);

[[noreturn]] int storage_main(int argc, char **argv);
