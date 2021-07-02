#pragma once

#include "../common.h"
#include "node.h"
#include "lookup_fn.h"

class Client : public Node {
public:
  Client(uint16_t port = 0) : Node("Client", port) {
    SocketNode::silent = true;
    requestTopo();
  }
//  unordered_map<K, Locations> dynamicCache;  // the return from storage node should include the
//
//  inline void UpdateCache(const K &k, Locations locations) {
//    dynamicCache.insert(make_pair(k, locations));
//  }
  
  bool onMessage(int msgType, int id, const int fd, const string &ip, vector<char> &msg) override {
    if (Node::onMessage(msgType, 0, fd, ip, msg)) return true;
    
    if (msgType == MessageTypes::Update) {
      K k = msg.data() + sizeof(Locations);
//      UpdateCache(k, *(Locations *) msg.data());
    } else if (msgType >= ReadReply) {  // all other msg types are for read reply
      mylock_guard g(commitBufLock);
      commitBuffer[msgType] = msg;
    } else return false;
    
    return true;
  }
  
  void requestTopo() {
    my_write(nameServer, PullRegisterInfo, &port, 2);
    
    while (masters.empty()) {  // use triangle communication, so the answer is not available via the current duplex TCP channel.
      this_thread::sleep_for(20ms);
    }
  }
  
  recursive_mutex commitBufLock;
  unordered_map <uint32_t, vector<char>> commitBuffer;
  
  vector<char> Read(const K &k) {
    vector<char> result;

//// log("Start reading key: " + k);

    while (result.size() != blockSize) {  // master/lookup may be not fully constructed
      if (result.size()) this_thread::sleep_for(1s);

      uint lId = getAnyLookupNode(k);

      uint32_t seq = 0;
      while (seq < 100) seq = rand();

      vector<char> msg(k.length() + 8 + 1);
      uint32_t *p = (uint32_t *) msg.data();
      p[0] = seq;
      p[1] = port;
      memcpy(msg.data() + 8, k.data(), k.length() + 1);

      my_write(lookups[lId].addrPort, MessageTypes::Read, msg);

      for (int count = 0; count <
                          5; ++count) {  // use triangle communication, so the answer is not available via the current duplex TCP channel.
        this_thread::sleep_for(20ms);

        mylock_guard g(commitBufLock);
        const auto &it = commitBuffer.find(seq);  // to avoid possible reorders, use a mapping to buffer
        if (it != commitBuffer.end()) {           // and each function instance polls the buffer
          result = it->second;
          commitBuffer.erase(it);
          break;
        }
      }
    }

//// log("End reading key: " + k);
    return result;
  }
  
  void Insert(const K &k, void *data) { // size is fixed 4MB
//// log("Start inserting key: " + k);
    
    uint mId = getShard(k);
    uint type = MessageTypes::Insert;
    size_t locSize = nReplicas * sizeof(Location);
    uint length = blockSize + locSize;
    
    int fd = connectToServer(masters[mId].addrPort);
    my_write(fd, type, k);
    vector<char> v = get<2>(my_read(fd));
    close(fd);
    
    Location *locs = (Location *) v.data();
    
    fd = connectToServer(storages[locs[0].dId].addrPort);
    cout << "Insert to "<< locs[0].dId << " in client.h" << end;
    raw_write(fd, &type, 4);   // for efficiency
    raw_write(fd, &thisId, 4);
    
    raw_write(fd, &length, 4);
    raw_write(fd, locs, locSize);
    raw_write(fd, data, blockSize);
    
    bool succ = get<2>(my_read(fd))[0];
    if (!succ) debug_break();
    close(fd);

//// log("End inserting key: " + k);
  }
  
  void Update(const K &k, void *data) { // size is fixed 4MB
//// log("Start updating key: " + k);
    
    uint lId = getAnyLookupNode(k);
    size_t locSize = nReplicas * sizeof(Location);
    uint length = blockSize + locSize;
    
    uint type = MessageTypes::Locate;
    int fd = connectToServer(lookups[lId].addrPort);
    my_write(fd, type, k);
    vector<char> v = get<2>(my_read(fd));
    close(fd);
    
    Location *locs = (Location *) v.data();
    
    if (v.empty() || locs[0].dId == uint32_t(-1)) {
      cerr << "Lookup fail for key " << k << ", lId: " << lId << ". Trying to recover via master" << endl;
      uint mId = getShard(k);
      
      uint type = MessageTypes::Insert;
      size_t locSize = nReplicas * sizeof(Location);
      uint length = blockSize + locSize;
      
      int fd = connectToServer(masters[mId].addrPort);
      my_write(fd, type, k);
      v = get<2>(my_read(fd));
      close(fd);
      
      if (v.empty())
        cerr << "Still fail" << endl;
    }

//// log("Updating key: " + k + " on storage " + to_string(locs[0].dId));
    type = MessageTypes::Insert;
    fd = connectToServer(storages[locs[0].dId].addrPort);
    raw_write(fd, &type, 4);   // for efficiency
    raw_write(fd, &thisId, 4);
    
    raw_write(fd, &length, 4);
    raw_write(fd, locs, locSize);
    raw_write(fd, data, blockSize);
    
    bool succ = get<2>(my_read(fd))[0];
    if (!succ) debug_break();
    close(fd);

//// log("End updating key: " + k);
  }
  
  void Remove(const K &k) {
//// log("Start removing key: " + k);
    
    uint mId = getShard(k);
    uint type = MessageTypes::Remove;
    
    int fd = connectToServer(masters[mId].addrPort);
    my_write(fd, type, k);
    bool succ = get<2>(my_read(fd))[0];
    if (!succ) debug_break();
    close(fd);

//// log("End removing key: " + k);
  }
};
