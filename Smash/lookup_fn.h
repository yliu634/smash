#pragma once

#include "node.h"
#include "../Ludo/ludo_cp_dp.h"

class Lookup : public Node {
public:
  Lookup(uint16_t port = 0) : Node("Lookup", port) {}
  
  int thisType() override {
    return LookupNode;
  }
  
  void sendRegisterMsg() override {
    vector<char> msg;
    msg.resize(3);
    *(uint16_t *) msg.data() = port;
    *(msg.data() + 2) = 0;
    
    my_write(nameServer, thisType(), msg);
  }
  
  uint myMaster = -1;
  
  void startRoutine() override {
    Node::startRoutine();
    name = string("Lookup#") + to_string(thisId);
    
    uint64_t sumCap = 0;
    for (auto info:storages) {
      sumCap += info.capacity;
    }
    
    dp = new DataPlaneLudo<K, Locations>;
    uint32_t cap = sumCap / nShards;
    dp->resizeCapacity(cap);
    for (int i = 0; i < nShards && myMaster > 0; ++i) {
      for (uint id:lookupFunctionsForShard[i]) {
        if (id == thisId) myMaster = i;
      }
    }
    dp->setSeed(myMaster * 0xe2211);
    
    fallback.reserve(cap / 10);
  }
  
  mutable recursive_mutex updateLock;
  
  bool onMessage(int msgType, int id, const int fd, const string &ip, vector<char> &msg) override {
    try {
      if (Node::onMessage(msgType, id, fd, ip, msg)) return true;
      
      if (msgType == Read) {
        uint32_t *p = (uint32_t *) msg.data();
        uint32_t seq = p[0];
        uint16_t port = p[1];
        string clientAddr = ip + ":" + to_string(port);
        cout << "rece read" << endl;
        string k = msg.data() + 8;
//      Location loc = locate(k).locs[rand() % nReplicas];
        Location loc;
        loc = locate(k).locs[0];  // always go to main node for latest update
        if (loc.dId == uint32_t(-1)) {
        // log("Error: key not found: " + k);
          my_write(clientAddr, seq, "\0");
          return true;
        }
        
        msg.resize(8 + clientAddr.size() + 1);
        p = (uint32_t *) msg.data();
        p[0] = seq;
        p[1] = loc.blkId;
        memcpy(msg.data() + 8, clientAddr.data(), clientAddr.size() + 1);
        cout << "memcy" << endl;
        my_write(storages[loc.dId].addrPort, Read, msg);
        cout << "sent" << endl;
      } else if (msgType == MessageTypes::Locate) {
        string k = msg.data();
        Locations locations = locate(k);
        my_write(fd, Return, &locations, sizeof(Locations));
      } else if (msgType == Insert) {  // no need for any lock, because only one writer and all the inconsistent data do not hurt
        char mode = msg[0];
        Locations locations = *(Locations *) (msg.data() + 1);
        
        if (mode == 0) {
          int off = 1 + sizeof(Locations);
          vector <Ludo_PathEntry<K>> entries;
          while (off < msg.size()) {
            Ludo_PathEntry<K> entry;
            memcpy(&entry, msg.data() + off, 10);
            
            entry.locatorCC.resize(entry.status);
            memcpy(entry.locatorCC.data(), msg.data() + off + 10, entry.status * 4);
            
            entries.emplace_back(move(entry));
            off += 10 + entry.status * 4;
          }
          if (off > msg.size()) debug_break();
          
          dp->applyInsert(entries, move(locations));
        } else { // mode == 1
          K k = msg.data() + 1 + sizeof(Locations);
          fallback.insert(make_pair(k, *(Locations *) (msg.data() + 1)));
        }
      } else if (msgType == Remove) {
        const string key = K(msg.data());
        fallback.erase(key);
      } else if (msgType == Update) {
        char mode = msg[0];
        Locations locations = *(Locations *) (msg.data() + 1);
        
        if (mode == 0) {
          int off = 1 + sizeof(Locations);
          uint32_t bs;
          memcpy(&bs, msg.data() + off, 4);
          dp->applyUpdate(bs, locations);
        } else { // mode == 1
          K k = msg.data() + 1 + sizeof(Locations);
          fallback.insert(make_pair(k, *(Locations *) (msg.data() + 1)));
        }
      } else if (msgType == UpdateOthello) {
        runtime_error("not implemented");
      } else if (msgType == UpdateLudo) {
        background = new DataPlaneLudo<K, Locations>;
        
        uint32_t *p = (uint32_t *) msg.data();
        DataPlaneOthello<K, uint8_t, 1> &locator = background->locator;
        locator.hab.s = *(uint64_t *) p;
        int i = 2;
        locator.hd.s = p[i++];
        locator.ma = p[i++];
        locator.mb = p[i++];
        
        uint64_t sz = (((uint64_t) locator.ma + locator.mb) * locator.VDL + 63) / 64;
        locator.mem.resize(sz);
        memcpy(locator.mem.data(), msg.data() + 4 * i, sz * 8);
        
        bool notOnlyOthello = msg[4 * i + sz * 8];
        assert(notOnlyOthello);
        
        uint64_t *ps = (uint64_t * )(msg.data() + 4 * i + sz * 8 + 1);
        background->h.s = ps[0];
        background->digestH.s = ps[1];
        uint32_t cnt = background->num_buckets_ = ps[2];
        
        auto *pb = (DataPlaneLudo<K, Locations>::Bucket *) (ps + 3);
        background->buckets.resize(background->num_buckets_);
        memcpy(background->buckets.data(), pb, cnt * sizeof(pb[0]));
        
        mylock_guard g(updateLock);
        fallback.clear();
        swap(dp, background);
        delete background;
      } else return false;
    } catch (exception &e) {
    // log(e.what());
    }
    
    return true;
  }
  
  DataPlaneLudo<K, Locations> *dp, *background;
  unordered_map <K, Locations> fallback;
  
  inline Locations locate(const K &k) const {
    mylock_guard g(updateLock);
    
    auto it = fallback.find(k);
    if (it != fallback.end()) return it->second;
    
    return dp->lookUp(k);
  }
};
