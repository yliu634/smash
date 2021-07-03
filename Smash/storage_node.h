#pragma once

#include <fcntl.h>
#include "node.h"
#include "../CuckooPresized/cuckoo_ht.h"

class Storage : public Node {
public:
  struct Log {
    uint16_t count; // low 6 bits are fraction bits. Just add 64 for acc.
    uint16_t time;  // should be decreased by 10% every 8 sec
    
    inline Log &update(uint acc = 0) {
      uint16_t curr = std::time(0);
      uint16_t diff = curr - time;
      
      if (diff >= 8) {
        count *= pow(0.9, diff / 8.0);
        time = curr;
      }
      
      if (acc) {
        count = min(count + 64U * acc, (uint32_t(-1)) >> 16);
      }
      return *this;
    }
  };
  
  int storageFile = -1; // fd
  uint32_t size;
  string fileName;
  
  recursive_mutex logLock;
  CuckooHashTable<uint32_t, Log> logs = CuckooHashTable<uint32_t, Log>(500U);
  
  inline void acc(const uint32_t blkId, uint times = 1) {
    mylock_guard g(logLock);
    Log log;
    
    if (!logs.lookUp(blkId, log)) {
      log = {(uint16_t)(64 * times), (uint16_t) time(0)};
      logs.overwriteWorst(blkId, log, [](uint32_t &blkId, Log &log) { return log.update().count; });
    } else {
      log.update(times);
      logs.changeValue(blkId, log);
    }
  }
  
  void onCongestion() {
    vector <pair<int, uint>> tmp;
    tmp.reserve(512);
    
    logLock.lock();
    for (auto &bucket: logs.buckets_) {  // all buckets
      for (int slot = 0; slot < 4; ++slot) {
        if (bucket.occupiedMask & (1ULL << slot)) {
          bucket.values[slot].update();
          tmp.emplace_back(-bucket.values[slot].count, bucket.keys[slot]);
        }
      }
    }
    logLock.unlock();
    
    sort(tmp.begin(), tmp.end());
    
    int count = min(5UL, tmp.size());
    vector <u_char> msg;
    msg.resize(count * sizeof(uint));
    uint *p = (uint *) msg.data();
    for (int i = 0; i < count; ++i) {
      p[i] = tmp[i].second;
    }
    
    my_write(nameServer, Hot, msg);
  }
  
  Storage(uint64_t size, string fileName, uint16_t port = 0) :
      Node("Storage", port), size(size), fileName(fileName) {
    storageFile = open(fileName.c_str(), O_RDWR | O_CREAT, 0666);
    ftruncate(storageFile, size * blockSize);
  }
  
  ~Storage() {
    stop();
  }
  
  void sendRegisterMsg() override {
    vector<char> msg;
    msg.resize(6);
    *(uint16_t *) msg.data() = port;
    *(uint32_t * )(msg.data() + 2) = size;
    
    my_write(nameServer, thisType(), msg);
  }
  
  int thisType() override {
    return StorageNode;
  }
  
  void startRoutine() override {
    Node::startRoutine();
    name = string("Storage#") + to_string(thisId);
  }
  
  void stop() {
    close(storageFile);
  }
  
  recursive_mutex locks[8192];
  
  bool onMessage(int msgType, int id, const int fd, const string &ip, vector<char> &msg) override {
    try {
      if (Node::onMessage(msgType, 0, fd, ip, msg)) return true;
      
      if (msgType == Insert || msgType == Remove) {
        Location *p = (Location *) msg.data();
        assert(p->dId == thisId);
        cout << "insert or remove received" << endl;
        if (msgType == Insert) {
          acc(p->blkId);
          mylock_guard g(locks[p->blkId % 8192]);
          pwrite(storageFile, p + nReplicas, msg.size() - sizeof(Location) * nReplicas, uint64_t(p->blkId) * blockSize);
        }
        
        bool result = true;
        Location next = p[1];
        if (next.dId != uint32_t(-1)) {
          // next presents. let it handle the rest
          p[0] = p[1];
          p[1] = p[2];
          p[2] = {uint32_t(-1), 0};
          cout << "and " << next.dId << endl;
          int followerFd = connectToServer(storages[next.dId].addrPort);
          my_write(followerFd, msgType, msg);
          result &= (get<2>(my_read(followerFd))[0] == 1);
          close(followerFd);
        }
        
        my_write(fd, Return, &result, 1);
      } else if (msgType == Read) {
        uint32_t seq = *(uint32_t *) msg.data();
        uint64_t blkId = *((uint32_t *) msg.data() + 1);
        acc(blkId);
        
        vector<char> buff(blockSize);
        mylock_guard g(locks[blkId % 8192]);
        pread(storageFile, buff.data(), blockSize, blkId * blockSize);

//      if (notValid) {   // not implemented. if the wrong value, return. should in some way store the full key
//        buff.resize(1);
//      }
        
        my_write(msg.data() + 8, seq, buff);
      } else if (msgType == Copy || msgType == Move) {
        uint32_t *p = (uint32_t *) msg.data();
        
        uint32_t sBlkId = p[0];
        uint32_t dSId = p[1];
        uint32_t dBlkId = p[2];
        cout << "copy from me to " << p[1] << endl;
        Locations onlyFirst;
        onlyFirst.locs[0] = {dSId, dBlkId};
        
        vector<char> buff(sizeof(Locations) + blockSize);
        mylock_guard g(locks[sBlkId % 8192]);
        pread(storageFile, buff.data() + sizeof(Locations), blockSize, sBlkId * blockSize);
        *(Locations *) buff.data() = onlyFirst;
        
        my_write(storages[dSId].addrPort, Insert, buff);
      } else {
        return false;
      }
    } catch (exception &e) {
      log(e.what());
    }
    
    return true;
  }
};
