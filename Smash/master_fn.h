#pragma once

#include <bitset>
#include <experimental/filesystem>
#include "node.h"
#include "../utils/CompactArray.h"
#include "../Ludo/ludo_cp_dp.h"
#include "../utils/fibonacci_queue.h"

using namespace std::experimental;

class Master;

// first available block on the least loaded disk. RESERVE before sending!
inline Locations allocateDefault(const K &k, Master *_this, uint8_t count = 3);
inline Locations allocateDefaultLeave(const K &k, Master *_this, set<uint> & st, uint8_t count = 3);

class Master : public Node {
public:
  Locations (*allocate)(const K &k, Master *_this, uint8_t count);
  
  std::function<bool(uint, uint)> comp = [this](uint i1, uint i2) {
    return loadInfo[i1].first * loadInfo[i2].second > loadInfo[i2].first * loadInfo[i1].second;
  };
  
  Master(uint16_t port = 0, Locations (*allocate)(const K &, Master *, uint8_t count) = 0
  ) : Node("Master", port), allocate(allocate ? allocate : &allocateDefault), leastLoaded(comp) {}
  
  int thisType() override {
    return MasterNode;
  }
  
  // dedicated disk blocks one bitmap per disk
  vector <CompactArray<1>> allocated;                   // [disk #] -> bulk bitmap
  vector <unordered_map<uint32_t, CompactArray<1>>> occupied;  // [disk #] [bulk #] -> block bitmap
  
  vector <pair<uint, uint>> loadInfo;                 // [disk #] -> (# of occupied blocks, # of allocated blocks)
  fibonacci_queue<uint, function < bool(uint, uint)>> leastLoaded;
  vector <uint16_t> lastAvailable;  // [disk #] -> last available bulk. for fast round-robin.
  
  ControlPlaneLudo<K, Locations> ludo;   // ludo is the main table. if the main is under construction, the fallback will buffer the requests.
  unordered_map <K, Locations> fallback;
  // if the fallback is full, no further updates are accepted. after construction, the fallback is merged into main. if merge fails, main rebuild again
  
  typedef LudoUpdateResult<K> UpdateResult;
  
  thread daemon, build;
  vector<int> updateChannels;

//  vector<recursive_mutex> locks;
  recursive_mutex updateLock, sendLock, loadLock;
  bool building = false;
  
  inline bool bulkEmpty(uint32_t dId, uint32_t bulkId) {
    const uint64_t *mem = occupied[dId].find(bulkId)->second.getMem();
    return mem[0] == 0 && mem[1] == 0 && mem[2] == 0 && mem[3] == 0;
  }
  
  inline bool bulkFull(uint32_t dId, uint32_t bulkId) {
    const uint64_t *mem = occupied[dId].find(bulkId)->second.getMem();
    
    const uint64_t FULL = uint64_t(-1ULL);
    return mem[0] == FULL && mem[1] == FULL && mem[2] == FULL && mem[3] == FULL;
  }
  
  inline int bulkFreeIndex(uint32_t dId, uint32_t bulkId) {
    const uint64_t *mem = occupied[dId].find(bulkId)->second.getMem();
    int i = 0;
    while (mem[i] == uint64_t(-1ULL) && i < 4) ++i;
    if (i == 4) return -1;
    
    return i * 64 + ffsl(~mem[i]) - 1;
  }
  
  // when the logs are full, aggregated into a new snapshot. In fact, just dump current memory structure is fine
  int logFile = -1;    // every 4KB, and should be roughly k times larger than the snapshot. Say k=5
  uint64_t logFileSize = 256 * 1024 * 1024;
  uint64_t logFileOffset = 0;
  char logBuffer[4096]; // flush to disk every 4KB
  uint64_t logBufferOffset = 0;
  
  recursive_mutex dumpLock;
  
  void dump() {
    updateLock.lock();
    vector <u_char> updateMsg = serializeLudo(true);
    
    logFileOffset = 0;
    logBufferOffset = 0;
    logFileSize = updateMsg.size() * 2;
    
    string logFileName = "back/" + name + ".buffer";
    logFile = open(logFileName.c_str(), O_WRONLY | O_CREAT, 0666);
    ftruncate(logFile, logFileSize);
    updateLock.unlock();
    
    mylock_guard g(dumpLock);
    string tmp = "back/" + name + ".snapshot.new";
    string snapshotFileName = "back/" + name + ".snapshot";
    
    int f = open(tmp.c_str(), O_WRONLY | O_CREAT, 0666);
    ftruncate(f, updateMsg.size());
    pwrite(f, updateMsg.data(), updateMsg.size(), 0);
    close(f);
    
    filesystem::rename(tmp, snapshotFileName);
  }
  
  // already synchronized by updateLock, just think as a single thread
  inline void appendLog(string &k, Locations &locations) {
    uint kOffset = 0;
    
    while (logBufferOffset + k.length() + 1 > 4096) {
      uint64_t toWrite = 4096 - logBufferOffset;
      memcpy(logBuffer + logBufferOffset, k.data() + kOffset, toWrite);
      pwrite(logFile, logBuffer, 4096, logFileOffset);
      
      logBufferOffset = 0;
      kOffset += toWrite;
      logFileOffset += 4096;
      
      if (logFileOffset > logFileSize) {
        dump();
        return;
      }
    }
    
    unsigned long toWrite = k.length() + 1 - kOffset;
    memcpy(logBuffer + logBufferOffset, k.data() + kOffset, toWrite);
    logBufferOffset += toWrite;
    
    if (logBufferOffset + sizeof(Locations) > 4096) {
      toWrite = 4096 - logBufferOffset;
      memcpy(logBuffer + logBufferOffset, &locations, toWrite);
      pwrite(logFile, logBuffer, 4096, logFileOffset);
      
      logBufferOffset = 0;
      logFileOffset += 4096;
      
      if (logFileOffset > logFileSize) {
        dump();
        return;
      }
    }
    
    memcpy(logBuffer + logBufferOffset, (char *) &locations + toWrite, sizeof(Locations) - toWrite);
    logBufferOffset += sizeof(Locations) - toWrite;
  }

// after registration, we know the whole system, and initialize accordingly
  void startRoutine() override {
    Node::startRoutine();
    name = string("Master#") + to_string(thisId);
    
    filesystem::create_directory("back");
    
    string logFileName = "back/" + name + ".buffer";
    logFile = open(logFileName.c_str(), O_WRONLY | O_CREAT, 0666);
    logFileSize = 256 * 1024 * 1024;
    ftruncate(logFile, logFileSize);
    
    uint64_t sumCap = 0;
    for (auto info:storages) {
      sumCap += info.capacity;
    }
    
    uint32_t cap = sumCap / nShards;
    ludo.resizeCapacity(cap);
    ludo.setSeed(thisId * 0xe2211);
    fallback.reserve(cap / 10);
    
    for (uint lId: getLookupNodes(thisId)) {
      updateChannels.push_back(connectToServer(lookups[lId].addrPort));
    }
    
    loadInfo.resize(nStorages);
    lastAvailable.resize(nStorages);
    
    allocated.reserve(nStorages);
    occupied.resize(nStorages);
//    keysAtSn.reserve(nStorages);
    
    for (uint dId = 0; dId < storages.size(); ++dId) {
      auto &info = storages[dId];
      uint64_t nBulks = (info.capacity + 255) / 256;   // 1GiB bulk
      allocated.emplace_back(nBulks);
      
      auto &_occupied = occupied[dId];
      CompactArray<1> &_allocated = allocated.back();
      pair <uint, uint> &_load = loadInfo[dId];
      
      for (int bulkId = 0; bulkId < nBulks; ++bulkId) {
        if (bulkId * nMasters / nBulks == thisId) {
          _allocated.memSet(bulkId, 1);   // initially, the disks are evenly allocated to masters
          CompactArray<1> y = CompactArray<1>(256);
          uint realBlocks = 256;
          if (bulkId == nBulks - 1 && info.capacity % 256) {
            realBlocks = info.capacity % 256;
            
            for (int i = realBlocks; i < 256; ++i) {
              y.memSet(i, 1);
            }
          }
          _occupied.insert(make_pair(bulkId, y));
          _load.second += realBlocks;
        }
      }
    }
    
    for (uint i = 0; i < nStorages; ++i) {
      leastLoaded.push(i);
    }
    
    daemon = thread([this] {
      prctl(PR_SET_NAME, (name + " daemon").c_str(), 0, 0, 0);
      while (alive) {
        // wait for a random time, bounded [,]
        uint r = full_debug ? 2 : 10000 + rand() % 5000;
        
        std::chrono::seconds t(r);
        this_thread::sleep_for(t);
        
        // send load info of disks
        // for busy disks, the name server will try to borrow some from other masters
        sendLoadInfo();
      }
    });
    daemon.detach();
  }
  
  ~Master() {
    stop();
  }
  
  void stop() {
    Node::stop();
    
    for (int fd: updateChannels) {
      close(fd);
    }
  }
  
  void onBorrowMsg(uint32_t mId, uint32_t dId, uint32_t nBulks) {
    // decide the bulks to borrow. send to the name server and mId
    CompactArray<1> &bulkAllocBitmap = allocated[dId];
    vector<char> msg(8 + bulkAllocBitmap._m.size() * 8);
    
    uint32_t *p = (uint32_t *) msg.data();
    p[0] = mId;
    p[1] = dId;
    CompactArray<1> grantedBitmap(bulkAllocBitmap.capacity, p + 2);
    uint cnt = 0;
    
    mylock_guard g(loadLock);
    for (uint bulkId = 0; bulkId < bulkAllocBitmap.capacity && cnt < nBulks; ++bulkId) {
      if (!bulkAllocBitmap.memGet(bulkId)) continue;
      
      if (bulkEmpty(dId, bulkId)) {  // assuming fixed 256 blocks in a bulk
        grantedBitmap.memSet(bulkId, 1);
        allocated[dId].memSet(bulkId, 0);
        occupied[dId].erase(bulkId);
        cnt++;
      }
    }
    loadInfo[dId].second -= cnt * 256;
    if (cnt) leastLoaded.decrease(dId);
    
    g.release();
    
    my_write(nameServer, Granted, msg);
    my_write(masters[mId].addrPort, Granted, msg);
  }
  
  void onGrantedMsg(uint32_t fromMasterId, vector<char> &msg) {
    // decide the blocks to borrow. maybe use some compact
    uint32_t *p = (uint32_t *) msg.data();
    uint32_t toMasterId = p[0];
    uint32_t dId = p[1];
    
    if (toMasterId != thisId) {
      cerr << "Granted msg to master#" << toMasterId << " wrongly sent to master#" << thisId << endl;
      return;
    }
    uint64_t nBulks = (storages[dId].capacity + 255) / 256;
    
    CompactArray<1> bitmap(nBulks, p + 2);
    CompactArray<1> &_allocated = allocated[dId];
    unordered_map <uint32_t, CompactArray<1>> &_occupied = occupied[dId];
    
    mylock_guard g(loadLock);
    uint cnt = 0;
    for (uint64_t bulkId = 0; bulkId < nBulks; ++bulkId) {
      if (!bitmap.memGet(bulkId)) continue;
//      if (allocated[dId][bulkId] == fromMasterId)  // we assume nodes are honest
      _allocated.memSet(bulkId, 1);
      _occupied.insert(make_pair(bulkId, CompactArray<1>(256)));
      cnt++;
    }
    
    loadInfo[dId].second += cnt * 256;
    if (cnt) leastLoaded.increase(dId);
  }
  
  void sendLoadInfo() {
    CompactArray<4> loads(nStorages);
    
    for (uint i = 0; i < nStorages; ++i) {
      auto &pair = loadInfo[i];
      double load = pair.first ? (double) pair.first / pair.second : 0;
      loads.memSet(i, uint(load * 15));
    }
    
    my_write(nameServer, LoadInfo, loads.getMem(), (loads.capacity + 1) / 2);
  }
  
  vector <u_char> serializeLudo(bool notOnlyOthello = true) {
    updateLock.lock();  // add the lock here is to prevent potential overlaps with Ludo updates
    
    int fallbackInferredSize = 0;
    if (!fallback.empty()) {
      fallbackInferredSize = fallback.size() * (fallback.begin()->first.size() * 2 + sizeof(Locations));
    }
    vector <u_char> updateMsg;
    
    unsigned long othelloSize = 4 + 8 + 4 + 4 + 8 * ludo.locator.mem.size();
    unsigned long fallbackOffset = 0;
    
    if (!notOnlyOthello) {
      updateMsg.resize(othelloSize + 1);
      fallbackOffset = othelloSize;
      updateMsg.reserve(othelloSize + fallbackInferredSize);
    } else {
      fallbackOffset = othelloSize + 1 + 8 + 8 + 8 + ludo.num_buckets_ * sizeof(DataPlaneLudo<K, Locations>::Bucket);
      updateMsg.resize(fallbackOffset);
      updateMsg.reserve(fallbackOffset + fallbackInferredSize);
    }
    
    thread t1 = thread([this, notOnlyOthello, othelloSize](uint32_t *p) {
      int i = 0;
      p[i++] = ludo.locator.hab.s;
      p[i++] = ludo.locator.hab.s >> 32;
      p[i++] = ludo.locator.hd.s;
      p[i++] = ludo.locator.ma;
      p[i++] = ludo.locator.mb;
      
      uint64_t sz = (((uint64_t) ludo.locator.ma + ludo.locator.mb) * ludo.locator.VDL + 63) / 64;
      assert(sz == ludo.locator.mem.size());
      memcpy((uint8_t *) p + 4 * i, ludo.locator.mem.data(), 8 * sz);
      
      ((uint8_t *) p)[othelloSize] = notOnlyOthello;
      
      if (notOnlyOthello) {
        uint64_t *ps = (uint64_t * )((uint8_t *) p + othelloSize + 1);
        ps[0] = ludo.h.s;
        ps[1] = ludo.digestH.s;
        ps[2] = ludo.num_buckets_;
        
        auto *pi = (DataPlaneLudo<K, Locations>::Bucket *) ((uint8_t *) p + othelloSize + 1 + 24);
        
        for (auto &cpBucket: ludo.buckets_) {
          DataPlaneLudo<K, Locations>::Bucket dpBucket;
          dpBucket.seed = cpBucket.seed;
          
          const FastHasher64<K> locateHash(cpBucket.seed);
          
          for (char slot = 0; slot < 4; ++slot) {
            if (cpBucket.occupiedMask & (1U << slot)) {
              const K &k = cpBucket.keys[slot];
              dpBucket.values[locateHash(k) >> 62] = cpBucket.values[slot];
            }
          }
          
          memcpy(pi++, &dpBucket, sizeof(DataPlaneLudo<K, Locations>::Bucket));
        }
      }
    }, (uint32_t *) updateMsg.data());
    
    vector <uint8_t> fallbackBuffer;
    fallbackBuffer.reserve(fallbackInferredSize);
    
    thread t2 = thread([this](vector <uint8_t> *fallbackBuffer) {
      uint offset = 0;
      
      for (auto &p:fallback) {
        int kSize = p.first.size();
        fallbackBuffer->resize(
            offset + kSize + 1 + sizeof(Locations));  // hopefully, won't trigger a realloc because we allocated much
        
        memcpy(fallbackBuffer->data() + offset, p.first.data(), kSize + 1);
        *(Locations *) (fallbackBuffer->data() + offset + kSize + 1) = p.second;
      }
    }, &fallbackBuffer);
    updateLock.unlock();
    
    t1.join();
    t2.join();
    
    updateMsg.resize(
        updateMsg.size() + fallbackBuffer.size());  // hopefully, won't trigger a realloc because we allocated much
    memcpy(updateMsg.data() + fallbackOffset, fallbackBuffer.data(), fallbackBuffer.size());
    
    return updateMsg;
  }
  
  bool onMessage(int msgType, int id, const int fd, const string &ip, vector<char> &msg) override {
    if (Node::onMessage(msgType, 0, fd, ip, msg)) return true;
    
    if (msgType == Borrow) {
      uint32_t *p = (uint32_t *) msg.data();
      onBorrowMsg(p[0], p[1], p[2]);
    } else if (msgType == Insert) {
      updateLock.lock();
      
      K k(msg.data());
      // find *nReplicas* suitable locations for k
      Locations locations;
      bool found = false;
      
      auto it = fallback.find(k);
      if ((found = it != fallback.end())) {
        locations = it->second;
      } else if ((found = ludo.lookUp(k, locations))) {
      } else {
        locations = allocate(k, this, 3);
        
        int i = 0;
        if (locations.locs[i++].dId == (uint32_t) - 1 || locations.locs[i++].dId == (uint32_t) - 1 || locations.locs[i++].dId == (uint32_t) - 1) {
          ostringstream oss;
          oss << "Locations for key " << k << " not fully allocated. Allocated: " << to_string(i) << endl;
          oss << "Not found key. Allocation failed. ";
          
        // log(oss.str());
        }
      }
      
    // log((found ? "Found key " : "Not found key ") + k);
      
      if (!found) {
        vector <u_char> updateMsg;
        if (building || (full_debug && rand() > INT_MAX / 4 * 3)) {
          fallback.insert(make_pair(k, locations));
          updateMsg.resize(1 + sizeof(Locations) + k.length() + 1);
          
          *updateMsg.data() = 1;
          memcpy(updateMsg.data() + 1, &locations, sizeof(Locations));
          memcpy(updateMsg.data() + 1 + sizeof(Locations), k.data(), k.length() + 1);
        } else {
          // insert these locations to local Ludo-CP
          UpdateResult result = ludo.insert(k, locations);
          if (result.status == 0 && (!full_debug || ludo.nKeys <= 3)) {
            // handle each path entry. caution with the last entry: it may indicate a rebuild in Othello
            // msg format: <0, locations, <lenOfCC, bid:sid, newSeed, slots. CC> * many>
            // lenOfCC<0 means othello is being reconstructed, so the msg contains the full key at the position of CC
            // -1: first bucket. -2: second bucket
            updateMsg.reserve(1000);
            updateMsg.resize(sizeof(Locations) + 1);
            *updateMsg.data() = 0;
            memcpy(updateMsg.data() + 1, &locations, sizeof(Locations));
            
            for (auto &entry: result.path) {
              uint len = 10 + (entry.status < 0 ? (k.length() + 1) : entry.status * 4);
              uint i = updateMsg.size();
              updateMsg.resize(i + len);
              
              memcpy(updateMsg.data() + i, &entry, 10);
              memcpy(updateMsg.data() + i + 10, entry.locatorCC.data(), entry.status * 4);
            }
          } else {
            if (!full_debug && !(result.status == -1 || result.status == -2))
              throw runtime_error("impossible");
            
            // just update the fallback table in DP. msg format: <1, locations, key>
            updateMsg.resize(1 + sizeof(Locations) + (k.length() + 1));
            *updateMsg.data() = 1;
            memcpy(updateMsg.data() + 1, &locations, sizeof(Locations));
            memcpy(updateMsg.data() + 1 + sizeof(Locations), k.data(), k.length() + 1);
            
            building = true;
            fallback.insert(make_pair(k, locations));
            // if ==1, only rebuild othello.
            // if ==2, rebuild whole ludo
            build = thread([this, result, k, locations]() {
              Clocker c("rebuild");
              // build in the background. in the meantime, the fallback table will handle all updates
              if (result.status == -1 || (full_debug && (rand() & 1))) {
                ludo.locator.build();
              } else {
                ludo.resizeCapacity(ludo.capacity + 1);  // will double
              }
              
              // after necessary rebuilding, insert all buffered kv. halt the update for a while, much shorter than the whole build time
              updateLock.lock();
              
              ludo.Merge(fallback, [](const Locations &locs) -> bool { return locs.locs[0].dId != uint32_t(-1); });
              bool notOnlyOthello = true;  // currently, we do not cope with intact ludo. because that is rare, after merging
              
              vector <u_char> updateMsg = serializeLudo(notOnlyOthello);
              
              building = false;
              
              // make sure sending out the update msg before accepting new updates becasue OOO insertions breaks the integrity of Ludo
              // send the update messages to lookups
              mylock_guard gg(sendLock);
              updateLock.unlock();
              for (int fd: updateChannels) {
                my_write(fd, UpdateOthello + notOnlyOthello, updateMsg);
              }
            });
            build.detach();
          }
        }
        
        appendLog(k, locations);
        
        mylock_guard gg(sendLock);
        updateLock.unlock();
        
        // send the update messages to lookups
        for (int fd: updateChannels) {
          my_write(fd, Insert, updateMsg);
        }
      } else {
      // log("Rested to insert an existing key: " + k + ", first disk storing it: " + to_string(locations.locs[0].dId));
        updateLock.unlock();
      }
      
      int i = 0;
      if (locations.locs[i++].dId == (uint32_t) - 1 || locations.locs[i++].dId == (uint32_t) - 1 || locations.locs[i++].dId == (uint32_t) - 1) {
        ostringstream oss;
        oss << "Locations for key " << k << " not fully allocated. Allocated: " << to_string(i) << endl;
        oss << (found ? "Found key. " : "Not found key. ");
        if (found) {
          oss << "If previous log does not show an allocation failure, then this is a lookup error. If you are debugging, you can follow the lookup again. " << endl;
  
          Locations locations;
          bool found = false;
  
          auto it = fallback.find(k);
          if ((found = it != fallback.end())) {
            locations = it->second;
          } else if ((found = ludo.lookUp(k, locations))) {
          }
        } else {
          oss << "Allocation error should show previously. If you are debugging, you can follow the allocation again. " << endl;
  
          locations = allocate(k, this, 3);
        }
        
      // log(oss.str());
      }
      
    // log("Locations for key " + k + " " + to_string(locations.locs[0].dId));
      
      // send back the locations to client
      my_write(fd, Return, &locations, sizeof(Locations));
    } else if (msgType == Remove) {
      mylock_guard g(updateLock);
      K k(msg.data());
      
      if (building) {
        fallback.insert(make_pair(k, Locations()));
      } else {
        Locations locations;
        if (!locate(k, locations)) return false;
        
        // remove from local Ludo-CP
        UpdateResult result = ludo.remove(k);
        
        // remove from remote Ludo-DPs, very limited work
        if (result.status == 1) {
          mylock_guard gg(sendLock);
          for (int fd: updateChannels) {
            my_write(fd, Remove, k);
          }
        } else {} // fine with the ludo dp
        
        // remove from storage, skipped
        
        // update load records
        for (int i = 0; i < 3; ++i) {
          Location &location = locations.locs[i];
          release(location.dId, location.blkId);
        }
      }
      
      Locations locations;
      locations.locs[0].dId = -1;  // mark as deleted
      appendLog(k, locations);
      
    // log("Locations for key " + k + to_string(locations.locs[0].dId));
      uint8_t succ = true;
      my_write(fd, Return, &succ, 1);
    } else if (msgType == Granted) {
      onGrantedMsg(id, msg);
    } else if (msgType == Leave) {
      // direct the data move. Just for performance test, and do not maintain whole system consistency.
      uint32_t sId = *(uint32_t *) msg.data();
      storages[sId].in = false;
      //leastLoaded.erase(sId);
      
      vector <u_char> updateMsg;
      updateMsg.reserve(500);
      
      mylock_guard g(updateLock);
      for (pair<const K, Locations> &pair:fallback) {
        const K &k = pair.first;
        auto &locs = pair.second.locs;
        int i = 0;
        while (locs[i].dId != sId && i < 3) ++i;
        
        if (i == 3) continue;
        
        set<uint> st = {locs[0].dId, locs[1].dId, locs[2].dId};
        locs[i] = allocateDefaultLeave(k, this, st, 1).locs[0];
        
        int j = (i + 1) % 3;
        // copy to storage node: j to i
        updateMsg.resize(12);
        uint32_t *p = (uint32_t *) updateMsg.data();
        p[0] = locs[j].blkId;
        p[1] = locs[i].dId;
        p[2] = locs[i].blkId;
        my_write(storages[locs[j].dId].addrPort, Copy, updateMsg);
        
        // send the update to lookups
        updateMsg.resize(1 + sizeof(Locations) + k.length() + 1);
        
        *updateMsg.data() = 1;
        memcpy(updateMsg.data() + 1, &locs, sizeof(Locations));
        memcpy(updateMsg.data() + 1 + sizeof(Locations), k.data(), k.length() + 1);
        for (int fd: updateChannels) {
          my_write(fd, Update, updateMsg);
        }
      }
      
      for (uint64_t bid = 0; bid < ludo.num_buckets_; ++bid) {
        auto &b = ludo.buckets_[bid];
        for (int s = 0; s < 4; ++s) {
          if ((b.occupiedMask & (1 << s)) == 0) continue;
          
          const K &k = b.keys[s];
          auto &locs = b.values[s].locs;
          int i = 0;
          while (locs[i].dId != sId && i < 3) ++i;
          
          if (i == 3) continue;
          set<uint> st = {locs[0].dId, locs[1].dId, locs[2].dId};
          locs[i] = allocateDefaultLeave(k, this, st, 1).locs[0];
          
          int j = (i + 1) % 3;
          // copy to storage node: j to i
          updateMsg.resize(12);
          uint32_t *p = (uint32_t *) updateMsg.data();
          p[0] = locs[j].blkId;
          p[1] = locs[i].dId;
          p[2] = locs[i].blkId;
          cout << "Copy from " << locs[j].dId << " to " << p[1] <<":"<< p[2] << endl;
          my_write(storages[locs[j].dId].addrPort, Copy, updateMsg);
          
          // send the update messages to lookups
          updateMsg.resize(1 + 4 + sizeof(Locations));
          
          *updateMsg.data() = 0;
          uint32_t bs = (bid << 2) + s;
          memcpy(updateMsg.data() + 1, &locs, sizeof(Locations));
          memcpy(updateMsg.data() + 1 + sizeof(Locations), &bs, 4);
          for (int fd: updateChannels) {
            my_write(fd, Update, updateMsg);
          }
        }
      }
      
      string tmp = string("Leave done in master ") + to_string(thisId);
      my_write(nameServer, Log, tmp);
    } else if (msgType == DumpKeys) {
      // direct the data move. Just for performance test, and do not maintain whole system consistency.
      uint32_t sId = *(uint32_t *) msg.data();
      storages[sId].in = false;
      
      ofstream f("dump.txt", ios_base::out | ios_base::app);
      f << "-------" << date() << "-------" << endl;
      mylock_guard g(updateLock);
      for (pair<const K, Locations> &pair:fallback) {
        const K &k = pair.first;
        auto &locs = pair.second.locs;
        int i = 0;
        while (locs[i].dId != sId && i < 3) ++i;
        
        if (i == 3) continue;
        f << k << endl;
      }
      
      for (uint64_t bid = 0; bid < ludo.num_buckets_; ++bid) {
        auto &b = ludo.buckets_[bid];
        for (int s = 0; s < 4; ++s) {
          if ((b.occupiedMask & (1 << s)) == 0) continue;
          
          const K &k = b.keys[s];
          auto &locs = b.values[s].locs;
          int i = 0;
          while (locs[i].dId != sId && i < 3) ++i;
          
          if (i == 3) continue;
          
          f << k << endl;
        }
      }
      
      f.close();
    } else if (msgType == Hot) {
      uint *p = (uint * )(msg.data());
      uint did = p[0];
      p++;
      storages[did].in = false;
      
      int n = msg.size() / 4 - 1;
      unordered_map <uint, Location> m;
      for (int i = 0; i < n; ++i) {
        uint32_t blkId = p[i];
        Location location = allocateDefault("1", this, 1).locs[0];  // just a random key is good
        m[blkId] = location;
      }
      
      vector <u_char> updateMsg;
      updateMsg.reserve(500);
      
      mylock_guard g(updateLock);
      for (pair<const K, Locations> &pair:fallback) {
        const K &k = pair.first;
        auto &locs = pair.second.locs;
        int i = 0;
        while (i < 3) {
          if (locs[i].dId == did) {
            auto it = m.find(locs[i].blkId);
            if (it != m.end()) {
              locs[i] = it->second;
              break;
            }
          }
          ++i;
        }
        
        if (i == 3) continue;
        
        int j = (i + 1) % 3;
        // copy to storage node: j to i
        updateMsg.resize(12);
        uint32_t *p = (uint32_t *) updateMsg.data();
        p[0] = locs[j].blkId;
        p[1] = locs[i].dId;
        p[2] = locs[i].blkId;
        my_write(storages[j].addrPort, Copy, updateMsg);
        
        // send the update to lookups
        updateMsg.resize(1 + sizeof(Locations) + k.length() + 1);
        
        *updateMsg.data() = 1;
        memcpy(updateMsg.data() + 1, &locs, sizeof(Locations));
        memcpy(updateMsg.data() + 1 + sizeof(Locations), k.data(), k.length() + 1);
        for (int fd: updateChannels) {
          my_write(fd, Update, updateMsg);
        }
      }
      
      for (uint64_t bid = 0; bid < ludo.num_buckets_; ++bid) {
        auto &b = ludo.buckets_[bid];
        for (int s = 0; s < 4; ++s) {
          if ((b.occupiedMask & (1 << s)) == 0) continue;
          const K &k = b.keys[s];
          auto &locs = b.values[s].locs;
          
          int i = 0;
          while (i < 3) {
            if (locs[i].dId == did) {
              auto it = m.find(locs[i].blkId);
              if (it != m.end()) {
                locs[i] = it->second;
                break;
              }
            }
            ++i;
          }
          
          if (i == 3) continue;
          
          int j = (i + 1) % 3;
          // copy to storage node: j to i
          updateMsg.resize(12);
          uint32_t *p = (uint32_t *) updateMsg.data();
          p[0] = locs[j].blkId;
          p[1] = locs[i].dId;
          p[2] = locs[i].blkId;
          cout << "this addrPort is: " << storages[j].addrPort << endl;
          my_write(storages[j].addrPort, Copy, updateMsg);
          
          // send the update messages to lookups
          updateMsg.resize(1 + 4 + sizeof(Locations));
          
          *updateMsg.data() = 0;
          uint32_t bs = (bid << 2) + s;
          memcpy(updateMsg.data() + 1, &locs, sizeof(Locations));
          memcpy(updateMsg.data() + 1 + sizeof(Locations), &bs, 4);
          for (int fd: updateChannels) {
            my_write(fd, Update, updateMsg);
          }
        }
      }
      
      mylock_guard gg(loadLock);
      for (int i = 0; i < n; ++i) {
        uint blkId = p[i];
        occupied[did][blkId / 256].memSet(blkId % 256, 0);
      }
    } else if (msgType == Size) {
      uint did = *(uint * )(msg.data());
      uint64_t size = 0;
      mylock_guard g(updateLock);
      for (pair<const K, Locations> &pair:fallback) {
        auto &locs = pair.second.locs;
        int i = 0;
        while (i < 3) {
          if (locs[i].dId == did) {
            size++;
          }
          ++i;
        }
      }
      
      for (uint64_t bid = 0; bid < ludo.num_buckets_; ++bid) {
        auto &b = ludo.buckets_[bid];
        for (int s = 0; s < 4; ++s) {
          if ((b.occupiedMask & (1 << s)) == 0) continue;
          const K &k = b.keys[s];
          auto &locs = b.values[s].locs;
          
          int i = 0;
          while (i < 3) {
            if (locs[i].dId == did) {
              size++;
            }
            ++i;
          }
        }
      }
      //size *= 4;  // unit: mega bytes; //Size is the number of blocks
      string tmp = string("Size for storage ") + to_string(did) + ": " + to_string(size)+ " : " << to_string((double)(size * 0.01)) << "MB";
      cout << "# Objects for storage: " << to_string(size) << " : " << to_string((double)(size * 0.01)) << "MB" << endl;
      my_write(nameServer, Log, tmp);
    } else return false;
    
    return true;
  }
  
  bool locate(const K &k, Locations &out) {
    return ludo.lookUp(k, out);
  }
  
  Locations locate(const K &k) {
    Locations out;
    ludo.lookUp(k, out);
    return out;
  }
  
  void occupy(uint dId, uint blkId, bool inHeap = true) {
    mylock_guard g(loadLock);
    occupied[dId][blkId / 256].memSet(blkId % 256U, 1);
    loadInfo[dId].first++;
    
    if (inHeap) leastLoaded.decrease(dId);
  }
  
  void release(uint dId, uint blkId, bool inHeap = true) {
    mylock_guard g(loadLock);
    occupied[dId][blkId / 256].memSet(blkId % 256U, 0);
    loadInfo[dId].first--;
    
    if (inHeap) leastLoaded.increase(dId);
  }
};

// first available block on the least loaded disk. RESERVE before sending!
inline Locations allocateDefault(const K &k, Master *_this, uint8_t count) {
  mylock_guard g(_this->loadLock);
  
  Locations locations;
  
  for (int i = 0; i < count; ++i) {
    uint dId = _this->leastLoaded.top();
    pair <uint, uint> &load = _this->loadInfo[dId]; 
    
    if (!_this->storages[dId].in) continue;
    if (load.first >= load.second) break;
    _this->leastLoaded.pop();
    
    uint start = _this->lastAvailable[dId];
    
    uint64_t bitsCnt = _this->allocated[dId].capacity;
    for (uint cnt = 0; cnt < bitsCnt; ++cnt) {
      uint bulkId = cnt + start;
      if (bulkId > bitsCnt) bulkId -= bitsCnt;
      
      if (!_this->allocated[dId].memGet(bulkId)) continue;
      
      uint blkId = _this->bulkFreeIndex(dId, bulkId);
      if (blkId != uint(-1)) {
        _this->lastAvailable[dId] = bulkId;
        
        blkId = bulkId * 256U + uint32_t(blkId);
        _this->occupy(dId, blkId, false);
        locations.locs[i] = {dId, blkId};
        break;
      }
    }
  }
  
  for (int i = 0; i < count; ++i) {
    uint32_t id = locations.locs[i].dId;
    if (id == uint32_t(-1)) break;  // not found and not deleted
    _this->leastLoaded.push(id);
  }
  
  return locations;
}

inline Locations allocateDefaultLeave(const K &k, Master *_this, set<uint> &st, uint8_t count) {
  mylock_guard g(_this->loadLock);
  
  Locations locations;
  
  for (int i = 0; i < count; ++i) {
    uint dId = _this->leastLoaded.top();
    //pair <uint, uint> &load = _this->loadInfo[dId];
    
    vector<uint> heap_fallback;
    cout << "st have: " << endl;
      for (auto it=st.cbegin(); it != st.cend(); ++it)
        std::cout << ' ' << *it;
    cout << endl;
      
    while(!_this->storages[dId].in || st.count(dId) != 0){
      heap_fallback.push_back(dId);
      cout << dId << "is not OK and is pop out." << endl;
      _this->leastLoaded.pop();
      /*if(_this->leastLoaded.size() < 3){
        perror("The storages number low.");
        debug_break();
      }*/
      cout << "heap 1 have: ";
      for (auto it=_this->leastLoaded.begin(); it != _this->leastLoaded.end(); ++it)
        std::cout << ' ' << *it;
      cout << endl;
      
      dId = _this->leastLoaded.top();
      cout << "The top element now is " << dId << endl;
    }
    cout << "finally we choose " << dId <<"because now st.count(): " << st.count(dId) << endl;
    cout << "push back to heap: " << endl;
    for(const uint &el: heap_fallback){
      cout << " "<< el;
      _this->leastLoaded.push(el);
    }
    cout << endl;
    
    cout << "heap 1 have: ";
    for (auto it=_this->leastLoaded.begin(); it != _this->leastLoaded.end(); ++it)
    std::cout << ' ' << *it;
    cout << endl;
    
    pair <uint, uint> &load = _this->loadInfo[dId];
    
    
    //if (!_this->storages[dId].in) continue;
    if (load.first >= load.second) break;
    //_this->leastLoaded.pop();
    
    uint start = _this->lastAvailable[dId];
    
    uint64_t bitsCnt = _this->allocated[dId].capacity;
    for (uint cnt = 0; cnt < bitsCnt; ++cnt) {
      uint bulkId = cnt + start;
      if (bulkId > bitsCnt) bulkId -= bitsCnt;
      
      if (!_this->allocated[dId].memGet(bulkId)) continue;
      
      uint blkId = _this->bulkFreeIndex(dId, bulkId);
      if (blkId != uint(-1)) {
        _this->lastAvailable[dId] = bulkId;
        
        blkId = bulkId * 256U + uint32_t(blkId);
        _this->occupy(dId, blkId, false);
        locations.locs[i] = {dId, blkId};
        break;
      }
    }
  }
  
  for (int i = 0; i < count; ++i) {
    uint32_t id = locations.locs[i].dId;
    if (id == uint32_t(-1)) break;  // not found and not deleted
    //_this->leastLoaded.push(id);
  }
  
  return locations;
}
