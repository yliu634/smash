#pragma once

#include "node.h"
#include "../utils/CompactArray.h"

// all nodes register here and thus they get the full connection information.
// to ensure the correct view, change config, and all nodes connects to
class NameServer : public Node {
public:
  NameServer() : Node("NS", 9999) {}
  
  bool ready = false;
  recursive_mutex borrowLock;
  
  bool onMessage(int msgType, int id, const int fd, const string &ip, vector<char> &msg) override {
    if (msgType == Log) {
      string s(msg.begin(), msg.end());
      ostringstream oss;
      oss << " Log request from #" << id << " @ " << ip << ". Log content: " << s;
      log(oss.str());
    } else if (msgType == Leave) {
      uint32_t sId = *(uint32_t *) msg.data();
      for (auto &m: masters) {
        my_write(m.addrPort, Leave, &sId, sizeof(sId));
      }
    } else if (msgType == Hot) {
      // check the blocks and inform related master
      // halt bulk borrowing for a while
      mylock_guard g(borrowLock);
      unordered_map<uint, vector<uint>> tmp;
      
      int size = msg.size() / 4;
      uint *p = (uint *) msg.data();
      
      for (int i = 0; i < size; ++i) {
        uint blkId = p[i];
        uint bulkId = blkId / 256;
        
        uint masterId = allocated[id][bulkId];
        if (tmp.find(masterId) == tmp.end()) {
          tmp[masterId] = {(uint) id};
        }
        tmp[masterId].push_back(blkId);
      }//这个是在将不同的blkId分配给不同的master.
      
      for (pair<const uint, vector<uint>> pair:tmp) {
        my_write(masters[pair.first].addrPort, Hot, pair.second.data(), pair.second.size() / 4);
      }
    } else {
      uint16_t port = *(uint16_t *) msg.data();  // only if regestering
      string addrPort = ip + ":" + to_string(port); // only if regestering
      if (addrPort.length() >= MAX_ADD_LEN) return false;  // equal is already not good
      
      bool added = true;
      if (masters.size() < nMasters && msgType == MasterRegister) {
        id = masters.size();
        MasterNodeInfo info; info.in = true;
        memccpy(info.addrPort, addrPort.data(), 0, MAX_ADD_LEN);
        masters.push_back(info);
        std::cout << "# masters is: " << masters.size() << std::endl;
      } else if (lookups.size() < nLookups && msgType == LookupRegister) {
        id = lookups.size();
        LookupNodeInfo info; info.in = true;
        memccpy(info.addrPort, addrPort.data(), 0, MAX_ADD_LEN);
        info.supportFilter = msg[2] == '1';
        lookups.emplace_back(info);
        std::cout << "# lookups is: " << lookups.size() << std::endl;
      } else if (storages.size() < nStorages && msgType == StorageRegister) {
        id = storages.size();
        StorageNodeInfo info; info.in = true;
        memccpy(info.addrPort, addrPort.data(), 0, MAX_ADD_LEN);
        info.capacity = *(uint32_t *) (msg.data() + 2);
        storages.emplace_back(info);
        std::cout << "# storages is: " << storages.size() << std::endl;
      } else {
        added = false;
      }
      
      if (added) {
        sendRegisterReply(addrPort, id);
        if (masters.size() < nMasters || lookups.size() < nLookups || storages.size() < nStorages) {}
        else {
          ready = true;
          startRoutine();
          broadcastRegisterInfo();
          std:: cout << "All here" << std::endl;
        }
      } else if (msgType == PullRegisterInfo) {
        if (ready) inform(addrPort);
      } else {
        if (msgType == LoadInfo) {
          onLoad(id, msg);
        } else if (msgType == Granted) {
          onGranted(id, msg);
        } else {
          return false;  // all unsupported msgTypes falls to here
        }
      }
    }
    return true;
  }
  
  void sendRegisterReply(const string &addrPort, int id) {
    my_write(addrPort, RegisterReply, to_string(id));
  }
  
  void broadcastRegisterInfo() {
    vector<string> nodes;
    nodes.reserve(nMasters + nLookups + nStorages);
    
    for (int i = 0; i < nMasters; ++i) nodes.emplace_back(masters[i].addrPort);
    for (int i = 0; i < nLookups; ++i) nodes.emplace_back(lookups[i].addrPort);
    for (int i = 0; i < nStorages; ++i) nodes.emplace_back(storages[i].addrPort);
    
    for (string &node : nodes) {
      inform(node);
    }
  }
  
  void inform(int fd) {
    // just send. the receiver organize the addr&ports
    my_write(fd, MasterRegisterInfo, masters.data(), nMasters * sizeof(MasterNodeInfo));
    my_write(fd, LookupRegisterInfo, lookups.data(), nLookups * sizeof(LookupNodeInfo));
    my_write(fd, StorageRegisterInfo, storages.data(), nStorages * sizeof(StorageNodeInfo));
  }
  
  void inform(const string &node) {
    int fd = connectToServer(node);
    inform(fd);
    close(fd);
  }
  
  vector<vector<uint8_t>> allocated;   // [disk #] [bulk #] -> value: master #
  
  // loads are not protected because it is not a critical information, and eventually will be correct
  vector<CompactArray<4>> loads;       // [master #] [disk #] -> 0~15
  
  void startRoutine() override {
    Node::startRoutine();
    
    for (auto &info: storages) {
      uint64_t nBulks = (info.capacity + 255) / 256;   // 1GiB bulk
      vector<uint8_t> cur;
      cur.resize(nBulks);
      
      for (int i = 0; i < nBulks; ++i) {
        cur[i] = i * nMasters / nBulks;
      }
      allocated.emplace_back(cur);
    }
    
    loads.reserve(nMasters);
    for (int i = 0; i < nMasters; ++i) {
      loads.emplace_back(nStorages);
    }
    
    daemon = thread([this] {
      prctl(PR_SET_NAME, (name + " daemon").c_str(), 0, 0, 0);
      vector<uint> _masters(nMasters);
      for (int i = 0; i < nMasters; ++i) {
        _masters[i] = i;
      }
      
      while (alive) {
        for (uint dId = 0; dId < storages.size(); ++dId) {
          mylock_guard g(borrowLock);
          // wait for a random time, bounded [,]
          uint r = 10000 + rand() % 5000;
          if (full_debug) {
            r = 5;
          }
          
          std::chrono::seconds t(r);
          this_thread::sleep_for(t);
          
          // for each master, find busy disks, and try to borrow some bulks from other masters that are free on the bulks
          // the free master grants some bulks to the busy master, and it sends the bitmap to both
          // the name server and the busy master
          vector<uint> mastersSorted(_masters);
          
          std::sort(mastersSorted.begin(), mastersSorted.end(), [this, dId](uint mId1, uint mId2) {
            return loads[mId1].memGet(dId) < loads[mId2].memGet(dId);
          });
          
          uint half = (nMasters + !!full_debug) / 2;
          
          for (int i = 0; i < half; ++i) {
            uint busyMaster = mastersSorted[mastersSorted.size() - 1 - i];
            uint freeMaster = mastersSorted[i];
            uint highLoad = loads[busyMaster].memGet(dId);
            uint lowLoad = loads[freeMaster].memGet(dId);
            
            if (!full_debug && highLoad < lowLoad + 2) break;
            
            uint64_t toBorrow = max(1U, storages[dId].capacity / 256 * (highLoad - lowLoad) / 2 / 16);
            vector<char> msg;
            msg.resize(12);
            uint32_t *p = (uint32_t *) msg.data();
            p[0] = busyMaster;
            p[1] = dId;
            p[2] = toBorrow;
            
            my_write(masters[freeMaster].addrPort, Borrow, msg);
          }
        }
      }
    });
    daemon.detach();
  }
  
  // transfer *allocated* (with lock), but hard to update *loads*
  void onGranted(uint32_t fromMasterId, vector<char> &msg) {
    uint32_t *p = (uint32_t *) msg.data();
    uint32_t toMasterId = p[0];
    uint32_t dId = p[1];
    uint64_t nBulks = (storages[dId].capacity + 255) / 256;
    
    CompactArray<1> bitmap(nBulks, p + 2);
    
    for (uint64_t bulkId = 0; bulkId < nBulks; ++bulkId) {
      if (!bitmap.memGet(bulkId)) continue;
      //      if (allocated[dId][bulkId] == fromMasterId)  // we assume all nodes are honest
      allocated[dId][bulkId] = toMasterId;
    }
  }
  
  void onLoad(uint32_t mId, vector<char> &msg) {
    memcpy(loads[mId].getMem(), msg.data(), (loads[mId].capacity + 1) / 2);
  }
  
  // daemon thread periodically:
  // Gather info on free and busy disks
  // Broadcast its current free disks to other masters
  // Skip a free disk, if received >3 same free
  // For each busy, request bits from masters recently said free
  thread daemon;
};

