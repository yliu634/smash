#pragma once

#include "node.h"

class Commander : public Node {
public:
  Client c;
  vector<K> keys;
  vector<char> buffer;  // for sending value
  uint inserted = 0;
  
  vector<K> hotKeys;
  
  Commander(string name = "Commander", uint16_t port = 0) : Node(name, port) {
    buffer.resize(blockSize);
    my_write(nameServer, PullRegisterInfo, &(this->port), 2);
  }
  
  void loadKeys(uint32_t size) {
    cout << "loading keys" << endl;
    string key;  // all commands:
    ifstream f("dist/keys.txt");
    int i = 0;
    while (!f.eof() && keys.size() < size) {
      getline(f, key);
      i++;
      if (i <= keys.size()) continue;
      keys.push_back(key);
    }
    cout << "loaded" << endl;
  }
  
  void loadHotKeys() {
    cout << "loading hotKeys" << endl;
    string key;  // all commands:
    ifstream f("dist/hotKeys.txt");
    int i = 0;
    while (!f.eof()) {
      getline(f, key);
      i++;
      if (i <= hotKeys.size()) continue;
      hotKeys.push_back(key);
    }
    cout << "loaded" << endl;
  }
  
  void startRoutine() override {
    while (true) {
      string command;  // all commands: 
      getline(cin, command);
      
      string id = command.substr(2);
      uint arg = atoi(id.c_str());
      
      if (command[0] == 'H') {// hot
        my_write(storages[arg].addrPort, Hot, &port, 2);
      } else if (command[0] == 'L') {// leave
        my_write(nameServer, Leave, &arg, 4);
      } else if (command[0] == 'S') {// size
        for (auto &m:masters) {
          my_write(m.addrPort, Size, &arg, 4);
        }
      } else if (command[0] == 'K') {// size
        for (auto &m:masters) {
          my_write(m.addrPort, DumpKeys, &arg, 4);
        }
      } else if (command[0] == 'I') {
        loadKeys(arg);
        
        for (int i = 0; i < arg; ++i) {
          sprintf((char *) buffer.data(), "%s#%d...", keys[i].c_str(), i);
          c.Insert(keys[i], buffer.data());
        }
        inserted = arg;
      } else if (command[0] == 'R') {
        loadKeys(arg);
        if (inserted - arg < 0) {
          cerr << "cannot delete more than # of inserted keys: " << inserted << endl;
        }
        for (int i = inserted - arg; i < inserted; ++i) {
          c.Remove(keys[i]);
        }
      } else if (command[0] == 'U') {
        loadKeys(arg);
        if (inserted - arg < 0) {
          cerr << "cannot update more than # of inserted keys: " << inserted << endl;
        }
        for (int i = inserted - arg; i < inserted; ++i) {
          sprintf((char *) buffer.data(), "%s#%d...", keys[i].c_str(), i);
          c.Update(keys[i], buffer.data());
        }
      } else if (command[0] == 'D' || command[0] == 'Z') { // dynamic imbalance
        loadHotKeys();
        
        double ratio = arg / 100.0;
        InputBase::distribution = (command[0] == 'Z') ? zipfian : uniform;
        InputBase::bound = hotKeys.size();
        
        for (int i = 0; i < 5000; ++i) {
          K k;
          if ((double) rand() / RAND_MAX < ratio) {
            k = hotKeys[InputBase::rand()];
          } else {
            k = keys[rand() % keys.size()];
          }
        }
      }
      
      cout << "command sent" << endl;
    }
  }
};