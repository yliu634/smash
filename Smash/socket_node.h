#pragma once

#include "common.h"
#include "node.h"

extern const char **NodeTypeNames;
extern const char **MessageTypeNames;

enum NodeTypes {
  MasterNode,
  LookupNode,
  StorageNode,
  NameNode = -1
};

enum MessageTypes {
  MasterRegister = MasterNode, // master √√ to name server √√   // 0
  LookupRegister = LookupNode, // lookup √√ to name server √√       || format: <"1" (for filter)> // 1
  StorageRegister = StorageNode, // storage√√  to name server √√    || format: <capacity (in blocks)>// 2
  RegisterReply,       // name server √√ to master √√ / lookup √√ / storage √√ // 3
  
  MasterRegisterInfo,  // name server √√ to master √√ / lookup √√ / storage √√ / client √√ // 4
  LookupRegisterInfo,  // name server √√ to master √√ / lookup √√ / storage √√ / client √√ // 5
  StorageRegisterInfo, // name server √√ to master √√ / lookup √√ / storage √√ / client √√ // 6
  
  PullRegisterInfo,    // client √√ to name server √√  // 7
  
  LoadInfo, // master √√ to name server √√ // 8
  
  Borrow,   // name server √√ to master √√         || format: <busyMasterId, dId, nBulks> // 9
  Granted,  // master √√ to master √√ / name server √√  || format: <busyMasterId, dId, bitmap>   // 10
  
  // client √√ to master √√    || forth: <k(string)>   back: <*nReplica* locations>
  // master √√ to lookup √√
  // client √√ to storage √√  || forth: <*nReplica* locations, content of 4MB>  back: <true/false>
  Insert,                                                                         // 11
  Remove,   // client √√  to master √√   //  [[[unnecessary]]] master to lookup   // 12
  Copy,     // any √ to storage √  || format: source blkId, dest sId, dest blkId   // 13
  Move,     // any √ to storage √  || format: source blkId, dest sId, dest blkId   // 14
  // master invoke/receive the data movement, and the location is updated in ludo.
  // master send the update message to the lookup
  Update,   // master √ to lookup √  || format: mode (ludo or fallback), locations, key/<bid, sid>   // 15
  
  UpdateOthello,   // [[[unnecessary]]] master to lookup   // 16
  UpdateLudo,   // master  √√ to lookup  √√                // 17
  Return,  // general packet for returning some data back to the caller   // 18
  
  // client √√ to lookup √√   || format: <seq(u32), port(u32), k(string)>
  // lookup √√ to storage √√  || format: <seq(u32), blkId (u32), client_addr(string)>
  Read,   // 19
  Locate, // 20  client √√ to lookup √√ || forth: <k(string)>    back: <*nReplica* locations>
  
  Leave, // name server √ to master √   // 21
  
  Hot, // storage √ to name server √  | format: list of hot blks  // 22
  // name server x to master x   | format: <dId, list of hot blks>
  // from hottest to coldest (already very hot)
  
  Size,  // any to master. the master will sent log to name server with the size in MiB  // 23
  DumpKeys, // any to master. format: dId  // 24
  Log, // any to name server. format: string.  // 25
  
  ReadReply // storage √√ to client √√     || format: <seq as type, block 4MiB>   // 26
};

class SocketNode {
public:
  string name;
  uint thisId = -1;
  int server_fd = -1;
  uint16_t port = -1;
  thread listeningThread;
  
  explicit SocketNode(string name = "", uint16_t port = 0) : name(std::move(name)) {
    this->port = initSocket(port);
  }
  
  recursive_mutex lock;
  
  // return if the msg is handled by some super classes
  virtual bool onMessage(int msgType, int id, const int fd, const string &ip, vector<char> &msg) { return false; }
  
  uint16_t initSocket(uint16_t port = 0) {
    sockaddr_in address;
    socklen_t addrlen = sizeof(address);
    int opt = 1;
    
    // Creating socket file descriptor
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
      error("socket failed");
    }
    
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
      error("setsockopt failed");
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);  // 0 will let the system allocate one
    
    if (bind(server_fd, (struct sockaddr *) &address, sizeof(address)) < 0) {
      error("bind failed");
    }
    
    if (getsockname(server_fd, (struct sockaddr *) &address, &addrlen) != -1) {
      port = ntohs(address.sin_port);
      this->port = port;
      printf("listening at port %d\n", port);
    } else {
      error("impossible");
    }
    
    if (listen(server_fd, 20) < 0) {
      error("listen failed");
    }
    
    listeningThread = thread([this, address, addrlen]() {
      prctl(PR_SET_NAME, (name + " server").c_str(), 0, 0, 0);
      
      int new_socket;
      while (true) {
        sockaddr_in clientAddr;
        socklen_t addrlen = sizeof(clientAddr);
        if ((new_socket = accept(server_fd, (struct sockaddr *) &clientAddr, &addrlen)) < 0) {
          error("accept failed");
        }
        
        auto serveThread = thread([this, new_socket, clientAddr]() {
          lock.lock();  // inet_ntoa is thread unsafe
          string addr(inet_ntoa(clientAddr.sin_addr));
          lock.unlock();
          
          ostringstream oss;
          oss << name << " serving for: " << addr << ". fd: " << new_socket;
          if (full_debug) Clocker clocker(oss.str());
          prctl(PR_SET_NAME, oss.str().c_str(), 0, 0, 0);
          
          while (true) {
            auto tuple = my_read(new_socket);
            int type = get<0>(tuple);
            int id = get<1>(tuple);
            vector<char> wholeMessage = get<2>(tuple);

            // if (full_debug)
            // cout << name << " received [" << type << "]: " << toHex(wholeMessage.data(), wholeMessage.size()) << endl;
            
            if (wholeMessage.empty()) break;
            
            onMessage(type, id, new_socket, addr, wholeMessage);
          }
          
          close(new_socket);
        });
        
        serveThread.detach();
      }
    });
    
    listeningThread.detach();
    
    return port;
  }
  
  int connectToServer(string host) {
    int index = host.find_last_of(':');
    host[index] = 0;
    
    return connectToServer(host.c_str(), atoi(host.c_str() + index + 1));
  }
  
  void error(string msg) {
  // log(string("ERROR: ") + msg);
    perror(msg.c_str());
    exit(1);
  }
  
  int connectToServer(const char *host, uint16_t portno) {
//// log(string("connecting to server: ") + host);
    
    int sockfd, n;
    struct sockaddr_in serv_addr;
    struct hostent *server;
    
    char buffer[256];
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
      error("ERROR opening socket");
    server = gethostbyname(host);
    if (server == NULL) {
      error("ERROR, no such host: " + string(host));
    }
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *) server->h_addr,
          (char *) &serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(portno);
    if (connect(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0)
      error("ERROR connecting host: " + string(host));

//// log(string("connected to server: ") + host);
    return sockfd;
  }
  
  int raw_write(int fd, void *data, uint length) {
    int bytes_left = length;
    char *ptr = static_cast<char *>(data);
    while (bytes_left > 0) {
      int written_bytes = write(fd, ptr, bytes_left);
      if (written_bytes <= 0) {
        return (-1);
      }
      bytes_left -= written_bytes;
      ptr += written_bytes;
    }
    return (0);
  }
  
  vector<char> raw_read(int fd, uint length) {
    vector<char> result;
    
    do {
      result.resize(length, 0);
      
      uint bytes_left = length;
      char *ptr = result.data();
      
      while (bytes_left > 0) {
        int bytes_read = read(fd, ptr, bytes_left);
        if (bytes_read <= 0) {
        // log("Not enough data received. Bytes left: " + to_string(bytes_left));
          break;
        }
        bytes_left -= bytes_read;
        ptr += bytes_read;
      }
    } while (false);
    
    return result;
  }
  
  int my_write(const string &addrPort, uint type, int id, const vector<char> &data) {
    return my_write(addrPort, type, id, data.data(), data.size());
  }
  
  int my_write(const string &addrPort, uint type, int id, const vector <u_char> &data) {
    return my_write(addrPort, type, id, data.data(), data.size());
  }
  
  int my_write(const string &addrPort, uint type, int id, const string &data) {
    return my_write(addrPort, type, id, data.data(), data.length() + 1);
  }
  
  int my_write(const string &addrPort, uint32_t type, int id, const void *data, uint32_t length) {
    int fd = connectToServer(addrPort);
    int result = my_write(fd, type, id, data, length);
    close(fd);
    
    return result;
  }
  
  int my_write(int fd, uint type, int id, const vector<char> &data) {
    return my_write(fd, type, id, data.data(), data.size());
  }
  
  int my_write(int fd, uint type, int id, const vector <u_char> &data) {
    return my_write(fd, type, id, data.data(), data.size());
  }
  
  int my_write(int fd, uint type, int id, const string &data) {
    return my_write(fd, type, id, data.data(), data.length() + 1);
  }
  
  static string date() {
    time_t seconds;
    long milliseconds;
    char timestring[32];
    
    char timebuffer[32] = {0};
    struct timeval tv = {0};
    struct tm *tmval = NULL;
    struct tm gmtval = {0};
    struct timespec curtime = {0};
    
    int i = 0;
    
    // Get current time
    clock_gettime(CLOCK_REALTIME, &curtime);
    
    // Set the fields
    seconds = curtime.tv_sec;
    milliseconds = round(curtime.tv_nsec / 1.0e6);
    
    if ((tmval = gmtime_r(&seconds, &gmtval)) != NULL) {
      // Build the first part of the time
      strftime(timebuffer, sizeof timebuffer, "%Y-%m-%d %H:%M:%S", &gmtval);
      
      // Add the milliseconds part and build the time string
      snprintf(timestring, sizeof timestring, "%s.%03ld", timebuffer, milliseconds);
    }
    
    return timestring;
  }
  
  bool silent;
  void log(const string &s) const {
    if(silent) return;
    
    cout << date() << ": " << s << endl;
    ofstream f("dist/log.txt", ios_base::out | ios_base::app);
    f << date() << ": " << s << endl << endl;
    f.close();
  }
  
  // format: 4B type, 4B length, and then the msg under that length
  int my_write(int fd, uint32_t type, int id, const void *data, uint32_t length) {
    ostringstream oss;
    oss << "Send message type: " << MessageTypeNames[min(type, (uint) ReadReply)] << ", as " << name;
  // log(oss.str());
    
    int written_bytes = write(fd, &type, 4);
    if (written_bytes <= 0) return (-1);
    
    written_bytes = write(fd, &id, 4);
    if (written_bytes <= 0) return (-1);
    
    written_bytes = write(fd, &length, 4);
    if (written_bytes <= 0) return (-1);
    
    int bytes_left = length;
    const char *ptr = (const char *) data;
    while (bytes_left > 0) {
      written_bytes = write(fd, ptr, bytes_left);
      if (written_bytes <= 0) return (-1);
      
      bytes_left -= written_bytes;
      ptr += written_bytes;
    }
    return (0);
  }
  
  // format
  tuple<uint, int, vector<char>> my_read(int fd) {
    uint type = -1;
    int id = -1;
    vector<char> result;
    
    do {
      uint length = 0;
      int bytes_read = read(fd, &type, 4);
      if (bytes_read <= 0) break;
      bytes_read = read(fd, &id, 4);
      if (bytes_read <= 0) break;
      bytes_read = read(fd, &length, 4);
      if (bytes_read <= 0) break;
      
      result.resize(length, 0);
      
      uint bytes_left = length;
      char *ptr = result.data();
      
      while (bytes_left > 0) {
        bytes_read = read(fd, ptr, bytes_left);
        if (bytes_read <= 0) {
        // log("Not enough data received. Bytes left: " + to_string(bytes_left));
          break;
        }
        bytes_left -= bytes_read;
        ptr += bytes_read;
      }
    } while (false);
    
    return make_tuple(type, id, result);
  }
};
