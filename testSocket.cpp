#pragma once

#include "common.h"
#include "Smash/socket_node.h"

typedef string K;
typedef string V;

int main() {
  commonInit();
  
  SocketNode a, b;
  a.name = "a";
  b.name = "b";
  a.initSocket(8080);
  b.initSocket();
  cout.flush();
  
  char messages[][6] = {{1, 2, 3, 4, 5, 6},
                        {6, 5, 4, 3, 2, 1}};
  
  int fd = b.connectToServer("localhost:8080");
  
  b.my_write(fd, 0, messages[0], 6);
  b.my_write(fd, 1, messages[1], 6);
  
  vector<char> longMessage;
  int n = 4096 * 2 + 3;
  longMessage.resize(n);
  
  for (int i = 0; i < n; ++i) {
    int c = i & 255;
    if (c == 0) c = (i / 255) & 255;
    
    longMessage[i] = c;
  }
  
  b.my_write(fd, 2, longMessage.data(), n);
  close(fd);
  
  this_thread::sleep_for(2s);
  
  return 0;
}
