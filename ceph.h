#pragma once

#include <rados/librados.h>
#include "common.h"
#include "Smash/node.h"

class CephClient {
public:
  /* Declare the cluster handle and required arguments. */
  rados_t cluster;
  const char *cluster_name = "ceph";
  const char *user_name = "client.admin";
  uint64_t flags = 0;
  vector<char> buffer;
  const char *argv[1] = {"ceph"};
  const char *poolname = "test";
  int argc = 1;
  rados_ioctx_t io;
  
  CephClient() {
    /* Initialize the cluster handle with the "ceph" cluster name and the "client.admin" user */
    int err;
    err = rados_create2(&cluster, cluster_name, user_name, flags);
    
    if (err < 0) {
      fprintf(stderr, "%s: Couldn't create the cluster handle! %s\n", argv[0], strerror(-err));
      exit(EXIT_FAILURE);
    } else {
      printf("\nCreated a cluster handle.\n");
    }
    
    /* Read a Ceph configuration file to configure the cluster handle. */
    err = rados_conf_read_file(cluster, "/etc/ceph/ceph.conf");
    if (err < 0) {
      fprintf(stderr, "%s: cannot read config file: %s\n", argv[0], strerror(-err));
      exit(EXIT_FAILURE);
    } else {
      printf("\nRead the config file.\n");
    }
    
    /* Read command line arguments */
    err = rados_conf_parse_argv(cluster, argc, argv);
    if (err < 0) {
      fprintf(stderr, "%s: cannot parse command line arguments: %s\n", argv[0], strerror(-err));
      exit(EXIT_FAILURE);
    } else {
      printf("\nRead the command line arguments.\n");
    }
    
    /* Connect to the cluster */
    err = rados_connect(cluster);
    if (err < 0) {
      fprintf(stderr, "%s: cannot connect to cluster: %s\n", argv[0], strerror(-err));
      exit(EXIT_FAILURE);
    } else {
      printf("\nConnected to the cluster.\n");
    }
    
    buffer.resize(blockSize);
    
    err = rados_ioctx_create(cluster, poolname, &io);
    if (err < 0) {
      fprintf(stderr, "%s: cannot open rados pool %s: %s\n", argv[0], poolname, strerror(-err));
      rados_shutdown(cluster);
      exit(1);
    }
  }
  
  ~CephClient() {
    rados_ioctx_destroy(io);
    rados_shutdown(cluster);
  }
  
  void Insert(string &k, char *data) {
    int err = rados_write_full(io, k.c_str(), data, blockSize);

    if (err < 0) {
      fprintf(stderr, "%s: cannot write pool %s: %s\n", argv[0], poolname, strerror(-err));
      rados_ioctx_destroy(io);
      rados_shutdown(cluster);
      exit(1);
    }
  }
  
  void Update(string &k, char *data) {
    Insert(k, data);
  }
  
  vector<char> Read(string &k) {
    int err = rados_read(io, k.c_str(), buffer.data(), blockSize, 0);

    if (err < 0) {
      fprintf(stderr, "%s: cannot read pool %s: %s\n", argv[0], poolname, strerror(-err));
      rados_ioctx_destroy(io);
      rados_shutdown(cluster);
      exit(1);
    }
    return buffer;
  }
  
  void Remove(string &k){
    int err = rados_remove(io, k.c_str());
    if (err < 0) {
      fprintf(stderr, "%s: cannot remove object in pool %s: %s\n", argv[0], poolname, strerror(-err));
      rados_ioctx_destroy(io);
      rados_shutdown(cluster);
      exit(1);
    }
  }
  
};

