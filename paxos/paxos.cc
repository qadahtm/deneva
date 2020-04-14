// Author: Kun Ren (kun.ren@yale.edu)
//         Alexander Thomson (thomson@cs.yale.edu)
// The Paxos object allows batches to be registered with a running zookeeper
// instance, inserting them into a globally consistent batch order.

// The above authors are the original authors of paxos.h
// Original code repository: https://github.com/yaledb/calvin
// This file is modified from the original by Thamir Qadah solely for academic research purposes

#include "paxos/paxos.h"

#include <fstream>
#include <utility>
#include <vector>

#include <config_yaml.h>
#include <cassert>

using std::ifstream;
using std::pair;
using std::vector;

Paxos::Paxos(const string &zookeeper_config_file, bool reader){
    init(zookeeper_config_file, reader, string("/root"), false);
}

Paxos::Paxos(const string &zookeeper_config_file, bool reader, const string &rootPath){
    init(zookeeper_config_file, reader, rootPath, false);
}

Paxos::Paxos(const string &zookeeper_config_file, bool reader, const string &rootPath, bool traceNodeCreation) {
    init(zookeeper_config_file, reader, rootPath, traceNodeCreation);
}
void Paxos::init(const string &zookeeper_config_file, bool reader, const string &rootPath, bool traceNodeCreation) {
    ifstream in(zookeeper_config_file.c_str());
    string s, port, ip, connection_string, timeout;
    // Get the connection string(ip and port) from the config file.

    this->rootPath = rootPath;
    this->batchPathPrefix = (rootPath + string("/batch-"));
    this->traceNodeCreation = traceNodeCreation;

    auto conf = new config_yaml();
    int rc = conf->load( (char *) zookeeper_config_file.c_str());
    if (rc == 1) {
        printf("Loading Zookeeper config failed.\n");
        return;
    }

//    hardcode timeout for now
//TODO(tq): add parsing timeout to config class
    timeout = string("800000");

    ip = conf->zk_nodes->at(0);
    port = conf->zk_ports->at(0);

    connection_string = ip + ":" + port;

    next_read_batch_index_ = 0;

    // Init the mutexes.
    for (uint64 i = 0; i < CONCURRENT_GETS; i++) {
        pthread_mutex_init(&(mutexes_[i]), NULL);
    }

    // Connect to the zookeeper.
    zh_ = zookeeper_init(connection_string.c_str(), NULL,
                         atoi(timeout.c_str()), 0, NULL, 0);
    if (zh_ == NULL) {
        printf("Connection to zookeeper failed.\n");
        return;
    }

    createRootZNode();

    if (reader) {
        for (uint64 i = 0; i < CONCURRENT_GETS; i++) {
            char current_read_batch_path[READ_PATH_MAX_LEN];
            snprintf(current_read_batch_path, sizeof(current_read_batch_path),
                     "%s%010lu", batchPathPrefix.c_str(), i);
            int get_rc = zoo_aget(zh_, current_read_batch_path, 0,
                                  get_data_completion,
                                  reinterpret_cast<void *>(
                                          new std::pair<uint64, Paxos *>(i, this)));
            if (get_rc) {
                printf("Have exited the Paxos thread, exit number is %d.\n", get_rc);
            }
        }
    }

}

void Paxos::createRootZNode() const {// Verify that whether the root node have been created,
// if not, create the root node.
    printf("Creating ZNode %s\n", this->rootPath.c_str());
    int rc = zoo_exists(zh_, this->rootPath.c_str(), 0, NULL);
    if (rc == ZNONODE) {
        printf("ZNode: %s\n does not exist \n", this->rootPath.c_str());
        // If multiple nodes executing this code to both see that
        // the root doesn't exist, only one node creates /root
        // node actually, the others return ZNODEEXISTS.
        int create_rc = zoo_create(zh_, this->rootPath.c_str(), NULL, 0,
                                   &ZOO_OPEN_ACL_UNSAFE,
                                   0, NULL, 0);
        if (create_rc != ZOK && create_rc != ZNODEEXISTS) {
            printf("zoo_create  error:error number is %d\n", create_rc);
        }
    }
}

Paxos::~Paxos() {
    // Destroy the mutexes.
    for (uint i = 0; i < CONCURRENT_GETS; i++) {
        pthread_mutex_destroy(&(mutexes_[i]));
    }
    // Close the connection with the zookeeper.
    int rc = zookeeper_close(zh_);
    if (rc != ZOK) {
        printf("zookeeper_close error:error number is %d\n", rc);
    }
}

void Paxos::SubmitBatch(const string &batch_data) {
    // Submit batch means that create new znode below the root directory.
    int rc = zoo_acreate(zh_, batchPathPrefix.c_str(), batch_data.c_str(),
                         batch_data.size(), &ZOO_OPEN_ACL_UNSAFE,
                         ZOO_SEQUENCE | ZOO_EPHEMERAL,
                         acreate_completion, NULL);
    if (rc != ZOK) {
        printf("zoo_acreate error:error number is %d\n", rc);
    }
}

bool Paxos::GetNextBatch(string *batch_data) {
    int next_batch_thread = next_read_batch_index_ % CONCURRENT_GETS;
    // If there have been some batches stored in the corresponding batch_table,
    // read from that and return true, else return false.
    if (batch_tables_[next_batch_thread].size() > 0) {
        // Lock the batch table.
        pthread_mutex_lock(&(mutexes_[next_batch_thread]));
        (*batch_data) = batch_tables_[next_batch_thread][next_read_batch_index_];
        batch_tables_[next_batch_thread].erase(next_read_batch_index_);
        // Unlock the batch table.
        pthread_mutex_unlock(&(mutexes_[next_batch_thread]));
        next_read_batch_index_++;
        return true;
    } else {
        return false;
    }
}

void Paxos::GetNextBatchBlocking(string *batch_data) {
    while (!GetNextBatch(batch_data)) {
    }
}

void Paxos::get_data_completion(int rc, const char *value, int value_len,
                                const struct Stat *stat, const void *data) {
    // XXX(scw): using const_cast is disgusting
    pair<uint64, Paxos *> *previous_data =
            reinterpret_cast<pair<uint64, Paxos *> *>(const_cast<void *>(data));
    uint64 previous_index_for_aget = previous_data->first;
    Paxos *paxos = previous_data->second;
    string batch_data(value, value_len);
    uint64 next_index_for_aget;
    // If zoo_aget function completed successfully, insert the batch into the
    // corresponding batch_tables_.
    if (rc == ZOK) {
        // Set the number of batch which will be got from zookeeper next time
        // (just plus the CONCURRENT_GETS).
        next_index_for_aget = previous_index_for_aget + CONCURRENT_GETS;
        pthread_mutex_lock(&paxos->mutexes_[previous_index_for_aget %
                                            CONCURRENT_GETS]);
        paxos->batch_tables_[previous_index_for_aget % CONCURRENT_GETS]
        [previous_index_for_aget] = batch_data;
        pthread_mutex_unlock(&paxos->mutexes_[previous_index_for_aget %
                                              CONCURRENT_GETS]);
        // If there are no new batch in the zookeeper, just wait for a while
        // and continue to get from zookeeper.
    } else if (rc == ZNONODE) {
        next_index_for_aget = previous_index_for_aget;
        usleep(0.2 * 1000);
    } else {
        return;
    }
    // Continue to get a batch from zookeeper.
    char current_read_batch_path[READ_PATH_MAX_LEN];
    snprintf(current_read_batch_path, sizeof(current_read_batch_path),
             "%s%010lu", paxos->batchPathPrefix.c_str(), next_index_for_aget);
    previous_data->first = next_index_for_aget;
    int get_rc = zoo_aget(paxos->zh_, current_read_batch_path, 0,
                          get_data_completion,
                          reinterpret_cast<const void *>(previous_data));
    if (get_rc) {
        return;
    }
}

void Paxos::acreate_completion(int rc, const char *name, const void *data) {
    if (rc) {
        printf("Error %d for zoo_acreate.\n", rc);
    }
}

void Paxos::cleanUpRoot() {
    // get root children
    int rc;
    String_vector *paths = (String_vector *) malloc(sizeof(struct String_vector));

    rc = zoo_get_children(this->zh_, rootPath.c_str(), 0, paths);
    if (rc != ZOK && paths != nullptr){
        printf("Error: Failed to get Root node children\n");
        return;
    }

    for (int i = 0; i < paths->count; i++){
        auto path = this->rootPath + "/" + string(paths->data[i]);
        rc = zoo_delete(this->zh_, path.c_str(), -1);
        if (rc != ZOK) {
            printf("Error: Failed to delete child node\n");
        }
    }
    free(paths);

    // using -1 below to skip version checks
    rc = zoo_delete(this->zh_, rootPath.c_str(), -1);
    if (rc != ZOK){
        printf("Error: Failed to delete Root node\n");
    }
}

// This function will automatically start zookeeper server based on the
// zookeeper config file(generate ssh commands and execute them).
void StartZookeeper(const string &zookeeper_config_file) {
    printf("StopZookeeper function is not supported\n");
    assert(false);
    vector<string> zookeepers;
    string line;
    // Read zookeeper config file.
    ifstream in(zookeeper_config_file.c_str());
    // Put all zookeeper server's ip into the vector.
    while (getline(in, line)) {
        if (line.substr(0, 6) == "server") {
            int pos1 = line.find('=');
            int pos2 = line.find('\0');
            zookeepers.push_back(line.substr(pos1 + 1, pos2 - pos1 - 1));
        }
    }
    for (unsigned int i = 0; i < zookeepers.size(); i++) {
        // Generate the ssh command.
        string ssh_command = "ssh " + zookeepers[i] +
                             " /tmp/kr358/zookeeper/zookeeper-3.3.3/" +
                             "bin/zkServer.sh start > zookeeper_log &";
        // Run the ssh command.
        system(ssh_command.c_str());
    }
    printf("Starting zookeeper servers.\n");
    sleep(8);
}

// This function will automatically stop zookeeper server based on the
// zookeeper config file(generate ssh commands and execute them).
void StopZookeeper(const string &zookeeper_config_file) {
    printf("StopZookeeper function is not supported\n");
    assert(false);
    vector<string> zookeepers;
    string line, port, ssh_command;
    // Read zookeeper config file.
    ifstream in(zookeeper_config_file.c_str());
    // Put all zookeeper server's ip into the vector.
    while (getline(in, line)) {
        if (line.substr(0, 6) == "server") {
            int pos1 = line.find('=');
            int pos2 = line.find('\0');
            zookeepers.push_back(line.substr(pos1 + 1, pos2 - pos1 - 1));
        }
        if (line.substr(0, 10) == "clientPort") {
            int pos1 = line.find('=');
            int pos2 = line.find('\0');
            port = line.substr(pos1 + 1, pos2 - pos1 - 1);
        }
    }
    //TODO(tq): fix this to be more generic
    ssh_command = "ssh " + zookeepers[0] +
                  " /tmp/kr358/zookeeper/zookeeper-3.3.3/bin/zkCli.sh -server "
                  + zookeepers[0] + ":" + port + " delete /root > zookeeper_log";
    system(ssh_command.c_str());
    sleep(2);
    for (unsigned int i = 0; i < zookeepers.size(); i++) {
        // Generate the ssh command.
        ssh_command = "ssh " + zookeepers[i] + " /tmp/kr358/zookeeper/"
                      + "zookeeper-3.3.3/bin/zkServer.sh stop > zookeeper_log &";
        system(ssh_command.c_str());
    }
}

