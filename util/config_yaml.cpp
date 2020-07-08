//
// Created by Thamir Qadah on 4/7/20.
//

#include "config_yaml.h"
#include <yaml.h>
#include <cstdio>
#include <cassert>

#define RETURN_IF_RC_EQ_ERROR if (rc == RC_ERROR){ \
return rc; \
}

#define MOVE_TO_NEXT_EVENT rc = nextEvent(); RETURN_IF_RC_EQ_ERROR

config_yaml::config_yaml() {
    servers = new std::vector<std::string>();
    replicas = new std::vector<std::vector<size_t>>();

    clients = new std::vector<std::string>();

    zk_nodes = new std::vector<std::string>();
    zk_ports = new std::vector<std::string>();

}

config_yaml::~config_yaml() {
    clear();
    assert(servers->size() == replicas->size());
    delete(servers);
    delete(replicas);

    delete(clients);

    assert(zk_nodes->size() == zk_ports->size());
    delete zk_nodes;
    delete zk_ports;
}

int config_yaml::load(std::string input) {

    int error = 0;
    printf("Loading YAML file: %s\n", input.c_str());

    // Using yaml-cpp
    site_deploy = YAML::LoadAllFromFile(std::string(input))[0]; // file has only one document

    std::string group = "servers";
    for (auto it = site_deploy[group].begin(); it != site_deploy[group].end() ; ++it) {
        auto address = (*it)["address"].as<std::string>();
        servers->push_back(address);
        auto yaml_replicas = (*it)["replicas"];
        for (auto r = yaml_replicas.begin(); r != yaml_replicas.end(); ++r){
            auto repIP = (*r).as<size_t>();
            if (replicas->size() < servers->size()){
                std::vector<size_t> repList;
                repList.push_back(repIP);
                replicas->push_back(repList);
            }
            else{
                replicas->back().push_back(repIP);
            }
        }
    }

    group = "clients";
    for (auto it = site_deploy[group].begin(); it != site_deploy[group].end() ; ++it) {
        auto address = (*it).as<std::string>();
        clients->push_back(address);
    }

    group = "zookeeper";
    for (auto it = site_deploy[group].begin(); it != site_deploy[group].end() ; ++it) {
        std::string socket = (*it).as<std::string>();
        size_t col_pos = socket.find_first_of(':',0);
        zk_nodes->push_back(socket.substr(0, socket.length()-(socket.length()-col_pos)));
        zk_ports->push_back(socket.substr(col_pos+1));
    }
    return error;
}

void config_yaml::print() {

    printf("Servers:\n");
    for( size_t i = 0; i < servers->size(); i++){
        printf("%zu: %s\n", i, servers->at(i).c_str());
        printf("Server %zu replicas: \n", i);
        std::vector<size_t> repList = replicas->at(i);
        for (size_t j = 0; j < repList.size(); ++j) {
            printf("%zu: %zu\n", j, repList.at(j));
        }
        printf("--- \n");
    }
    printf("=== \n");
    printf("Clients:\n");
    for( size_t i = 0; i < clients->size(); i++){
        printf("%zu: %s\n", i, clients->at(i).c_str());
    }

    printf("=== \n");
    printf("Zookeeper cluster:\n");
    assert(zk_nodes->size() == zk_ports->size());
    for(size_t i = 0; i < zk_nodes->size(); i++){
        printf("%zu ZkNode: %s:%s \n", i, zk_nodes->at(i).c_str(), zk_ports->at(i).c_str());
    }
}

void config_yaml::clear() {
    assert(servers->size() == replicas->size());
    for( size_t i = 0; i < servers->size(); i++){
        replicas->at(i).clear();
//        std::vector<std::string> * repList = replicas->at(i);
//        repList->clear();
//        delete repList;
    }
    servers->clear();
    replicas->clear();

    clients->clear();

    zk_nodes->clear();
    zk_ports->clear();
}


