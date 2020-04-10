//
// Created by Thamir Qadah on 4/7/20.
//

#ifndef EXPODB_QCD_CONFIG_YAML_H
#define EXPODB_QCD_CONFIG_YAML_H

#include <yaml.h>

#include <vector>
#include <string>

typedef enum rc_e {
    OK,
    ERROR
} rc_t;

class config_yaml {
    yaml_parser_t parser;
    yaml_document_t document;
    yaml_node_t *node;
    yaml_event_t event;
    int done = 0;

    // list of servers ip addresses
    // server[i] has replicas[i]
    std::vector<std::string *> * servers;
    std::vector<std::vector<std::string *> *> * replicas;

    // list of servers ip addresses
    std::vector<std::string *> * clients;

    // list of zookeeper ips
    // zk_nodes[i] with port at zk_ports[i]
    std::vector<std::string *> * zk_nodes;
    std::vector<std::string *> * zk_ports;


public:
    config_yaml();

    ~config_yaml();
    int load(char * input);

    void print();

    rc_t parseServerAddress();

    rc_t parseServerReplicas();

    void saveReplicaServerIP();

    static std::string event_type_get_name(yaml_event_t * s);
    static std::string scalar_get_value(yaml_event_t * s);

    rc_t parserServerList();

    rc_t nextEvent();

    rc_t parseClientList();

    rc_e parseClientAddress();

    rc_t parseZkList();

    rc_e parseZkEntry();
};


#endif //EXPODB_QCD_CONFIG_YAML_H
