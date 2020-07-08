//
// Created by Thamir Qadah on 4/7/20.
//

#ifndef EXPODB_QCD_CONFIG_YAML_H
#define EXPODB_QCD_CONFIG_YAML_H

#include <yaml-cpp/yaml.h>
#include <yaml.h>

#include <vector>
#include <string>



typedef enum rc_e {
    OK,
    RC_ERROR
} rc_t;


class config_yaml {

    YAML::Node site_deploy;
    int done = 0;

public:
    // list of servers ip addresses
    std::vector<std::string> * servers;
    std::vector<std::vector<size_t>> * replicas;

    // list of servers ip addresses
    std::vector<std::string> * clients;

    // list of zookeeper ips
    // zk_nodes[i] with port at zk_ports[i]
    std::vector<std::string> * zk_nodes;
    std::vector<std::string> * zk_ports;

    void clear();

    config_yaml();

    ~config_yaml();

    /**
     * Loads configuration parameters into class
     * @param input File path to YAML config file
     * @return 0 if success or 1 in case of error in YAML file
     */

    int load(std::string input);

    void print();

};


#endif //EXPODB_QCD_CONFIG_YAML_H
