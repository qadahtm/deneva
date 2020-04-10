//
// Created by Thamir Qadah on 4/7/20.
//

#include "config_yaml.h"
#include <yaml.h>
#include <cstdio>
#include <cassert>

#define RETURN_IF_RC_EQ_ERROR if (rc == ERROR){ \
return rc; \
}

#define MOVE_TO_NEXT_EVENT rc = nextEvent(); RETURN_IF_RC_EQ_ERROR

config_yaml::config_yaml() {
    yaml_parser_initialize(&this->parser);
    servers = new std::vector<std::string *>();
    clients = new std::vector<std::string *>();
    replicas = new std::vector<std::vector<std::string *> *>();

    zk_nodes = new std::vector<std::string *>();
    zk_ports = new std::vector<std::string *>();
}

config_yaml::~config_yaml() {
    assert(servers->size() == replicas->size());
    for( size_t i = 0; i < servers->size(); i++){
        std::vector<std::string *> * repList = replicas->at(i);
        for (size_t j = 0; j < repList->size(); ++j) {
            delete repList->at(j);
        }
        repList->clear();
        delete repList;
        delete servers->at(i);
    }
    servers->clear();
    replicas->clear();
    delete(servers);
    delete(replicas);

    for(auto it = clients->begin(); it < clients->end(); it++){
        delete *it;
    }
    clients->clear();
    delete(clients);

    assert(zk_nodes->size() == zk_ports->size());
    for(size_t i = 0; i < zk_nodes->size(); i++){
        delete zk_nodes->at(i);
        delete zk_ports->at(i);
    }
    zk_nodes->clear();
    zk_ports->clear();
    delete zk_nodes;
    delete zk_ports;
}

int config_yaml::load(char * input) {

//    size_t flength = strlen(input);

    printf("Test yaml file: %s\n", input);
    FILE * conf_file = fopen(input, "rb");
    if (conf_file == NULL){
        return 1;
    }
    yaml_parser_set_input_file(&parser, conf_file);
    int error = 0;
    rc_t rc __attribute((unused)) = OK;

    while (true){

        if (!yaml_parser_parse(&parser, &event)){
            error = 1;
            goto done;
        }

        if (event.type == YAML_NO_EVENT){
            break;
        }
        printf("%s\n", event_type_get_name(&event).c_str());

        if (event.type == YAML_SCALAR_EVENT &&
            scalar_get_value(&event) == "servers") {
            rc = parserServerList();
            if (rc == ERROR){
                error = 1;
                goto done;
            }
        }

        if (event.type == YAML_SCALAR_EVENT &&
            scalar_get_value(&event) == "clients") {
            rc = parseClientList();
            if (rc == ERROR){
                error = 1;
                goto done;
            }
        }

        if (event.type == YAML_SCALAR_EVENT &&
            scalar_get_value(&event) == "zookeeper") {
            rc = parseZkList();
            if (rc == ERROR){
                error = 1;
                goto done;
            }
        }


        yaml_event_delete(&event);
    }

//    if (!yaml_parser_load(&parser, &document)) {
//        error = 1;
//        goto done;
//    }

//    rc = parse_config();
//    if (rc == ERROR){
//        error = 1;
//    }

    printf("Servers list size: %lu\n", servers->size());
    printf("Client list size: %lu\n", clients->size());

done:
    fclose(conf_file);
    yaml_document_delete(&document);
    return error;
}

void config_yaml::print() {

    printf("Servers:\n");
    for( size_t i = 0; i < servers->size(); i++){
        printf("%zu: %s\n", i, servers->at(i)->c_str());
        printf("Server %zu replicas: \n", i);
        std::vector<std::string *> * repList = replicas->at(i);
        for (size_t j = 0; j < repList->size(); ++j) {
            printf("%zu: %s\n", j, repList->at(j)->c_str());
        }
        printf("--- \n");
    }
    printf("=== \n");
    printf("Clients:\n");
    for( size_t i = 0; i < clients->size(); i++){
        printf("%zu: %s\n", i, clients->at(i)->c_str());
    }

    printf("=== \n");
    printf("Zookeeper cluster:\n");
    assert(zk_nodes->size() == zk_ports->size());
    for(size_t i = 0; i < zk_nodes->size(); i++){
        printf("%zu ZkNode: %s:%s \n", i, zk_nodes->at(i)->c_str(), zk_ports->at(i)->c_str());
    }
}

rc_t config_yaml::parseServerAddress() {
    rc_t rc;
    if (scalar_get_value(&event) != "address"){
        return ERROR;
    }
    MOVE_TO_NEXT_EVENT
    std::string * address = new std::string(scalar_get_value(&event));
    servers->push_back(address);
    return OK;
}

void config_yaml::saveReplicaServerIP() {
    std::vector<std::string *> * repList;
    if (replicas->size() < servers->size()){
        repList = new std::vector<std::string *>();
        replicas->push_back(repList);
    }
    else{
        repList = replicas->back();
    }

    auto * ip = new std::string(scalar_get_value(&event));
    repList->push_back(ip);
}

rc_t config_yaml::parseServerReplicas() {
    rc_t rc;
    if (event.type != YAML_SCALAR_EVENT){
        return ERROR;
    }

    if (scalar_get_value(&event) != "replicas"){
        return ERROR;
    }

    MOVE_TO_NEXT_EVENT
    assert(event.type == YAML_SEQUENCE_START_EVENT);

    MOVE_TO_NEXT_EVENT
    while (event.type == YAML_SCALAR_EVENT){
        saveReplicaServerIP();
        MOVE_TO_NEXT_EVENT
    }
    assert(event.type == YAML_SEQUENCE_END_EVENT);

    return OK;
}

std::string config_yaml::event_type_get_name(yaml_event_t * s) {
    auto res_s = std::string();
    switch (s->type){
        /** An empty event. */
        case YAML_NO_EVENT:
            return std::string("No Event");
        case YAML_STREAM_START_EVENT:
            return std::string("Stream Start Event");

        case YAML_STREAM_END_EVENT:
            return std::string("Stream End Event");

        case YAML_DOCUMENT_START_EVENT:
            return std::string("Doc Start Event");

        case YAML_DOCUMENT_END_EVENT:
            return std::string("Doc End Event");

        case YAML_SCALAR_EVENT:
            res_s.append("Scalar Event - ");
            res_s.append("key=");
            res_s.append((char *)s->data.scalar.value, s->data.scalar.length);
            return res_s;

        case YAML_SEQUENCE_START_EVENT:
            return std::string("Seq Start Event");

        case YAML_SEQUENCE_END_EVENT:
            return std::string("Seq End Event");

        case YAML_MAPPING_START_EVENT:
            return std::string("Mapping Start Event");

        case YAML_MAPPING_END_EVENT:
            return std::string("Mapping End Event");

        default:
            return std::string("Unknown Event");

    }
}

std::string config_yaml::scalar_get_value(yaml_event_t * s) {
    assert(s->type == YAML_SCALAR_EVENT);
    return std::string((char *)s->data.scalar.value, s->data.scalar.length);
}


rc_t config_yaml::parserServerList() {
    rc_t rc;

    MOVE_TO_NEXT_EVENT
    assert(event.type == YAML_SEQUENCE_START_EVENT);

    MOVE_TO_NEXT_EVENT
    do{
        printf("%s\n", event_type_get_name(&event).c_str());
        assert(event.type == YAML_MAPPING_START_EVENT);

        MOVE_TO_NEXT_EVENT
        assert(event.type == YAML_SCALAR_EVENT);
        rc = parseServerAddress();
        RETURN_IF_RC_EQ_ERROR

        MOVE_TO_NEXT_EVENT
        rc = parseServerReplicas();
        RETURN_IF_RC_EQ_ERROR

        MOVE_TO_NEXT_EVENT
        assert(event.type == YAML_MAPPING_END_EVENT);

        MOVE_TO_NEXT_EVENT
    } while (event.type != YAML_SEQUENCE_END_EVENT);
    return OK;
}

rc_t config_yaml::nextEvent() {
    if (!yaml_parser_parse(&parser, &event)){
        return  ERROR;
    }
    return OK;
}

rc_t config_yaml::parseClientList() {
    rc_t rc;

    MOVE_TO_NEXT_EVENT
    assert(event.type == YAML_SEQUENCE_START_EVENT);

    MOVE_TO_NEXT_EVENT
    while(event.type != YAML_SEQUENCE_END_EVENT){
        rc = parseClientAddress();
        RETURN_IF_RC_EQ_ERROR

        MOVE_TO_NEXT_EVENT
    }
    return OK;
}

rc_e config_yaml::parseClientAddress() {
    if (event.type != YAML_SCALAR_EVENT){
        return  ERROR;
    }
    std::string * address = new std::string(scalar_get_value(&event));
    clients->push_back(address);
    return OK;
}

rc_t config_yaml::parseZkList() {
    rc_t rc;

    MOVE_TO_NEXT_EVENT
    assert(event.type == YAML_SEQUENCE_START_EVENT);

    MOVE_TO_NEXT_EVENT
    while(event.type != YAML_SEQUENCE_END_EVENT){
        rc = parseZkEntry();
        RETURN_IF_RC_EQ_ERROR

        MOVE_TO_NEXT_EVENT
    }
    return OK;
}

rc_e config_yaml::parseZkEntry() {
    if (event.type != YAML_SCALAR_EVENT){
        return  ERROR;
    }
    std::string socket = std::string(scalar_get_value(&event));
    size_t col_pos = socket.find_first_of(':',0);
    std::string * port = new std::string(socket.substr(col_pos+1));
    std::string * address = new std::string(socket.substr(0, socket.length()-(socket.length()-col_pos)));
    zk_nodes->push_back(address);
    zk_ports->push_back(port);
    return OK;
}


