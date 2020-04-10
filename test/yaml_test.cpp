//
// Created by Thamir Qadah on 4/7/20.
//

#include <gtest/gtest.h>
#include <util/config_yaml.h>

TEST(ConfigYaml, TraceTest){
    auto * conf = new config_yaml();
    char * in = (char *) "./test/test_config.yml";
    int rc = conf->trace(in);
    delete(conf);
    ASSERT_EQ(rc, 0);
}

TEST(ConfigYaml, LoadTest){
    auto * conf = new config_yaml();
    char * in = (char *) "./test/test_config.yml";
    int rc = conf->load(in);
    conf->print();

    ASSERT_EQ(rc, 0);
    ASSERT_EQ(conf->servers->size(), 2);
    ASSERT_EQ(conf->replicas->at(0)->size(), 3);
    ASSERT_EQ(conf->replicas->at(1)->size(), 2);
    ASSERT_EQ(conf->zk_nodes->size(), 3);
    ASSERT_EQ(conf->zk_ports->size(), 3);

    delete(conf);
}

TEST(ConfigYaml, LoadTest_NoZks){
    auto conf = new config_yaml();
    char * in = (char *) "./test/test_config2.yml";
    int rc = conf->load(in);
    conf->print();

    ASSERT_EQ(rc, 0);
    ASSERT_EQ(conf->servers->size(), 2);
    ASSERT_EQ(conf->replicas->at(0)->size(), 2);
    ASSERT_EQ(conf->replicas->at(1)->size(), 2);
    ASSERT_EQ(conf->zk_nodes->size(), 0);
    ASSERT_EQ(conf->zk_ports->size(), 0);

    delete(conf);
}