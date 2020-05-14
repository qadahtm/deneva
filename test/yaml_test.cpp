//
// Created by Thamir Qadah on 4/7/20.
//

#include <gtest/gtest.h>
#include <util/config_yaml.h>

TEST(ConfigYaml, TraceTest){
    auto * conf = new config_yaml(false);
    auto in = std::string("./test/test_config.yml");
    int rc = conf->trace(in);
    delete(conf);
    ASSERT_EQ(rc, 0);
}

TEST(ConfigYaml, LoadTest){
    auto * conf = new config_yaml(false);
    auto in = std::string("./test/test_config.yml");
    int rc = conf->load(in);
    conf->print();

    ASSERT_EQ(rc, 0);
    ASSERT_EQ(conf->servers->size(), 2);
    ASSERT_STREQ(conf->servers->at(0).c_str(), "192.168.1.200");
    ASSERT_STREQ(conf->servers->at(1).c_str(), "192.168.2.200");

    ASSERT_EQ(conf->replicas->at(0).size(), 3);
    ASSERT_STREQ(conf->replicas->at(0).at(0).c_str(), "192.168.1.1");
    ASSERT_STREQ(conf->replicas->at(0).at(1).c_str(), "192.169.1.2");
    ASSERT_STREQ(conf->replicas->at(0).at(2).c_str(), "192.169.1.3");

    ASSERT_EQ(conf->replicas->at(1).size(), 2);
    ASSERT_STREQ(conf->replicas->at(1).at(0).c_str(), "192.168.1.100");
    ASSERT_STREQ(conf->replicas->at(1).at(1).c_str(), "192.168.1.101");

    ASSERT_EQ(conf->clients->size(), 3);
    ASSERT_STREQ(conf->clients->at(0).c_str(), "192.168.10.1");
    ASSERT_STREQ(conf->clients->at(1).c_str(), "192.168.10.2");
    ASSERT_STREQ(conf->clients->at(2).c_str(), "192.168.10.3");

    ASSERT_EQ(conf->zk_nodes->size(), 3);
    ASSERT_STREQ(conf->zk_nodes->at(0).c_str(), "192.168.50.1");
    ASSERT_STREQ(conf->zk_nodes->at(1).c_str(), "192.168.50.2");
    ASSERT_STREQ(conf->zk_nodes->at(2).c_str(), "192.168.50.3");

    ASSERT_EQ(conf->zk_ports->size(), 3);
    ASSERT_STREQ(conf->zk_ports->at(0).c_str(), "2828");
    ASSERT_STREQ(conf->zk_ports->at(1).c_str(), "2828");
    ASSERT_STREQ(conf->zk_ports->at(2).c_str(), "2828");

    delete(conf);
}

TEST(ConfigYaml, LoadTest_NoZks){
    auto conf = new config_yaml(false);
    auto in = std::string("./test/test_config2.yml");
    int rc = conf->load(in);
    conf->print();

    ASSERT_EQ(rc, 0);
    ASSERT_EQ(conf->servers->size(), 2);
    ASSERT_EQ(conf->replicas->at(0).size(), 2);
    ASSERT_EQ(conf->replicas->at(1).size(), 2);

    ASSERT_EQ(conf->clients->size(), 2);
    ASSERT_STREQ(conf->clients->at(0).c_str(), "192.168.100.1");
    ASSERT_STREQ(conf->clients->at(1).c_str(), "192.168.100.2");

    ASSERT_EQ(conf->zk_nodes->size(), 0);
    ASSERT_EQ(conf->zk_ports->size(), 0);

    delete(conf);
}

TEST(ConfigYaml, LoadTest_YamlCpp){
    auto conf = new config_yaml(true);
    auto in = std::string("./test/test_config.yml");
    int rc = conf->load(in);
    conf->print();

    ASSERT_EQ(rc, 0);
    ASSERT_EQ(conf->servers->size(), 2);
    ASSERT_STREQ(conf->servers->at(0).c_str(), "192.168.1.200");
    ASSERT_STREQ(conf->servers->at(1).c_str(), "192.168.2.200");

    ASSERT_EQ(conf->replicas->at(0).size(), 3);
    ASSERT_STREQ(conf->replicas->at(0).at(0).c_str(), "192.168.1.1");
    ASSERT_STREQ(conf->replicas->at(0).at(1).c_str(), "192.169.1.2");
    ASSERT_STREQ(conf->replicas->at(0).at(2).c_str(), "192.169.1.3");

    ASSERT_EQ(conf->replicas->at(1).size(), 2);
    ASSERT_STREQ(conf->replicas->at(1).at(0).c_str(), "192.168.1.100");
    ASSERT_STREQ(conf->replicas->at(1).at(1).c_str(), "192.168.1.101");

    ASSERT_EQ(conf->clients->size(), 3);
    ASSERT_STREQ(conf->clients->at(0).c_str(), "192.168.10.1");
    ASSERT_STREQ(conf->clients->at(1).c_str(), "192.168.10.2");
    ASSERT_STREQ(conf->clients->at(2).c_str(), "192.168.10.3");

    ASSERT_EQ(conf->zk_nodes->size(), 3);
    ASSERT_STREQ(conf->zk_nodes->at(0).c_str(), "192.168.50.1");
    ASSERT_STREQ(conf->zk_nodes->at(1).c_str(), "192.168.50.2");
    ASSERT_STREQ(conf->zk_nodes->at(2).c_str(), "192.168.50.3");

    ASSERT_EQ(conf->zk_ports->size(), 3);
    ASSERT_STREQ(conf->zk_ports->at(0).c_str(), "2828");
    ASSERT_STREQ(conf->zk_ports->at(1).c_str(), "2828");
    ASSERT_STREQ(conf->zk_ports->at(2).c_str(), "2828");

    delete(conf);
}