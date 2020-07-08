//
// Created by Thamir Qadah on 4/10/20.
//

#include <util/config_yaml.h>
#include <gtest/gtest.h>

#include <paxos.h>


TEST(ZookeeperPaxos, LoadConfigTest){
    auto conf = new config_yaml();
    auto in = std::string("./test/test_zk_only_config.yml");
    int rc = conf->load(in);
    conf->print();

    ASSERT_EQ(rc, 0);
    ASSERT_EQ(conf->servers->size(), 0);
    ASSERT_EQ(conf->replicas->size(), 0);
    ASSERT_EQ(conf->clients->size(), 0);

    ASSERT_EQ(conf->zk_nodes->size(), 1);
    ASSERT_EQ(conf->zk_ports->size(), 1);

    ASSERT_STREQ(conf->zk_ports->at(0).c_str(), "2181");

    delete(conf);
}

TEST(ZookeeperPaxos, PaxosInitTest){

    auto rootPath = std::string("/test_root");
    auto paxos_writer = new Paxos(std::string("./test/test_zk_only_config.yml"), false, rootPath);
    auto paxos_reader = new Paxos(std::string("./test/test_zk_only_config.yml"), true, rootPath);
//    auto paxos_writer = new Paxos(std::string("./test/test_zk_only_config.yml"), false);
//    auto paxos_reader = new Paxos(std::string("./test/test_zk_only_config.yml"), true);

    auto test_data = std::string("TestData-Batch0");

    printf("Testing: Submitting a batch\n");
    paxos_writer->SubmitBatch(test_data);

    std::string read_data;

    printf("Testing: Reading a batch\n");
    paxos_reader->GetNextBatchBlocking(&read_data);

    ASSERT_STREQ(test_data.c_str(), read_data.c_str());

    printf("Testing: Cleanup ...\n");
    paxos_writer->cleanUpRoot();

    delete (paxos_writer);
    delete (paxos_reader);
}