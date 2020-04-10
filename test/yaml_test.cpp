//
// Created by Thamir Qadah on 4/7/20.
//

#include <gtest/gtest.h>
#include <util/config_yaml.h>

TEST(ConfigYaml, LoadTest){
    auto * conf = new config_yaml();
    char * in = (char *) "./test/test_config.yml";
    int rc = conf->load(in);
    conf->print();

    delete(conf);
    ASSERT_EQ(rc, 0);
}

TEST(ConfigYaml, LoadTest2){
    auto * conf = new config_yaml();
    char * in = (char *) "./test/test_config2.yml";
    int rc = conf->load(in);
    conf->print();
    delete(conf);
    ASSERT_EQ(rc, 0);
}