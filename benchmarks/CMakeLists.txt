add_sources(pps_helper.cpp
        pps_query.cpp
        pps_txn.cpp
        pps_wl.cpp
        tpcc_wl.cpp
        tpcc_txn.cpp
        tpcc_query.cpp
        tpcc_helper.cpp
        ycsb_wl.cpp
        ycsb_query.cpp
        ycsb_txn.cpp
        )

configure_file(YCSB_schema.txt YCSB_schema.txt COPYONLY)
configure_file(TPCC_full_schema.txt TPCC_full_schema.txt COPYONLY)
configure_file(TPCC_short_schema.txt TPCC_short_schema.txt COPYONLY)
configure_file(PPS_schema.txt PPS_schema.txt COPYONLY)