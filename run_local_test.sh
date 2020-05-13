# A simple shell script to run processes in the background.
# Note: This assumes the build was made using CMake.
./bin/rundb -nid0 &
./bin/rundb -nid1 &
./bin/runcl -nid2 &
#./bin/runcl -nid2 &
#./bin/runcl -nid2 &
#./bin/runcl -nid3 &