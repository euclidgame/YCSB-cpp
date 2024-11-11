#!/bin/bash

# Cleanup function to kill background processes and process iostat output
cleanup() {
    kill $iostat_pid 2>/dev/null
    kill $mpstat_pid 2>/dev/null
    kill $ycsb_pid 2>/dev/null
    process_iostat_output
    process_mpstat_output
    # rm iostat_output.txt  # Clean up temporary file
}

# Function to process iostat's output and write to CSV
process_iostat_output() {
    echo "Timestamp,r/s,rMB/s,r_await,rareq-sz,w/s,wMB/s,w_await,wareq-sz" > iostat_results.csv
    current_timestamp=""
    while IFS= read -r line; do
        if [[ $line == *"Time:"* ]]; then
            # Extract timestamp
            current_timestamp=$(echo $line | awk '{print $2, $3}')
        elif [[ $line == *sda1* ]]; then
            # Write the current timestamp along with the iostat metrics
            echo "$line" | awk -v ts="$current_timestamp" '{print ts,",",$2,",",$3,",",$6,",",$7,",",$8,",",$9,",",$12,",",$13}' >> iostat_results.csv
        fi
    done < iostat_output.txt
}

process_mpstat_output() {
  echo "Timestamp,core,usr,sys,iowait,soft,idle" > mpstat_results.csv
  current_timestamp=""
  while IFS= read -r line; do
      if [[ $line == *"Time:"* ]]; then
          # Extract timestamp
          current_timestamp=$(echo $line | awk '{print $2, $3}')
      else
          # Write the current timestamp along with the iostat metrics
          echo "$line" | awk -v ts="$current_timestamp" '{print ts,",",$2,",",$3,",",$5,",",$6,",",$8,",",$12}' >> mpstat_results.csv
      fi
  done < mpstat_output.txt
}

# Remove the ycsb-rocksdb-data directory
# rm -rf /home/${USER}/ycsb-rocksdb-data

# Setup trap for cleanup on script exit
trap cleanup EXIT

# Start iostat in the background, appending a timestamp to each interval, and redirecting output to a file
(echo "Time: $(date +'%Y-%m-%d %H:%M:%S.%3N')"; iostat -xdm /dev/sda1 1;) > iostat_output.txt &
iostat_pid=$!

(echo "Time: $(date +'%Y-%m-%d %H:%M:%S.%3N')"; mpstat -P ALL 1;) > mpstat_output.txt &
mpstat_pid=$!

# Start ycsb process in the background
./ycsb -run -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
  -p rocksdb.dbname=/home/windsey/ycsb-rocksdb-data \
  -p requestdistribution=zipfian \
  -s -p operationcount=6000000 \
  -p recordcount=31250000 \
  -threads 4 \
  -p status.interval_ms=500 \
  -target_rates "2500,1000,500,10" \
  -p refill_period=5 \
  -p real_op_mode=false \
  -p status=true \
  -p lookback_intervals=30 \
  | tee status_thread.txt &


# To add:
# rocksdb parameters for memtable size, etc.

# Multi column family
# ./ycsb -run -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#   -p rocksdb.dbname=/mnt/multi-cf2/ycsb-rocksdb-data \
#   -s -p operationcount=35000000 \
#   -p recordcount=1562500 \
#   -p updateproportion=0 \
#   -p insertproportion=0 \
#   -p readproportion=1 \
#   -p scanproportion=0 \
#   -p randominsertproportion=0 \
#   -threads 2 \
#   -p op_mode=real \
#   -target_rates "900,900" \
#   -p requestdistribution=uniform \
#   | tee status_thread.txt &

# Multi column family
# ./ycsb -run -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#   -p rocksdb.dbname=/mnt/multi-cf2/ycsb-rocksdb-data-2 \
#   -s -p operationcount=35000000 \
#   -p recordcount=1562500 \
#   -p updateproportion=0 \
#   -p insertproportion=0 \
#   -p readproportion=1 \
#   -p scanproportion=0 \
#   -p randominsertproportion=0 \
#   -threads 2 \
#   -p op_mode=fake \
#   -target_rates "200,900" \
#   -p rate_limits=60 \
#   -p read_rate_limits=60 \
#   -p refill_period=10 \
#   -p requestdistribution=uniform \
#   | tee status_thread2.txt &

ycsb_pid=$!
echo "YCSB pid: ${ycsb_pid}"
# Wait for the ycsb process to finish
wait $ycsb_pid

# The script exits here, triggering the cleanup function

# GDB format
# gdb --args ./ycsb -run -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties -p rocksdb.dbname=/home/windsey/ycsb-rocksdb-data \
#   -s -p operationcount=35000000 \
#   -p recordcount=1562500 \
#   -p updateproportion=1 \
#   -p insertproportion=0 \
#   -p readproportion=0 \
#   -p scanproportion=0 \
#   -p randominsertproportion=0 \
#   -threads 4 \
#   -p rate_limits=200 \
#   -p requestdistribution=uniform \
#   | tee status_thread.txt


# nohup ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#   -p recordcount=3125000 \
#   -p fieldcount=16 \
#   -p fieldlength=1024 \
#   -p table=default \
#   -threads 1 \
#   -p op_mode=real \
#   -p requestdistribution=uniform \
#   -p rocksdb.dbname=/home/windsey/ycsb-rocksdb-data -s | tee status_thread.txt &

# nohup ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#   -p recordcount=3125000 \
#   -p fieldcount=16 \
#   -p fieldlength=1024 \
#   -p table=cf2 \
#   -threads 1 \
#   -p op_mode=real \
#   -p requestdistribution=uniform \
#   -p rocksdb.dbname=/home/windsey/ycsb-rocksdb-data -s | tee status_thread.txt &

#   nohup ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#   -p recordcount=3125000 \
#   -p fieldcount=16 \
#   -p fieldlength=1024 \
#   -p table=cf3 \
#   -threads 1 \
#   -p op_mode=real \
#   -p requestdistribution=uniform \
#   -p rocksdb.dbname=/home/windsey/ycsb-rocksdb-data -s | tee status_thread.txt &

#   nohup ./ycsb -load -db rocksdb -P workloads/workloada -P rocksdb/rocksdb.properties \
#   -p recordcount=3125000 \
#   -p fieldcount=16 \
#   -p fieldlength=1024 \
#   -p table=cf4 \
#   -threads 1 \
#   -p op_mode=real \
#   -p requestdistribution=uniform \
#   -p rocksdb.dbname=/home/windsey/ycsb-rocksdb-data -s | tee status_thread.txt &