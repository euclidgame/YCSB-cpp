//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <iostream>
#include <pthread.h>
#include <string>
#include <chrono>
#include <vector>
#include <tuple>

#include "db.h"
#include "core_workload.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/utils.h"
#include "threadpool.h"
namespace ycsbc {

void EnforceClientRateLimit(long op_start_time_ns, long target_ops_per_s, long target_ops_tick_ns, int op_num) {
  if (target_ops_per_s > 0) {
    long deadline = op_start_time_ns + target_ops_tick_ns;
    std::chrono::nanoseconds deadline_ns(deadline);
    std::chrono::time_point<std::chrono::high_resolution_clock, std::chrono::nanoseconds> target_time_point(deadline_ns);

    while (std::chrono::high_resolution_clock::now() < target_time_point) {}
  }
}

inline std::tuple<long long, std::vector<int>> ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops, bool is_loading,
                        bool init_db, bool cleanup_db, utils::CountDownLatch *latch, utils::RateLimiter *rlim, ThreadPool *threadpool, 
                        int client_id, int target_ops_per_s) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  // CPU_SET(2*client_id+1, &cpuset);

  size_t cpu_for_client = client_id + 8;
  // if (client_id == 0 || client_id == 1) {
  //   cpu_for_client = client_id + 1;
  // } else {
  //   cpu_for_client = client_id + 8;
  // }
  CPU_SET(cpu_for_client, &cpuset);
  std::cout << "[TGRIGGS_LOG] Pinning client to " << cpu_for_client << std::endl;
  int rc = pthread_setaffinity_np(pthread_self(),
                                  sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    fprintf(stderr, "Couldn't set thread affinity.\n");
    std::exit(1);
  }

  // TODO: create flags for this
  int burst_gap_s = 30;
  int num_bursts = 1;
  int adjusted_num_ops = num_ops;
  if (client_id == 0 || client_id == 1) {
    std::this_thread::sleep_for(std::chrono::seconds(30));
    adjusted_num_ops = 240; 
    num_bursts = 6;
  }

  std::vector<int> op_progress;       
  int client_log_interval_s = 1;                 

  long target_ops_tick_ns = 0;
  if (target_ops_per_s > 0) {
    target_ops_tick_ns = (long) (1000000000 / target_ops_per_s);
  }

  try {
    if (init_db) {
      db->Init();
    }

    // if (client_id == 0) {
    //   adjusted_num_ops = int(adjusted_num_ops * 1.5);
    // }

    auto client_start = std::chrono::system_clock::now();
    auto client_start_micros = std::chrono::duration_cast<std::chrono::microseconds>(client_start.time_since_epoch()).count();
    // auto client_start_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(client_start.time_since_epoch()).count();
    auto interval_start_time = std::chrono::steady_clock::now();

    int ops = 0;
    for (int b = 0; b < num_bursts; ++b) {
      for (int i = 0; i < adjusted_num_ops; ++i) {
        if (rlim) {
          rlim->Consume(1);
        }

        auto op_start_time = std::chrono::high_resolution_clock::now();
        auto op_start_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(op_start_time.time_since_epoch()).count();

        if (is_loading) {
          wl->DoInsert(*db);
        } else {

          // auto txn_lambda = [wl, db, client_id]() {
          //   wl->DoTransaction(*db, client_id);
          //   return nullptr;  // to match void* return
          // };

          // // Submit operation to thread pool and wait for it. 
          // std::future<void*> result = threadpool->dispatch(txn_lambda);
          // result.wait();

          wl->DoTransaction(*db, client_id);
        }
        ops++;

        // Periodically check whether log interval has been hit
        if (i % 100 == 0) {
          auto current_time = std::chrono::steady_clock::now();
          auto elapsedTime = std::chrono::duration_cast<std::chrono::seconds>(current_time - interval_start_time);
          if (elapsedTime.count() >= client_log_interval_s) {
            op_progress.push_back(ops);
            interval_start_time = std::chrono::steady_clock::now();
          }
        }

        EnforceClientRateLimit(op_start_time_ns, target_ops_per_s, target_ops_tick_ns, ops);
      }
      std::this_thread::sleep_for(std::chrono::seconds(burst_gap_s));
    }

    if (cleanup_db) {
      db->Cleanup();
    }

    latch->CountDown();
    op_progress.push_back(ops);
    return std::make_tuple(client_start_micros, op_progress);
  } catch (const utils::Exception &e) {
    std::cerr << "Caught exception: " << e.what() << std::endl;
    exit(1);
  }
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
