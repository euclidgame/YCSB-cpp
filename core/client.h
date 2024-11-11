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
#include "thread_pool.h"
namespace ycsbc
{

  void EnforceClientRateLimit(long op_start_time_ns, long target_ops_per_s, long target_ops_tick_ns, int op_num)
  {
    if (target_ops_per_s > 0)
    {
      long deadline = op_start_time_ns + target_ops_tick_ns;
      std::chrono::nanoseconds deadline_ns(deadline);
      std::chrono::time_point<std::chrono::high_resolution_clock, std::chrono::nanoseconds> target_time_point(deadline_ns);
      while (std::chrono::high_resolution_clock::now() < target_time_point)
      {
      }
    }
  }

  inline std::tuple<long long, std::vector<int>> ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops, bool is_loading,
                                                              bool init_db, bool cleanup_db, utils::CountDownLatch *latch, utils::RateLimiter *rlim, ThreadPool *threadpool,
                                                              int client_id, int target_ops_per_s, int burst_gap_s, int burst_size_ops)
  {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);

    size_t cpu_for_client = client_id + 8;
    CPU_SET(cpu_for_client, &cpuset);
    std::cout << "[YCSB] Pinning client to " << cpu_for_client << std::endl;
    int rc = pthread_setaffinity_np(pthread_self(),
                                    sizeof(cpu_set_t), &cpuset);
    if (rc != 0)
    {
      fprintf(stderr, "Couldn't set thread affinity.\n");
      std::exit(1);
    }

    int num_bursts = 1;
    int adjusted_num_ops = num_ops;
    if (burst_gap_s > 0)
    {
      if (client_id == 0)
      {
        std::this_thread::sleep_for(std::chrono::seconds(burst_gap_s));
        adjusted_num_ops = 13;
      }
      else if (client_id == 1)
      {
        std::this_thread::sleep_for(std::chrono::seconds(burst_gap_s / 2));
        adjusted_num_ops = 21;
      }
      num_bursts = 100;
    }

    std::vector<int> op_progress;
    int client_log_interval_s = 1;

    long target_ops_tick_ns = 0;
    if (target_ops_per_s > 0)
    {
      target_ops_tick_ns = (long)(1000000000 / target_ops_per_s);
    }
    std::cout << "\033[34m[YCSB] Target Operation Rate (ops/s): " << target_ops_per_s << "\033[0m" << std::endl;
    std::cout << "\033[34m[YCSB] Target Operation Tick (ns): " << target_ops_tick_ns << "\033[0m" << std::endl;

    try
    {
      if (init_db)
      {
        db->Init();
      }
      std::this_thread::sleep_for(std::chrono::seconds(10));
      auto client_start = std::chrono::system_clock::now();
      auto client_start_micros = std::chrono::duration_cast<std::chrono::microseconds>(client_start.time_since_epoch()).count();
      auto client_start_steady = std::chrono::steady_clock::now();
      auto interval_start_time = std::chrono::steady_clock::now();
      int printed = 0;
      std::atomic<int> active_threads(0);
      MyThreadPool pool(4);

      int ops = 0;
      for (int b = 0; b < num_bursts; ++b)
      {
        for (int i = 0; i < adjusted_num_ops; ++i)
        {
          if (rlim)
          {
            rlim->Consume(1);
          }

          auto op_start_time = std::chrono::high_resolution_clock::now();
          auto op_start_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(op_start_time.time_since_epoch()).count();

          if (is_loading)
          {
            wl->DoInsert(*db);
          }
          else
          {
            auto now = std::chrono::high_resolution_clock::now();
            auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
            pool.enqueue([wl, db, client_id, now_ns]()
                         { wl->DoTransaction(*db, client_id, now_ns); });
          }
          ops++;

          // Periodically check whether log interval has been hit
          auto current_time = std::chrono::steady_clock::now();
          int elapsed_time_s = std::chrono::duration_cast<std::chrono::seconds>(current_time - client_start_steady).count();
          if (elapsed_time_s % 5 == 0 && elapsed_time_s > printed)
          {
            printed = elapsed_time_s;
            std::cout << "\033[34m[YCSB] Requests sent for client " << client_id << ": " << ops << " at " << elapsed_time_s << "s\033[0m" << std::endl;
          }
          if (i % 100 == 0)
          {
            if (elapsed_time_s >= client_log_interval_s)
            {
              op_progress.push_back(ops);
              interval_start_time = std::chrono::steady_clock::now();
            }
          }
          if (client_id == 0)
          {
            if (elapsed_time_s % 50 <= 5 && elapsed_time_s > 0)
            {
              EnforceClientRateLimit(op_start_time_ns, (long)10000, (long)100000, ops);
            }
            else
            {
              EnforceClientRateLimit(op_start_time_ns, target_ops_per_s, target_ops_tick_ns, ops);
            }
          }
          else
          {
            EnforceClientRateLimit(op_start_time_ns, target_ops_per_s, target_ops_tick_ns, ops);
          }
        }
        std::this_thread::sleep_for(std::chrono::seconds(burst_gap_s));
      }

      pool.waitAll();

      if (cleanup_db)
      {
        db->Cleanup();
      }

      latch->CountDown();
      op_progress.push_back(ops);
      return std::make_tuple(client_start_micros, op_progress);
    }
    catch (const utils::Exception &e)
    {
      std::cerr << "Caught exception: " << e.what() << std::endl;
      exit(1);
    }
  }

} // ycsbc

#endif // YCSB_C_CLIENT_H_
