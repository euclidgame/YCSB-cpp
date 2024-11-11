//
//  ycsbc.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include <cstring>
#include <ctime>

#include <string>
#include <iostream>
#include <vector>
#include <thread>
#include <future>
#include <chrono>
#include <iomanip>
#include "threadpool.h"

#include "client.h"
#include "core_workload.h"
#include "db_factory.h"
#include "fair_scheduler.h"
#include "measurements.h"
#include "resource_scheduler.h"
#include "threadpool.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/resources.h"
#include "utils/timer.h"
#include "utils/utils.h"

// #ifdef HDRMEASUREMENT
#include <hdr/hdr_histogram.h>
// #endif

using namespace std::chrono;
using ycsbc::utils::MultiTenantResourceShares;
using ycsbc::utils::MultiTenantResourceUsage;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
void ParseCommandLine(int argc, const char *argv[], ycsbc::utils::Properties &props);

void StatusThread(ycsbc::Measurements *measurements, std::vector<ycsbc::Measurements *> per_client_measurements,
                  ycsbc::utils::CountDownLatch *latch, double interval_ms, std::vector<ycsbc::DB *> dbs)
{
  std::string client_stats_filename = "logs/client_stats.log";
  std::ofstream client_stats_logfile;
  std::string memtable_size_filename = "logs/memtable_size.log";
  std::ofstream memtable_size_logfile;
  client_stats_logfile.open(client_stats_filename, std::ios::out | std::ios::trunc);
  memtable_size_logfile.open(memtable_size_filename, std::ios::out | std::ios::trunc);
  if (!client_stats_logfile.is_open())
  {
    // TODO(tgriggs):  Handle file open failure, propagate exception
    // throw std::ios_base::failure("Failed to open the file.");
  }
  if (!memtable_size_logfile.is_open())
  {
    throw std::ios_base::failure("Failed to open the file.");
  }
  {
    /* code */
  }

  client_stats_logfile << "timestamp,client_id,op_type,count,max,min,avg,50p,90p,99p,99.9p" << std::endl;

  time_point<system_clock> start = system_clock::now();
  bool done = false;

  int print_intervals = 10;
  int cur_interval = 0;
  bool should_print = false;
  bool started = false;
  while (1)
  {
    if (++cur_interval == print_intervals)
    {
      cur_interval = 0;
      should_print = true;
    }

    // Print experiment duration so far
    time_point<system_clock> now = system_clock::now();
    auto elapsed_time_s = std::chrono::duration_cast<std::chrono::seconds>(now - start).count();
    if (elapsed_time_s % 5 == 0)
    {
      std::cout << "[YCSB] Exp time: " << elapsed_time_s << "s" << std::endl;
    }

    auto duration_since_epoch = now.time_since_epoch();
    auto duration_since_epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration_since_epoch).count();

    // Print status message less frequently
    for (size_t i = 0; i < per_client_measurements.size(); ++i)
    {
      std::vector<std::string> op_csv_stats = per_client_measurements[i]->GetCSVStatusMsg();
      if (op_csv_stats.size() > 0)
      {
        started = true;
      }
      for (const auto &csv : op_csv_stats)
      {
        client_stats_logfile << duration_since_epoch_ms << ',' << i << ',' << csv << std::endl;
        if (should_print)
        {
          std::cout << duration_since_epoch_ms << ',' << i << ',' << csv << std::endl;
        }
      }
      if (started)
      {
        memtable_size_logfile << duration_since_epoch_ms << ',' << i << ','
                              << dbs[i]->GetCurSizeActiveMemtable(i) << ','
                              << dbs[i]->GetNumImmutableMemtable(i) << std::endl;
      }
      per_client_measurements[i]->Reset();
    }
    std::vector<std::string> op_csv_stats_total = measurements->GetCSVStatusMsg();
    for (const auto &csv : op_csv_stats_total)
    {
      if (should_print)
      {
        std::cout << duration_since_epoch_ms << ',' << csv << std::endl;
      }
    }

    if (done)
    {
      break;
    }
    should_print = false;
    done = latch->AwaitForMs(interval_ms);
  };
}

void RateLimitThread(std::string rate_file, std::vector<ycsbc::utils::RateLimiter *> rate_limiters,
                     ycsbc::utils::CountDownLatch *latch)
{
  std::ifstream ifs;
  ifs.open(rate_file);

  if (!ifs.is_open())
  {
    ycsbc::utils::Exception("failed to open: " + rate_file);
  }

  int64_t num_threads = rate_limiters.size();

  int64_t last_time = 0;
  while (!ifs.eof())
  {
    int64_t next_time;
    int64_t next_rate;
    ifs >> next_time >> next_rate;

    if (next_time <= last_time)
    {
      ycsbc::utils::Exception("invalid rate file");
    }

    bool done = latch->AwaitFor(next_time - last_time);
    if (done)
    {
      break;
    }
    last_time = next_time;

    for (auto x : rate_limiters)
    {
      x->SetRate(next_rate / num_threads);
    }
  }
}

std::vector<int> stringToIntVector(const std::string &input)
{
  std::vector<int> result;
  std::stringstream ss(input);
  std::string item;
  while (std::getline(ss, item, ','))
  {
    result.push_back(std::stoi(item));
  }
  return result;
}

void writeToCSV(const std::string &filename, const std::vector<std::tuple<long long, std::vector<int>>> &data)
{
  std::ofstream file(filename);
  if (!file.is_open())
  {
    std::cerr << "Failed to open file for writing.\n";
    return;
  }
  for (const auto &row : data)
  {
    auto &timestamp = std::get<0>(row);
    auto &ints = std::get<1>(row);
    // auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(timestamp.time_since_epoch()).count();
    file << timestamp << ",";
    for (size_t i = 0; i < ints.size(); ++i)
    {
      file << ints[i];
      if (i < ints.size() - 1)
      {
        file << ",";
      }
    }
    file << "\n";
  }
  file.close();
  std::cout << "Data written to " << filename << std::endl;
}

std::vector<int> calculateOperations(const std::vector<int> &target_rates, int total_operations)
{
  // Calculate the sum of target rates
  int total_rate = std::accumulate(target_rates.begin(), target_rates.end(), 0);

  // If all target rates are zero, distribute operations equally
  if (total_rate == 0)
  {
    int equal_operations = total_operations / target_rates.size();
    int remainder = total_operations % target_rates.size();

    std::vector<int> operations_per_client(target_rates.size(), equal_operations);

    // Distribute the remainder among the first few clients
    for (int i = 0; i < remainder; ++i)
    {
      operations_per_client[i] += 1;
    }

    return operations_per_client;
  }
  // Otherwise, distribute based on target rates
  std::vector<int> operations_per_client;
  std::vector<double> remainders;
  int operation_sum = 0;

  for (int rate : target_rates)
  {
    double operations = static_cast<double>(rate) / total_rate * total_operations;
    int rounded_operations = std::floor(operations);
    operation_sum += rounded_operations;
    operations_per_client.push_back(rounded_operations);
    remainders.push_back(operations - rounded_operations);
  }

  // Adjust for rounding errors to match total_operations
  while (operation_sum < total_operations)
  {
    auto max_remainder_it = std::max_element(remainders.begin(), remainders.end());
    int idx = std::distance(remainders.begin(), max_remainder_it);
    operations_per_client[idx] += 1;
    operation_sum += 1;
    remainders[idx] = 0.0;
  }

  return operations_per_client;
}

int main(const int argc, const char *argv[])
{
  ycsbc::utils::Properties props;
  ParseCommandLine(argc, argv, props);

  const bool do_load = (props.GetProperty("doload", "false") == "true");
  const bool do_transaction = (props.GetProperty("dotransaction", "false") == "true");
  if (!do_load && !do_transaction)
  {
    std::cerr << "No operation to do" << std::endl;
    exit(1);
  }

  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  std::vector<int> target_rates = stringToIntVector(props.GetProperty("target_rates", "0"));

  ycsbc::Measurements *measurements = ycsbc::CreateMeasurements(&props);
  if (measurements == nullptr)
  {
    std::cerr << "Unknown measurements name" << std::endl;
    exit(1);
  }

  std::vector<ycsbc::Measurements *> per_client_measurements = ycsbc::CreatePerClientMeasurements(&props, num_threads);
  for (const auto measurement : per_client_measurements)
  {
    if (measurement == nullptr)
    {
      std::cerr << "Unknown per-client measurements name" << std::endl;
      exit(1);
    }
  }
  std::shared_ptr<ycsbc::utils::MultiTenantCounter> per_client_bytes_written = std::make_shared<ycsbc::utils::MultiTenantCounter>(num_threads);

  std::vector<ycsbc::DB *> dbs;
  for (int i = 0; i < num_threads; i++)
  {
    // ycsbc::DB *db = ycsbc::DBFactory::CreateDB(&props, measurements);
    ycsbc::DB *db = ycsbc::DBFactory::CreateDBWithPerClientStats(&props, measurements, per_client_measurements, per_client_bytes_written);
    if (db == nullptr)
    {
      std::cerr << "Unknown database name " << props["dbname"] << std::endl;
      exit(1);
    }
    dbs.push_back(db);
  }

  ycsbc::CoreWorkload wl;
  wl.Init(props);

  // print status periodically
  const bool show_status = (props.GetProperty("status", "false") == "true");
  const bool use_rsched = (props.GetProperty("rsched", "false") == "true");
  const double status_interval_ms = std::stod(props.GetProperty("status.interval_ms", "500"));

  // load phase
  if (do_load)
  {
    const int total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);

    ycsbc::utils::CountDownLatch latch(num_threads);
    ycsbc::utils::Timer<double> timer;

    timer.Start();
    std::future<void> status_future;
    if (show_status)
    {
      status_future = std::async(std::launch::async, StatusThread,
                                 measurements, per_client_measurements, &latch, status_interval_ms, dbs);
    }
    std::vector<std::future<std::tuple<long long, std::vector<int>>>> client_threads;
    std::vector<int> operations_per_client = calculateOperations(target_rates, total_ops);
    for (int i = 0; i < num_threads; ++i)
    {
      std::cout << "[YCSB] Thread ops for client " << i << ": " << operations_per_client[i] << std::endl;
      client_threads.emplace_back(std::async(std::launch::async, ycsbc::ClientThread, dbs[i], &wl,
                                             operations_per_client[i], true, /*init_db=*/true, !do_transaction, &latch, nullptr, nullptr, i, /*target_op_per_s*/ 0, 0, 0));
    }
    assert((int)client_threads.size() == num_threads);

    std::vector<std::tuple<long long, std::vector<int>>> client_op_progresses;
    int sum = 0;
    for (auto &n : client_threads)
    {
      assert(n.valid());
      std::tuple<long long, std::vector<int>> start_and_client_op_progress = n.get();
      client_op_progresses.push_back(start_and_client_op_progress);
      sum += std::get<1>(start_and_client_op_progress).back();
    }
    double runtime = timer.End();

    if (show_status)
    {
      status_future.wait();
    }

    std::cout << "Load runtime(sec): " << runtime << std::endl;
    std::cout << "Load operations(ops): " << sum << std::endl;
    std::cout << "Load throughput(ops/sec): " << sum / runtime << std::endl;
  }

  measurements->Reset();
  std::this_thread::sleep_for(std::chrono::seconds(stoi(props.GetProperty("sleepafterload", "0"))));

  // FairScheduler scheduler;
  ThreadPool threadpool;
  // threadpool.start(/*num_threads=*/ 1);

  int burst_gap_s = std::stoi(props.GetProperty("burst_gap_s", "0"));
  int burst_size_ops = std::stoi(props.GetProperty("burst_size_ops", "0"));

  // transaction phase
  if (do_transaction)
  {
    // initial ops per second, unlimited if <= 0
    const int64_t ops_limit = std::stoi(props.GetProperty("limit.ops", "0"));
    // rate file path for dynamic rate limiting, format "time_stamp_sec new_ops_per_second" per line
    std::string rate_file = props.GetProperty("limit.file", "");

    const int total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);

    ycsbc::utils::CountDownLatch latch(num_threads);
    ycsbc::utils::Timer<double> timer;

    timer.Start();
    std::future<void> status_future;
    if (show_status)
    {
      status_future = std::async(std::launch::async, StatusThread,
                                 measurements, per_client_measurements, &latch, status_interval_ms, dbs);
    }
    std::vector<std::future<std::tuple<long long, std::vector<int>>>> client_threads;
    std::vector<ycsbc::utils::RateLimiter *> rate_limiters;
    std::vector<int> operations_per_client = calculateOperations(target_rates, total_ops);
    for (int i = 0; i < num_threads; ++i)
    {
      std::cout << "Thread ops for client " << i << ": " << operations_per_client[i] << std::endl;
      ycsbc::utils::RateLimiter *rlim = nullptr;
      if (ops_limit > 0 || rate_file != "")
      {
        int64_t per_thread_ops = ops_limit / num_threads;
        rlim = new ycsbc::utils::RateLimiter(per_thread_ops, per_thread_ops);
      }
      rate_limiters.push_back(rlim);
      client_threads.emplace_back(std::async(std::launch::async, ycsbc::ClientThread, dbs[i], &wl,
                                             operations_per_client[i], false, !do_load, true, &latch, rlim,
                                             &threadpool, i, target_rates[i], burst_gap_s,
                                             burst_size_ops));
    }

    std::future<void> rlim_future;
    if (rate_file != "")
    {
      rlim_future = std::async(std::launch::async, RateLimitThread, rate_file, rate_limiters, &latch);
    }

    std::future<void> rsched_future;
    if (use_rsched)
    {
      ycsbc::ResourceSchedulerOptions rsched_options;
      rsched_options.rsched_interval_ms = std::stod(props.GetProperty("rsched_interval_ms"));
      rsched_options.stats_dump_interval_s = 5;
      rsched_options.lookback_intervals = std::stoi(props.GetProperty("lookback_intervals"));
      rsched_options.ramp_up_multiplier = std::stod(props.GetProperty("rsched_rampup_multiplier"));
      rsched_options.io_read_capacity_kbps = std::stoi(props.GetProperty("io_read_capacity_kbps"));
      rsched_options.io_write_capacity_kbps = std::stoi(props.GetProperty("io_write_capacity_kbps"));
      rsched_options.memtable_capacity_kb = std::stoi(props.GetProperty("memtable_capacity_kb"));
      rsched_options.max_memtable_size_kb = std::stoi(props.GetProperty("max_memtable_size_kb"));
      rsched_options.min_memtable_size_kb = std::stoi(props.GetProperty("min_memtable_size_kb"));
      rsched_options.min_memtable_count = std::stoi(props.GetProperty("min_memtable_count"));
      rsched_future = std::async(std::launch::async, ycsbc::CentralResourceSchedulerThread, dbs,
                                 measurements, per_client_measurements, rsched_options, &latch);
    }

    assert((int)client_threads.size() == num_threads);

    std::vector<std::tuple<long long, std::vector<int>>> client_op_progresses;
    int sum = 0;
    for (auto &n : client_threads)
    {
      assert(n.valid());
      std::tuple<long long, std::vector<int>> start_and_client_op_progress = n.get();
      client_op_progresses.push_back(start_and_client_op_progress);
      sum += std::get<1>(start_and_client_op_progress).back();
    }
    double runtime = timer.End();

    if (show_status)
    {
      status_future.wait();
    }
    if (use_rsched)
    {
      rsched_future.wait();
    }
    writeToCSV("client_progress.csv", client_op_progresses);

    std::cout << "Run runtime(sec): " << runtime << std::endl;
    std::cout << "Run operations(ops): " << sum << std::endl;
    std::cout << "Run throughput(ops/sec): " << sum / runtime << std::endl;
  }

  for (int i = 0; i < num_threads; i++)
  {
    delete dbs[i];
  }
}

void ParseCommandLine(int argc, const char *argv[], ycsbc::utils::Properties &props)
{
  int argindex = 1;
  while (argindex < argc && StrStartWith(argv[argindex], "-"))
  {
    if (strcmp(argv[argindex], "-load") == 0)
    {
      props.SetProperty("doload", "true");
      argindex++;
    }
    else if (strcmp(argv[argindex], "-run") == 0 || strcmp(argv[argindex], "-t") == 0)
    {
      props.SetProperty("dotransaction", "true");
      argindex++;
    }
    else if (strcmp(argv[argindex], "-threads") == 0)
    {
      argindex++;
      if (argindex >= argc)
      {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -threads" << std::endl;
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    }
    else if (strcmp(argv[argindex], "-target_rates") == 0)
    {
      argindex++;
      if (argindex >= argc)
      {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -target_rates" << std::endl;
        exit(0);
      }
      props.SetProperty("target_rates", argv[argindex]);
      argindex++;
    }
    else if (strcmp(argv[argindex], "-db") == 0)
    {
      argindex++;
      if (argindex >= argc)
      {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -db" << std::endl;
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    }
    else if (strcmp(argv[argindex], "-P") == 0)
    {
      argindex++;
      if (argindex >= argc)
      {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -P" << std::endl;
        exit(0);
      }
      std::string filename(argv[argindex]);
      std::ifstream input(argv[argindex]);
      try
      {
        props.Load(input);
      }
      catch (const std::string &message)
      {
        std::cerr << message << std::endl;
        exit(0);
      }
      input.close();
      argindex++;
    }
    else if (strcmp(argv[argindex], "-p") == 0)
    {
      argindex++;
      if (argindex >= argc)
      {
        UsageMessage(argv[0]);
        std::cerr << "Missing argument value for -p" << std::endl;
        exit(0);
      }
      std::string prop(argv[argindex]);
      size_t eq = prop.find('=');
      if (eq == std::string::npos)
      {
        std::cerr << "Argument '-p' expected to be in key=value format "
                     "(e.g., -p operationcount=99999)"
                  << std::endl;
        exit(0);
      }
      props.SetProperty(ycsbc::utils::Trim(prop.substr(0, eq)),
                        ycsbc::utils::Trim(prop.substr(eq + 1)));
      argindex++;
    }
    else if (strcmp(argv[argindex], "-s") == 0)
    {
      props.SetProperty("status", "true");
      argindex++;
    }
    else
    {
      UsageMessage(argv[0]);
      std::cerr << "Unknown option '" << argv[argindex] << "'" << std::endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc)
  {
    UsageMessage(argv[0]);
    exit(0);
  }
}

void UsageMessage(const char *command)
{
  std::cout << "Usage: " << command << " [options]\n"
                                       "Options:\n"
                                       "  -load: run the loading phase of the workload\n"
                                       "  -t: run the transactions phase of the workload\n"
                                       "  -run: same as -t\n"
                                       "  -threads n: execute using n threads (default: 1)\n"
                                       "  -db dbname: specify the name of the DB to use (default: basic)\n"
                                       "  -P propertyfile: load properties from the given file. Multiple files can\n"
                                       "                   be specified, and will be processed in the order specified\n"
                                       "  -p name=value: specify a property to be passed to the DB and workloads\n"
                                       "                 multiple properties can be specified, and override any\n"
                                       "                 values in the propertyfile\n"
                                       "  -s: print status every 10 seconds (use status.interval prop to override)"
            << std::endl;
}

inline bool StrStartWith(const char *str, const char *pre)
{
  return strncmp(str, pre, strlen(pre)) == 0;
}
