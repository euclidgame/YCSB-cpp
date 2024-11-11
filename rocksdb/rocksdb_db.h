//
//  rocksdb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_ROCKSDB_DB_H_
#define YCSB_C_ROCKSDB_DB_H_

#include <string>
#include <mutex>

#include "core/db.h"
#include "utils/properties.h"
#include "utils/resources.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/statistics.h>

namespace ycsbc
{

  class RocksdbDB : public DB
  {
  public:
    RocksdbDB() {}
    ~RocksdbDB() {}

    void Init();

    void Cleanup();

    Status Read(const std::string &table, const std::string &key,
                const std::vector<std::string> *fields, std::vector<Field> &result,
                int client_id = 0)
    {
      return (this->*(method_read_))(table, key, fields, result);
    }

    Status Scan(const std::string &table, const std::string &key, int len,
                const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result, int client_id = 0)
    {
      return (this->*(method_scan_))(table, key, len, fields, result);
    }

    Status Update(const std::string &table, const std::string &key, std::vector<Field> &values, int client_id = 0)
    {
      return (this->*(method_update_))(table, key, values);
    }

    Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values, long long start_time_ns = 0, int client_id = 0)
    {
      return (this->*(method_insert_))(table, key, values);
    }

    Status InsertBatch(const std::string &table, int start_key, std::vector<Field> &values, int num_keys, int client_id = 0)
    {
      return (this->*(method_insert_batch_))(table, start_key, values, num_keys);
    }

    Status Delete(const std::string &table, const std::string &key)
    {
      return (this->*(method_delete_))(table, key);
    }

    void UpdateRateLimit(int client_id, int64_t rate_limit_bytes);
    void UpdateMemtableSize(int client_id, int memtable_size_bytes);
    void UpdateResourceShares(std::vector<ycsbc::utils::MultiTenantResourceShares> res_opts);
    std::vector<ycsbc::utils::MultiTenantResourceUsage> GetResourceUsage();
    uint64_t GetCurSizeActiveMemtable(int client_id) override;
    uint64_t GetNumImmutableMemtable(int client_id) override;

    void PrintDbStats();
    rocksdb::ColumnFamilyHandle *table2handle(const std::string &table);

  private:
    enum RocksFormat
    {
      kSingleRow,
    };
    RocksFormat format_;

    void GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                    std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs);
    void GetCfOptions(const utils::Properties &props,
                      std::vector<rocksdb::ColumnFamilyOptions> &cf_opt);
    static void SerializeRow(const std::vector<Field> &values, std::string &data);
    static void DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                     const std::vector<std::string> &fields);
    static void DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                     const std::vector<std::string> &fields);
    static void DeserializeRow(std::vector<Field> &values, const char *p, const char *lim);
    static void DeserializeRow(std::vector<Field> &values, const std::string &data);

    Status ReadSingle(const std::string &table, const std::string &key,
                      const std::vector<std::string> *fields, std::vector<Field> &result);
    Status ScanSingle(const std::string &table, const std::string &key, int len,
                      const std::vector<std::string> *fields,
                      std::vector<std::vector<Field>> &result);
    Status UpdateSingle(const std::string &table, const std::string &key,
                        std::vector<Field> &values);
    Status MergeSingle(const std::string &table, const std::string &key,
                       std::vector<Field> &values);
    Status InsertSingle(const std::string &table, const std::string &key,
                        std::vector<Field> &values);
    Status DeleteSingle(const std::string &table, const std::string &key);
    Status InsertMany(const std::string &table, int start_key,
                      std::vector<Field> &values, int num_keys);

    Status (RocksdbDB::*method_read_)(const std::string &, const std::string &,
                                      const std::vector<std::string> *, std::vector<Field> &);
    Status (RocksdbDB::*method_scan_)(const std::string &, const std::string &,
                                      int, const std::vector<std::string> *,
                                      std::vector<std::vector<Field>> &);
    Status (RocksdbDB::*method_update_)(const std::string &, const std::string &,
                                        std::vector<Field> &);
    Status (RocksdbDB::*method_insert_)(const std::string &, const std::string &,
                                        std::vector<Field> &);
    Status (RocksdbDB::*method_insert_batch_)(const std::string &, int,
                                              std::vector<Field> &, int);
    Status (RocksdbDB::*method_delete_)(const std::string &, const std::string &);

    int fieldcount_;

    static std::vector<rocksdb::ColumnFamilyHandle *> cf_handles_;
    static rocksdb::DB *db_;
    static int ref_cnt_;
    static std::mutex mu_;
  };

  DB *NewRocksdbDB();

} // ycsbc

#endif // YCSB_C_ROCKSDB_DB_H_
