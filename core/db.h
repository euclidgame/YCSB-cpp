//
//  db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_DB_H_
#define YCSB_C_DB_H_

#include "utils/properties.h"
#include "utils/resources.h"

#include <vector>
#include <string>
#include <memory>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
namespace ycsbc
{

  ///
  /// Database interface layer.
  /// per-thread DB instance.
  ///
  class DB
  {
  public:
    struct Field
    {
      std::string name;
      std::string value;
    };
    enum Status
    {
      kOK = 0,
      kError,
      kNotFound,
      kNotImplemented
    };
    ///
    /// Initializes any state for accessing this DB.
    ///
    virtual void Init() {}
    ///
    /// Clears any state for accessing this DB.
    ///
    virtual void Cleanup() {}
    ///
    /// Reads a record from the database.
    /// Field/value pairs from the result are stored in a vector.
    ///
    /// @param table The name of the table.
    /// @param key The key of the record to read.
    /// @param fields The list of fields to read, or NULL for all of them.
    /// @param result A vector of field/value pairs for the result.
    /// @return Zero on success, or a non-zero error code on error/record-miss.
    ///
    virtual Status Read(const std::string &table, const std::string &key,
                        const std::vector<std::string> *fields,
                        std::vector<Field> &result, int client_id = 0) = 0;
    ///
    /// Performs a range scan for a set of records in the database.
    /// Field/value pairs from the result are stored in a vector.
    ///
    /// @param table The name of the table.
    /// @param key The key of the first record to read.
    /// @param record_count The number of records to read.
    /// @param fields The list of fields to read, or NULL for all of them.
    /// @param result A vector of vector, where each vector contains field/value
    ///        pairs for one record
    /// @return Zero on success, or a non-zero error code on error.
    ///
    virtual Status Scan(const std::string &table, const std::string &key,
                        int record_count, const std::vector<std::string> *fields,
                        std::vector<std::vector<Field>> &result, int client_id = 0) = 0;
    ///
    /// Updates a record in the database.
    /// Field/value pairs in the specified vector are written to the record,
    /// overwriting any existing values with the same field names.
    ///
    /// @param table The name of the table.
    /// @param key The key of the record to write.
    /// @param values A vector of field/value pairs to update in the record.
    /// @return Zero on success, a non-zero error code on error.
    ///
    virtual Status Update(const std::string &table, const std::string &key,
                          std::vector<Field> &values, int client_id = 0) = 0;
    ///
    /// Inserts a record into the database.
    /// Field/value pairs in the specified vector are written into the record.
    ///
    /// @param table The name of the table.
    /// @param key The key of the record to insert.
    /// @param values A vector of field/value pairs to insert in the record.
    /// @return Zero on success, a non-zero error code on error.
    ///
    virtual Status Insert(const std::string &table, const std::string &key,
                          std::vector<Field> &values, long long start_time_ns = 0, int client_id = 0) = 0;
    ///
    /// Deletes a record from the database.
    ///
    /// @param table The name of the table.
    /// @param key The key of the record to delete.
    /// @return Zero on success, a non-zero error code on error.
    ///
    virtual Status Delete(const std::string &table, const std::string &key) = 0;

    virtual Status InsertBatch(const std::string &table, int start_key, std::vector<Field> &values, int num_keys, int client_id = 0) = 0;

    virtual void UpdateRateLimit(int client_id, int64_t rate_limit_bytes);
    virtual uint64_t GetCurSizeActiveMemtable(int client_id)
    {
      return 0;
    }
    virtual uint64_t GetNumImmutableMemtable(int client_id)
    {
      return 0;
    }
    virtual void UpdateMemtableSize(int client_id, int memtable_size_bytes);
    virtual void UpdateResourceShares(std::vector<ycsbc::utils::MultiTenantResourceShares> res_opts);
    virtual std::vector<ycsbc::utils::MultiTenantResourceUsage> GetResourceUsage();
    virtual void PrintDbStats() = 0;

    virtual ~DB() {}

    void SetProps(utils::Properties *props)
    {
      props_ = props;
    }

  protected:
    utils::Properties *props_;
  };

} // ycsbc

#endif // YCSB_C_DB_H_
