//
//  db_wrapper.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_DB_WRAPPER_H_
#define YCSB_C_DB_WRAPPER_H_

#include <string>
#include <vector>

#include "db.h"
#include "measurements.h"
#include "utils/timer.h"
#include "utils/utils.h"

namespace ycsbc {

class DBWrapper : public DB {
 public:
  DBWrapper(DB *db, Measurements *measurements) : db_(db), measurements_(measurements) {}
  DBWrapper(DB *db, Measurements *measurements, std::vector<Measurements*> per_client_measurements) : db_(db), measurements_(measurements), per_client_measurements_(per_client_measurements) {}
  ~DBWrapper() {
    delete db_;
  }
  void Init() {
    db_->Init();
  }
  void Cleanup() {
    db_->Cleanup();
  }
  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result,
              int client_id) {
    timer_.Start();
    Status s = db_->Read(table, key, fields, result);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(READ, elapsed);
      per_client_measurements_[client_id]->Report(READ, elapsed);
    } else {
      measurements_->Report(READ_FAILED, elapsed);
      per_client_measurements_[client_id]->Report(READ_FAILED, elapsed);
    }
    return s;
  }
  Status Scan(const std::string &table, const std::string &key, int record_count,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    timer_.Start();
    Status s = db_->Scan(table, key, record_count, fields, result);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(SCAN, elapsed);
    } else {
      measurements_->Report(SCAN_FAILED, elapsed);
    }
    return s;
  }
  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
    timer_.Start();
    Status s = db_->Update(table, key, values);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(UPDATE, elapsed);
    } else {
      measurements_->Report(UPDATE_FAILED, elapsed);
    }
    return s;
  }
  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
    timer_.Start();
    Status s = db_->Insert(table, key, values);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(INSERT, elapsed);
    } else {
      measurements_->Report(INSERT_FAILED, elapsed);
    }
    return s;
  }
  Status Delete(const std::string &table, const std::string &key) {
    timer_.Start();
    Status s = db_->Delete(table, key);
    uint64_t elapsed = timer_.End();
    if (s == kOK) {
      measurements_->Report(DELETE, elapsed);
    } else {
      measurements_->Report(DELETE_FAILED, elapsed);
    }
    return s;
  }
 private:
  DB *db_;
  Measurements *measurements_;
  std::vector<Measurements*> per_client_measurements_;
  utils::Timer<uint64_t, std::nano> timer_;
};

} // ycsbc

#endif // YCSB_C_DB_WRAPPER_H_
