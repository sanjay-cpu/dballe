#ifndef DBALLE_DB_V7_MYSQL_LEVTRV7_H
#define DBALLE_DB_V7_MYSQL_LEVTRV7_H

#include <dballe/db/db.h>
#include <dballe/db/v7/levtr.h>
#include <dballe/sql/fwd.h>
#include <cstdio>
#include <memory>

namespace dballe {
struct Record;
struct Msg;

namespace msg {
struct Context;
}

namespace db {
namespace v7 {
namespace mysql {
struct DB;

/**
 * Precompiled queries to manipulate the lev_tr table
 */
struct MySQLLevTr : public v7::LevTr
{
protected:
    /**
     * DB connection.
     */
    dballe::sql::MySQLConnection& conn;

    void _dump(std::function<void(int, const Level&, const Trange&)> out) override;

public:
    MySQLLevTr(dballe::sql::MySQLConnection& conn);
    MySQLLevTr(const LevTr&) = delete;
    MySQLLevTr(const LevTr&&) = delete;
    MySQLLevTr& operator=(const MySQLLevTr&) = delete;
    ~MySQLLevTr();

    void prefetch_ids(const std::set<int>& ids, std::function<void(int, const LevTrDesc&)> dest) override;
    void prefetch_same_level(int id, std::function<void(int, const LevTrDesc&)> dest) override;
    levtrs_t::iterator lookup_id(State& st, int id) override;
    levtrs_t::iterator obtain_id(State& state, const LevTrDesc& desc) override;
};


}
}
}
}
#endif
