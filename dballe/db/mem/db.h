#ifndef DBA_DB_MEM_H
#define DBA_DB_MEM_H

#include <dballe/db/db.h>
#include <dballe/db/mem/repinfo.h>
#include <dballe/db/mem/station.h>
#include <dballe/db/mem/value.h>
#include <wreport/varinfo.h>
#include <string>
#include <vector>
#include <memory>

namespace dballe {
namespace db {

namespace mem {

/**
 * DB-ALLe database, in-memory db implementation
 */
class DB : public dballe::DB
{
public:
    Repinfo repinfo;
    Stations stations;
    StationValues station_values;
    DataValues data_values;

protected:
    std::string serialization_dir;

    /*
     * Order used by station queries: ana_id
     * Order used by data queries: ana_id, datetime, level, trange, report, var
     * Order used by export queries: ana_id, report, datetime, level, trange, var
     */

    /// Query stations, returning a list of station IDs
    void raw_query_stations(const core::Query& q, std::function<void(int)> dest);

    /// Query station data, returning a list of Value IDs
    void raw_query_station_data(const core::Query& q, std::function<void(std::map<StationValue, int>::const_iterator i)> dest);

#if 0
    /// Query data, returning a list of Value IDs
    void raw_query_data(const core::Query& q, memdb::Results<memdb::Value>& res);
#endif

public:
    DB();
    DB(const std::string& arg);
    virtual ~DB();

    db::Format format() const override { return MEM; }

    void disappear() override ;

    /**
     * Reset the database, removing all existing DBALLE tables and re-creating them
     * empty.
     *
     * @param repinfo_file
     *   The name of the CSV file with the report type information data to load.
     *   The file is in CSV format with 6 columns: report code, mnemonic id,
     *   description, priority, descriptor, table A category.
     *   If repinfo_file is NULL, then the default of /etc/dballe/repinfo.csv is
     *   used.
     */
    void reset(const char* repinfo_file = 0) override;

    /**
     * Update the repinfo table in the database, with the data found in the given
     * file.
     *
     * @param repinfo_file
     *   The name of the CSV file with the report type information data to load.
     *   The file is in CSV format with 6 columns: report code, mnemonic id,
     *   description, priority, descriptor, table A category.
     *   If repinfo_file is NULL, then the default of /etc/dballe/repinfo.csv is
     *   used.
     * @retval added
     *   The number of repinfo entryes that have been added
     * @retval deleted
     *   The number of repinfo entryes that have been deleted
     * @retval updated
     *   The number of repinfo entryes that have been updated
     */
    void update_repinfo(const char* repinfo_file, int* added, int* deleted, int* updated) override;

    std::map<std::string, int> get_repinfo_priorities() override;

    void insert_station_data(dballe::StationValues& vals, bool can_replace, bool station_can_add) override;
    void insert_data(dballe::DataValues& vals, bool can_replace, bool station_can_add) override;

    void remove_station_data(const Query& query) override;
    void remove(const Query& rec) override;
    void remove_all() override;

    /**
     * Remove orphan values from the database.
     *
     * Orphan values are currently:
     * \li lev_tr values for which no data exists
     * \li station values for which no lev_tr exists
     *
     * Depending on database size, this routine can take a few minutes to execute.
     */
    void vacuum() override;

    std::unique_ptr<db::CursorStation> query_stations(const Query& query) override;
    std::unique_ptr<db::CursorStationData> query_station_data(const Query& query) override;
    std::unique_ptr<db::CursorData> query_data(const Query& query) override;
    std::unique_ptr<db::CursorSummary> query_summary(const Query& query) override;

    void attr_query_station(int data_id, std::function<void(std::unique_ptr<wreport::Var>)>&& dest) override;
    void attr_query_data(int data_id, std::function<void(std::unique_ptr<wreport::Var>)>&& dest) override;
    void attr_insert_station(int data_id, const dballe::Values& attrs) override;
    void attr_insert_data(int data_id, const dballe::Values& attrs) override;
    void attr_remove_station(int data_id, const db::AttrList& qcs) override;
    void attr_remove_data(int data_id, const db::AttrList& qcs) override;
    bool is_station_variable(int data_id, wreport::Varcode varcode) override;

    void import_msg(const Message& msg, const char* repmemo, int flags) override;
    bool export_msgs(const Query& query, std::function<bool(std::unique_ptr<Message>&&)> dest) override;

    /**
     * Dump the entire contents of the database to an output stream
     */
    void dump(FILE* out) override;

    friend class dballe::DB;
};

}
}
}
#endif
