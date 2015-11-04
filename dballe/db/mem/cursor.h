#ifndef DBA_DB_MEM_CURSOR_H
#define DBA_DB_MEM_CURSOR_H

#include <dballe/db/mem/db.h>
#include <set>
#include <iosfwd>

namespace dballe {
struct DB;
struct Record;

namespace db {

namespace mem {
struct DB;

/**
 * Simple typedef to make typing easier, and also to help some versions of swig
 * match this complex type
 */
typedef std::vector<wreport::Varcode> AttrList;


namespace cursor {

std::unique_ptr<db::CursorStation> createStations(mem::DB& db, unsigned modifiers, std::set<int>&& ana_ids);
std::unique_ptr<db::CursorStationData> createStationData(mem::DB& db, unsigned modifiers, std::vector<StationValues::Ptr>&& results);
std::unique_ptr<db::CursorData> createData(mem::DB& db, unsigned modifiers, std::vector<DataValues::Ptr>&& results);
std::unique_ptr<db::CursorData> createDataBest(mem::DB& db, unsigned modifiers, std::vector<DataValues::Ptr>&& results);
std::unique_ptr<db::CursorSummary> createSummary(mem::DB& db, unsigned modifiers, std::vector<DataValues::Ptr>&& results);

}

}
}
}
#endif
