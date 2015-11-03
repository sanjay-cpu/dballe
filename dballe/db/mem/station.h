#ifndef DBA_DB_MEM_STATION_H
#define DBA_DB_MEM_STATION_H

#include <dballe/types.h>
#include <dballe/core/defs.h>
#include <dballe/core/values.h>
#include <string>
#include <vector>
#include <unordered_set>
#include <cstddef>

namespace dballe {
struct Record;
struct Station;

namespace core {
struct Query;
}

namespace db {
namespace mem {

/// Storage and index for station information
class Stations : public std::vector<Station>
{
public:
    Stations();

    /// Get a Station record
    int obtain(const std::string& report, const Coords& coords, const Ident& ident, bool create=true);

    /// Get a fixed or mobile Station record depending on the data in rec
    int obtain(const Record& rec, bool create=true);

    /// Get a fixed or mobile Station record depending on the data in rec
    int obtain(const dballe::Station& st, bool create=true);

    /// Query stations returning the IDs
    void query(const core::Query& q, std::function<void(int)> dest) const;

    void dump(FILE* out) const;
};

}
}
}

#endif

