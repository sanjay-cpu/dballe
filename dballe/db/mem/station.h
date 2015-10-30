#ifndef DBA_DB_MEM_STATION_H
#define DBA_DB_MEM_STATION_H

#include <dballe/types.h>
#include <dballe/core/defs.h>
#include <string>
#include <vector>
#include <unordered_set>
#include <cstddef>

namespace dballe {
struct Record;
struct Msg;
struct Station;

namespace core {
struct Query;
}

namespace msg {
struct Context;
}

namespace db {
namespace mem {
template<typename T> struct Results;

/// Station information
struct Station
{
    size_t id;
    Coords coords;
    Ident ident;
    std::string report;

    // Fixed station
    Station(size_t id, const Coords& coords, const std::string& report)
        : id(id), coords(coords), report(report) {}
    Station(size_t id, double lat, double lon, const std::string& report)
        : id(id), coords(lat, lon), report(report) {}

    // Mobile station
    Station(size_t id, const Coords& coords, const char* ident, const std::string& report)
        : id(id), coords(coords), ident(ident), report(report) {}
    Station(size_t id, double lat, double lon, const char* ident, const std::string& report)
        : id(id), coords(lat, lon), ident(ident), report(report) {}

    /**
     * Fill lat, lon, report information, message type (from report) and identifier in msg.
     *
     * Return the station level in msg, so further changes to msg will not need
     * to look it up again.
     */
    msg::Context& fill_msg(Msg& msg) const;

    bool operator<(const Station& o) const { return id < o.id; }
    bool operator>(const Station& o) const { return id > o.id; }
    bool operator==(const Station& o) const { return id == o.id; }
    bool operator!=(const Station& o) const { return id != o.id; }
};

/// Storage and index for station information
class Stations : public std::vector<Station>
{
public:
    Stations();

    /// Get a fixed Station record
    size_t obtain_fixed(const Coords& coords, const std::string& report, bool create=true);

    /// Get a mobile Station record
    size_t obtain_mobile(const Coords& coords, const std::string& ident, const std::string& report, bool create=true);

    /// Get a fixed or mobile Station record depending on the data in rec
    size_t obtain(const Record& rec, bool create=true);

    /// Get a fixed or mobile Station record depending on the data in rec
    size_t obtain(const dballe::Station& st, bool create=true);

    /// Query stations returning the IDs
    std::unordered_set<size_t> query(const core::Query& q) const;

    void dump(FILE* out) const;
};

}
}
}

#endif

