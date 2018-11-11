#include "cursor.h"
#include "types.h"
#include <wreport/error.h>

namespace dballe {

namespace {

template<class Interface>
struct EmptyCursor : public Interface
{
    int remaining() const override { return 0; }
    bool next() override { return false; }
    void discard() override {}

#if 0
    bool enqi(const char* key, unsigned len, int& res) const override { return false; }
    bool enqd(const char* key, unsigned len, double& res) const override { return false; }
    bool enqs(const char* key, unsigned len, std::string& res) const override { return false; }
    bool enqf(const char* key, unsigned len, std::string& res) const override { return false; }
#endif
    void to_record(Record& rec) override {}

    DBStation get_station() const override { return DBStation(); }
};

struct EmptyCursorStation : public EmptyCursor<dballe::CursorStation>
{
};

struct EmptyCursorStationData : public EmptyCursor<dballe::CursorStationData>
{
    wreport::Varcode get_varcode() const override { return 0; }
    wreport::Var get_var() const { throw wreport::error_consistency("cursor not on a result"); }
};

struct EmptyCursorData : public EmptyCursor<dballe::CursorData>
{
    wreport::Varcode get_varcode() const override { return 0; }
    wreport::Var get_var() const { throw wreport::error_consistency("cursor not on a result"); }
    Level get_level() const override { return Level(); }
    Trange get_trange() const override { return Trange(); }
    Datetime get_datetime() const override { return Datetime(); }
};

struct EmptyCursorSummary : public EmptyCursor<dballe::CursorSummary>
{
    Level get_level() const override { return Level(); }
    Trange get_trange() const override { return Trange(); }
    wreport::Varcode get_varcode() const override { return 0; }
    DatetimeRange get_datetimerange() const override { return DatetimeRange(); }
    size_t get_count() const override { return 0; }
};

}

std::unique_ptr<CursorStation> CursorStation::make_empty()
{
    return std::unique_ptr<CursorStation>(new EmptyCursorStation);
}

std::unique_ptr<CursorStationData> CursorStationData::make_empty()
{
    return std::unique_ptr<CursorStationData>(new EmptyCursorStationData);
}

std::unique_ptr<CursorData> CursorData::make_empty()
{
    return std::unique_ptr<CursorData>(new EmptyCursorData);
}

std::unique_ptr<CursorSummary> CursorSummary::make_empty()
{
    return std::unique_ptr<CursorSummary>(new EmptyCursorSummary);
}

}