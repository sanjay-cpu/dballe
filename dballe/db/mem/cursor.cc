#include "cursor.h"
#include "db.h"
#include "dballe/core/record.h"
#include "dballe/core/query.h"
#include <algorithm>
#include <sstream>

using namespace std;
using namespace wreport;

namespace dballe {
namespace db {
namespace mem {
namespace cursor {

namespace {

/**
 * Structure used to build and execute a query, and to iterate through the
 * results
 */
template<typename Interface>
class Base : public Interface
{
protected:
    /// Database to operate on
    mem::DB& db;

    /// Modifier flags to enable special query behaviours
    const unsigned int modifiers;

    /// Number of results still to be fetched
    size_t count = 0;

public:
    virtual ~Base() {}

    dballe::DB& get_db() const override { return db; }
    int remaining() const override { return count; }

    /**
     * Get a new item from the results of a query
     *
     * @returns
     *   true if a new record has been read, false if there is no more data to read
     */
    bool next() override = 0;

    /// Discard the results that have not been read yet
    void discard_rest() override = 0;

#if 0
    /**
     * Iterate the cursor until the end, returning the number of items.
     *
     * If dump is a FILE pointer, also dump the cursor values to it
     */
    virtual unsigned test_iterate(FILE* dump=0) = 0;
#endif

protected:
    /**
     * Create a query cursor
     *
     * @param wanted
     *   The values wanted in output
     * @param modifiers
     *   Optional modifiers to ask for special query behaviours
     */
    Base(mem::DB& db, unsigned modifiers)
        : db(db), modifiers(modifiers)
    {
    }

    void to_record_station(const Station& st, Record& rec)
    {
        rec.set("ana_id", st.ana_id);
        rec.set_coords(st.coords);
        if (st.ident.is_missing())
        {
            rec.unset("ident");
            rec.set("mobile", 0);
        } else {
            rec.set("ident", (const char*)st.ident);
            rec.set("mobile", 1);
        }
        rec.set("rep_memo", st.report);
        rec.set("priority", db.repinfo.get_prio(st.report));
    }

    void to_record_varcode(wreport::Varcode code, Record& rec)
    {
        char bname[7];
        snprintf(bname, 7, "B%02d%03d", WR_VAR_X(code), WR_VAR_Y(code));
        rec.setc("var", bname);
    }

    void to_record_data_value(const DataValues::Ptr& val, Record& rec)
    {
        rec.set(val->first.level);
        rec.set(val->first.trange);
        rec.set(val->first.datetime);
        to_record_varcode(val->first.code, rec);
    }

    /// Query extra station info and add it to \a rec
    void add_station_info(int ana_id, Record& rec)
    {
        db.station_values.fill_record(ana_id, rec);
    }
};


/**
 * Cursor implementation that iterates over a C++ collection
 */
template<typename Interface, typename Results>
struct ResultsCursor : public Base<Interface>
{
    Results results;
    bool first = true;
    typename Results::const_iterator cur;

    ResultsCursor(mem::DB& db, unsigned modifiers, Results&& results)
        : Base<Interface>(db, modifiers), results(move(results))
    {
        this->count = this->results.size();
    }

    bool next() override
    {
        auto& count = this->count;

        if (first)
        {
            cur = results.begin();
            --count;
            first = false;
        }
        else if (cur != results.end())
        {
            ++cur;
            --count;
        }

        return cur != results.end();
    }

    void discard_rest() override
    {
        results.clear();
        cur = results.end();
        this->count = 0;
    }
};


/**
 * CursorStation implementation
 */
struct MemCursorStations : public ResultsCursor<CursorStation, std::set<int>>
{
    using ResultsCursor::ResultsCursor;

    int get_station_id() const override { return *cur; }
    double get_lat() const override { return db.stations[*cur].coords.dlat(); }
    double get_lon() const override { return db.stations[*cur].coords.dlon(); }
    const char* get_ident(const char* def=0) const override
    {
        const Ident& ident = db.stations[*cur].ident;
        if (ident.is_missing())
            return def;
        else
            return (const char*)ident;
    }
    const char* get_rep_memo() const override { return db.stations[*cur].report.c_str(); }

    void to_record(Record& rec)
    {
        this->to_record_station(db.stations[*cur], rec);
        this->add_station_info(*cur, rec);
    }
};


/**
 * CursorStationData implementation
 */
struct MemCursorStationData : public ResultsCursor<CursorStationData, std::vector<StationValues::Ptr>>
{
    using ResultsCursor::ResultsCursor;

    int ana_id() const { return (*this->cur)->first.ana_id; }
    int get_station_id() const override { return ana_id(); }
    double get_lat() const override { return db.stations[ana_id()].coords.dlat(); }
    double get_lon() const override { return db.stations[ana_id()].coords.dlon(); }
    const char* get_ident(const char* def=0) const override
    {
        const Ident& ident = db.stations[ana_id()].ident;
        if (ident.is_missing())
            return def;
        else
            return (const char*)ident;
    }
    const char* get_rep_memo() const override { return db.stations[ana_id()].report.c_str(); }
    wreport::Varcode get_varcode() const override { return (*this->cur)->first.code; }
    wreport::Var get_var() const override
    {
        // Return the variable without its attributes
        const Var& src = db.station_values.variables[(*this->cur)->second];
        Var res(src.info());
        res.setval(src);
        return res;
    }
    int attr_reference_id() const override { return (*this->cur)->second; }

    void to_record(Record& rec)
    {
        this->to_record_station(db.stations[ana_id()], rec);
        this->to_record_varcode(get_varcode(), rec);
        rec.set(get_var());
    }
};

#if 0
template<typename Interface, typename QUEUE>
struct CursorDataBase : public CursorSorted<Interface, QUEUE>
{
    const ValueStorage<memdb::Value>& values;

    CursorDataBase(mem::DB& db, unsigned modifiers, Results<memdb::Value>& res)
        : CursorSorted<Interface, QUEUE>(db, modifiers, res), values(res.values)
    {
    }

    int get_station_id() const override { return values[this->cur]->station.id; }
    double get_lat() const override { return values[this->cur]->station.coords.dlat(); }
    double get_lon() const override { return values[this->cur]->station.coords.dlon(); }
    const char* get_ident(const char* def=0) const override
    {
        if (values[this->cur]->station.mobile)
            return values[this->cur]->station.ident.c_str();
        else
            return def;
    }
    const char* get_rep_memo() const override { return values[this->cur]->station.report.c_str(); }
    Level get_level() const override { return values[this->cur]->levtr.level; }
    Trange get_trange() const override { return values[this->cur]->levtr.trange; }
    wreport::Varcode get_varcode() const override { return values[this->cur]->var->code(); }
    Datetime get_datetime() const override { return values[this->cur]->datetime; }
    wreport::Var get_var() const override
    {
        Var res(values[this->cur]->var->info());
        res.setval(*values[this->cur]->var);
        return res;
    }
    int attr_reference_id() const override { return this->cur; }

#if 0
    void attr_insert(const Values& attrs) override
    {
        this->values[this->cur_idx]->attr_insert(attrs);
    }

    void attr_remove(const AttrList& qcs) override
    {
        this->values[this->cur_idx]->attr_remove(qcs);
    }

    void query_attrs(function<void(unique_ptr<Var>&&)> dest) override
    {
        this->cur_value->query_attrs(dest);
    }
#endif

    void to_record(Record& rec) override
    {
        this->to_record_station(values[this->cur]->station, rec);
        rec.clear_vars();
        this->to_record_value(*values[this->cur], rec);
        rec.seti("context_id", this->cur);
    }
};
#endif

namespace {

// Order used by data queries: ana_id, datetime, level, trange, report, var
struct CompareForCursorData
{
    const Stations& stations;

    CompareForCursorData(const Stations& stations) : stations(stations) {}

    bool operator() (const DataValues::Ptr& x, const DataValues::Ptr& y) const
    {
        const DataValue& dvx = x->first;
        const DataValue& dvy = y->first;
        const Station& sx = stations[dvx.ana_id];
        const Station& sy = stations[dvy.ana_id];

        // Compare station data, but not ana_id, because we are aggregating stations with different report
        if (int res = sx.coords.compare(sy.coords)) return res < 0;
        if (int res = sx.ident.compare(sy.ident)) return res < 0;

        if (int res = dvx.datetime.compare(dvy.datetime)) return res < 0;
        if (int res = dvx.level.compare(dvy.level)) return res < 0;
        if (int res = dvx.trange.compare(dvy.trange)) return res < 0;
        if (int res = dvx.code - dvy.code) return res < 0;

        return sx.report < sy.report;
    }
};

}

struct MemCursorData : public ResultsCursor<CursorData, std::vector<DataValues::Ptr>>
{
    MemCursorData(mem::DB& db, unsigned modifiers, std::vector<DataValues::Ptr>&& results)
        : ResultsCursor(db, modifiers, move(results))
    {
        std::sort(this->results.begin(), this->results.end(), CompareForCursorData(db.stations));
    }

    int ana_id() const { return (*this->cur)->first.ana_id; }
    int get_station_id() const override { return ana_id(); }
    double get_lat() const override { return db.stations[ana_id()].coords.dlat(); }
    double get_lon() const override { return db.stations[ana_id()].coords.dlon(); }
    const char* get_ident(const char* def=0) const override
    {
        const Ident& ident = db.stations[ana_id()].ident;
        if (ident.is_missing())
            return def;
        else
            return (const char*)ident;
    }
    const char* get_rep_memo() const override { return db.stations[ana_id()].report.c_str(); }
    Level get_level() const override { return (*this->cur)->first.level; }
    Trange get_trange() const override { return (*this->cur)->first.trange; }
    Datetime get_datetime() const override { return (*this->cur)->first.datetime; }
    wreport::Varcode get_varcode() const override { return (*this->cur)->first.code; }
    wreport::Var get_var() const override
    {
        // Return the variable without its attributes
        const Var& src = db.data_values.variables[(*this->cur)->second];
        Var res(src.info());
        res.setval(src);
        return res;
    }
    int attr_reference_id() const override { return (*this->cur)->second; }

    void to_record(Record& rec)
    {
        this->to_record_station(db.stations[ana_id()], rec);
        rec.clear_vars();
        this->to_record_data_value(*(this->cur), rec);
        rec.set(get_var());
        rec.seti("context_id", (*this->cur)->second);
    }
};

namespace {

struct DataBestKey
{
    const Station* st;
    Datetime datetime;
    Level level;
    Trange trange;
    Varcode code;
    DataBestKey(const Station* st, const DataValues::Ptr& val)
        : st(st), datetime(val->first.datetime),
          level(val->first.level), trange(val->first.trange),
          code(val->first.code)
    {
    }

    int compare(const DataBestKey& v) const
    {
        // Compare station data, but not ana_id, because we are aggregating stations with different report
        if (int res = st->coords.compare(v.st->coords)) return res < 0;
        if (int res = st->ident.compare(v.st->ident)) return res < 0;
        if (int res = datetime.compare(v.datetime)) return res;
        if (int res = level.compare(v.level)) return res;
        if (int res = trange.compare(v.trange)) return res;
        return code - v.code;
    }

    bool operator<(const DataBestKey& v) const { return compare(v) < 0; }
};

std::map<DataBestKey, DataValues::Ptr> best_index(mem::DB& db, const std::vector<DataValues::Ptr>& results)
{
    std::map<DataBestKey, DataValues::Ptr> best;
    for (const auto& val: results)
    {
        const Station& val_st = db.stations[val->first.ana_id];
        DataBestKey key(&val_st, val);
        std::map<DataBestKey, DataValues::Ptr>::iterator i = best.find(key);
        if (i == best.end())
        {
            best.insert(make_pair(key, val));
            continue;
        }
        int prio_existing = db.repinfo.get_prio(db.stations[i->second->first.ana_id].report);
        int prio_new = db.repinfo.get_prio(val_st.report);
        if (prio_new > prio_existing)
            i->second = val;
    }
    return best;
}

}

// Order used by data queries: ana_id, datetime, level, trange, var (and aggregated by report keeping the one with max priority)
struct MemCursorDataBest : public ResultsCursor<CursorData, std::map<DataBestKey, DataValues::Ptr>>
{
    using ResultsCursor::ResultsCursor;

    MemCursorDataBest(mem::DB& db, unsigned modifiers, std::vector<DataValues::Ptr>&& results)
        : ResultsCursor(db, modifiers, best_index(db, results))
    {
    }

    int ana_id() const { return this->cur->second->first.ana_id; }
    int data_id() const { return this->cur->second->second; }
    int get_station_id() const override { return ana_id(); }
    double get_lat() const override { return db.stations[ana_id()].coords.dlat(); }
    double get_lon() const override { return db.stations[ana_id()].coords.dlon(); }
    const char* get_ident(const char* def=0) const override
    {
        const Ident& ident = db.stations[ana_id()].ident;
        if (ident.is_missing())
            return def;
        else
            return (const char*)ident;
    }
    const char* get_rep_memo() const override { return db.stations[ana_id()].report.c_str(); }
    Level get_level() const override { return this->cur->first.level; }
    Trange get_trange() const override { return this->cur->first.trange; }
    Datetime get_datetime() const override { return this->cur->first.datetime; }
    wreport::Varcode get_varcode() const override { return this->cur->first.code; }
    wreport::Var get_var() const override
    {
        // Return the variable without its attributes
        const Var& src = db.data_values.variables[data_id()];
        Var res(src.info());
        res.setval(src);
        return res;
    }
    int attr_reference_id() const override { return data_id(); }

    void to_record(Record& rec)
    {
        this->to_record_station(db.stations[ana_id()], rec);
        rec.clear_vars();
        this->to_record_data_value(this->cur->second, rec);
        rec.set(get_var());
        rec.seti("context_id", this->cur->second->second);
    }
};

namespace {

struct SummaryKey
{
    int ana_id;
    Level level;
    Trange trange;
    Varcode code;

    SummaryKey(const DataValues::Ptr& val)
        : ana_id(val->first.ana_id),
          level(val->first.level), trange(val->first.trange),
          code(val->first.code)
    {
    }

    int compare(const SummaryKey& v) const
    {
        if (int res = ana_id - v.ana_id) return res < 0;
        if (int res = level.compare(v.level)) return res;
        if (int res = trange.compare(v.trange)) return res;
        return code - v.code;
    }

    bool operator<(const SummaryKey& v) const { return compare(v) < 0; }
};

struct SummaryStats
{
    size_t count;
    Datetime dtmin;
    Datetime dtmax;

    SummaryStats(const Datetime& dt) : count(1), dtmin(dt), dtmax(dt) {}

    void extend(const Datetime& dt)
    {
        if (count == 0)
        {
            dtmin = dtmax = dt;
        } else {
            if (dt < dtmin)
                dtmin = dt;
            else if (dt > dtmax)
                dtmax = dt;
        }
        ++count;
    }
};

std::map<SummaryKey, SummaryStats> summary_index(const std::vector<DataValues::Ptr>& results)
{
    std::map<SummaryKey, SummaryStats> summary;
    for (const auto& val: results)
    {
        SummaryKey key(val);
        std::map<SummaryKey, SummaryStats>::iterator i = summary.find(key);
        if (i == summary.end())
            summary.insert(make_pair(key, SummaryStats(val->first.datetime)));
        else
            i->second.extend(val->first.datetime);
    }
    return summary;
}

}

struct MemCursorSummary : public ResultsCursor<CursorSummary, std::map<SummaryKey, SummaryStats>>
{
    using ResultsCursor::ResultsCursor;

    MemCursorSummary(mem::DB& db, unsigned modifiers, std::vector<DataValues::Ptr>&& results)
        : ResultsCursor(db, modifiers, summary_index(results))
    {
    }

    int ana_id() const { return this->cur->first.ana_id; }
    int get_station_id() const override { return ana_id(); }
    double get_lat() const override { return db.stations[ana_id()].coords.dlat(); }
    double get_lon() const override { return db.stations[ana_id()].coords.dlon(); }
    const char* get_ident(const char* def=0) const override
    {
        const Ident& ident = db.stations[ana_id()].ident;
        if (ident.is_missing())
            return def;
        else
            return (const char*)ident;
    }
    const char* get_rep_memo() const override { return db.stations[ana_id()].report.c_str(); }
    Level get_level() const override { return this->cur->first.level; }
    Trange get_trange() const override { return this->cur->first.trange; }
    wreport::Varcode get_varcode() const override { return this->cur->first.code; }
    DatetimeRange get_datetimerange() const override { return DatetimeRange(this->cur->second.dtmin, this->cur->second.dtmax); }
    size_t get_count() const override { return this->cur->second.count; }

    void to_record(Record& rec)
    {
        this->to_record_station(db.stations[ana_id()], rec);
        rec.clear_vars();
        rec.set(this->cur->first.level);
        rec.set(this->cur->first.trange);
        to_record_varcode(this->cur->first.code, rec);
        if (modifiers & DBA_DB_MODIFIER_SUMMARY_DETAILS)
        {
            rec.set(get_datetimerange());
            rec.seti("context_id", get_count());
        }
    }
};

#if 0

struct MemCursorSummary : public Base<CursorSummary>
{
    memdb::Summary values;
    memdb::Summary::const_iterator iter_cur;
    memdb::Summary::const_iterator iter_end;
    bool first;

    MemCursorSummary(mem::DB& db, unsigned modifiers, Results<memdb::Value>& res)
        : Base<CursorSummary>(db, modifiers), first(true)
    {
        memdb::Summarizer summarizer(values);
        res.copy_valptrs_to(stl::trivial_inserter(summarizer));

        // Start iterating at the beginning
        iter_cur = values.begin();
        iter_end = values.end();

        // Initialize the result count
        count = values.size();
    }

    int get_station_id() const override { return iter_cur->first.sample.station.id; }
    double get_lat() const override { return iter_cur->first.sample.station.coords.dlat(); }
    double get_lon() const override { return iter_cur->first.sample.station.coords.dlon(); }
    const char* get_ident(const char* def=0) const override
    {
        if (iter_cur->first.sample.station.mobile)
            return iter_cur->first.sample.station.ident.c_str();
        else
            return def;
    }
    const char* get_rep_memo() const override { return iter_cur->first.sample.station.report.c_str(); }
    Level get_level() const override { return iter_cur->first.sample.levtr.level; }
    Trange get_trange() const override { return iter_cur->first.sample.levtr.trange; }
    wreport::Varcode get_varcode() const override { return iter_cur->first.sample.var->code(); }
    DatetimeRange get_datetimerange() const override { return DatetimeRange(iter_cur->second.dtmin, iter_cur->second.dtmax); }
    size_t get_count() const override { return iter_cur->second.count; }

    bool next()
    {
        if (iter_cur == iter_end)
            return false;

        if (first)
            first = false;
        else
            ++iter_cur;

        --count;
        if (iter_cur != iter_end)
        {
#if 0
            cur_value = &(iter_cur->first.sample);
            cur_var = cur_value->var;
            cur_station = &(iter_cur->first.sample.station);
#endif
            return true;
        }
        return false;
    }

    void discard_rest()
    {
        iter_cur = iter_end;
        count = 0;
    }

    void to_record(Record& rec)
    {
        to_record_station(iter_cur->first.sample.station, rec);
        to_record_levtr(iter_cur->first.sample, rec);
        to_record_varcode(iter_cur->first.sample.var->code(), rec);
        if (modifiers & DBA_DB_MODIFIER_SUMMARY_DETAILS)
        {
            rec.set(get_datetimerange());
            rec.seti("context_id", get_count());
        }
    }
};

#endif

}

unique_ptr<db::CursorStation> createStations(mem::DB& db, unsigned modifiers, set<int>&& results)
{
    return unique_ptr<db::CursorStation>(new MemCursorStations(db, modifiers, move(results)));
}

unique_ptr<db::CursorStationData> createStationData(mem::DB& db, unsigned modifiers, std::vector<StationValues::Ptr>&& results)
{
    return unique_ptr<db::CursorStationData>(new MemCursorStationData(db, modifiers, move(results)));
}

unique_ptr<db::CursorData> createData(mem::DB& db, unsigned modifiers, std::vector<DataValues::Ptr>&& results)
{
    return unique_ptr<db::CursorData>(new MemCursorData(db, modifiers, move(results)));
}

unique_ptr<db::CursorData> createDataBest(mem::DB& db, unsigned modifiers, std::vector<DataValues::Ptr>&& results)
{
    return unique_ptr<db::CursorData>(new MemCursorDataBest(db, modifiers, move(results)));
}

unique_ptr<db::CursorSummary> createSummary(mem::DB& db, unsigned modifiers, std::vector<DataValues::Ptr>&& results)
{
    return unique_ptr<db::CursorSummary>(new MemCursorSummary(db, modifiers, move(results)));
}

#if 0
unsigned CursorStations::test_iterate(FILE* dump)
{
    Record r;
    unsigned count;
    for (count = 0; next(); ++count)
    {
        if (dump)
        {
            to_record(r);
            fprintf(dump, "%02d %02.4f %02.4f %-10s\n",
                    r.get(DBA_KEY_ANA_ID, -1),
                    r.get(DBA_KEY_LAT, 0.0),
                    r.get(DBA_KEY_LON, 0.0),
                    r.get(DBA_KEY_IDENT, ""));
        }
    }
    return count;
}

unsigned CursorData::test_iterate(FILE* dump)
{
    Record r;
    unsigned count;
    for (count = 0; next(); ++count)
    {
        if (dump)
        {
/*
            to_record(r);
            fprintf(dump, "%02d %06d %06d %-10s\n",
                    r.get(DBA_KEY_ANA_ID, -1),
                    r.get(DBA_KEY_LAT, 0.0),
                    r.get(DBA_KEY_LON, 0.0),
                    r.get(DBA_KEY_IDENT, ""));
                    */
        }
    }
    return count;
}

void CursorSummary::to_record(Record& rec)
{
    to_record_pseudoana(rec);
    to_record_repinfo(rec);
    //rec.key(DBA_KEY_CONTEXT_ID).seti(sqlrec.out_id_data);
    to_record_varcode(rec);
    to_record_ltr(rec);

    /*
    // Min datetime
    rec.key(DBA_KEY_YEARMIN).seti(sqlrec.out_datetime.year);
    rec.key(DBA_KEY_MONTHMIN).seti(sqlrec.out_datetime.month);
    rec.key(DBA_KEY_DAYMIN).seti(sqlrec.out_datetime.day);
    rec.key(DBA_KEY_HOURMIN).seti(sqlrec.out_datetime.hour);
    rec.key(DBA_KEY_MINUMIN).seti(sqlrec.out_datetime.minute);
    rec.key(DBA_KEY_SECMIN).seti(sqlrec.out_datetime.second);

    // Max datetime
    rec.key(DBA_KEY_YEARMAX).seti(out_datetime_max.year);
    rec.key(DBA_KEY_MONTHMAX).seti(out_datetime_max.month);
    rec.key(DBA_KEY_DAYMAX).seti(out_datetime_max.day);
    rec.key(DBA_KEY_HOURMAX).seti(out_datetime_max.hour);
    rec.key(DBA_KEY_MINUMAX).seti(out_datetime_max.minute);
    rec.key(DBA_KEY_SECMAX).seti(out_datetime_max.second);

    // Abuse id_data and datetime for count and min(datetime)
    rec.key(DBA_KEY_LIMIT).seti(sqlrec.out_id_data);
    */
}

unsigned CursorSummary::test_iterate(FILE* dump)
{
    Record r;
    unsigned count;
    for (count = 0; next(); ++count)
    {
        if (dump)
        {
            to_record(r);
            fprintf(dump, "%02d %03d %03d %s %04d-%02d-%02d %02d:%02d:%02d  %04d-%02d-%02d %02d:%02d:%02d  %d\n",
                    r.get(DBA_KEY_ANA_ID, -1),
                    r.get(DBA_KEY_REP_COD, -1),
                    (int)sqlrec.out_id_ltr,
                    r.get(DBA_KEY_VAR, ""),
                    r.get(DBA_KEY_YEARMIN, 0), r.get(DBA_KEY_MONTHMIN, 0), r.get(DBA_KEY_DAYMIN, 0),
                    r.get(DBA_KEY_HOURMIN, 0), r.get(DBA_KEY_MINUMIN, 0), r.get(DBA_KEY_SECMIN, 0),
                    r.get(DBA_KEY_YEARMAX, 0), r.get(DBA_KEY_MONTHMAX, 0), r.get(DBA_KEY_DAYMAX, 0),
                    r.get(DBA_KEY_HOURMAX, 0), r.get(DBA_KEY_MINUMAX, 0), r.get(DBA_KEY_SECMAX, 0),
                    r.get(DBA_KEY_LIMIT, -1));
        }
    }
    return count;
}
#endif

}
}
}
}
