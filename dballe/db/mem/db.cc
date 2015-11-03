#include "db.h"
#include "cursor.h"
#include "dballe/msg/msg.h"
#include "dballe/msg/context.h"
#include "dballe/core/varmatch.h"
#include "dballe/core/record.h"
#include "dballe/core/query.h"
#include "dballe/core/values.h"
#include "dballe/core/defs.h"
#include <algorithm>
#include <queue>

// #define TRACE_SOURCE
#include "dballe/core/trace.h"

using namespace std;
using namespace wreport;

namespace dballe {
namespace db {
namespace mem {

DB::DB() {}

DB::DB(const std::string& arg)
    : serialization_dir(arg)
{
    if (!serialization_dir.empty())
    {
        throw error_unimplemented("deserializing memdb is not implemented");
#if 0
        serialize::CSVReader reader(serialization_dir, memdb);
        reader.read();
#endif
    }
}

DB::~DB()
{
    if (!serialization_dir.empty())
    {
        throw error_unimplemented("serializing memdb is not implemented");
#if 0
        serialize::CSVWriter writer(serialization_dir);
        writer.write(memdb);
        writer.commit();
#endif
    }
}

void DB::disappear()
{
    stations.clear();
    station_values.clear();
    data_values.clear();
}

void DB::reset(const char* repinfo_file)
{
    disappear();
    repinfo.load(repinfo_file);
}

void DB::update_repinfo(const char* repinfo_file, int* added, int* deleted, int* updated)
{
    repinfo.update(repinfo_file, added, deleted, updated);
}

std::map<std::string, int> DB::get_repinfo_priorities()
{
    return repinfo.get_priorities();
}

void DB::insert_station_data(dballe::StationValues& vals, bool can_replace, bool station_can_add)
{
    vals.info.ana_id = MISSING_INT;

    // Obtain the station
    int ana_id = vals.info.ana_id = stations.obtain(vals.info, station_can_add);

    // Insert all the variables we find
    for (auto& i: vals.values)
        i.second.data_id = station_values.insert(*i.second.var, can_replace, ana_id);
}

void DB::insert_data(dballe::DataValues& vals, bool can_replace, bool station_can_add)
{
    vals.info.ana_id = MISSING_INT;

    // Obtain the station
    int ana_id = vals.info.ana_id = stations.obtain(vals.info, station_can_add);

    // Insert all the variables we find
    for (auto& i: vals.values)
        i.second.data_id = data_values.insert(*i.second.var, can_replace, ana_id, vals.info.datetime, vals.info.level, vals.info.trange);
}

void DB::remove_station_data(const Query& query)
{
    throw error_unimplemented("removing station data is not implemented");
#if 0
    Results<StationValue> res(memdb.stationvalues);
    raw_query_station_data(core::Query::downcast(query), res);
    memdb.remove(res);
#endif
}

void DB::remove(const Query& query)
{
    throw error_unimplemented("removing data is not implemented");
#if 0
    Results<Value> res(memdb.values);
    raw_query_data(core::Query::downcast(query), res);
    memdb.remove(res);
#endif
}

void DB::remove_all()
{
    stations.clear();
    station_values.clear();
    data_values.clear();
}

void DB::vacuum()
{
    // Nothing to do
}

void DB::raw_query_stations(const core::Query& q, std::function<void(int)> dest)
{
    // Build a matcher for queries by priority
    const int& priomin = q.prio_min;
    const int& priomax = q.prio_max;
    if (priomin != MISSING_INT || priomax != MISSING_INT)
    {
        // If priomax == priomin, use exact prio instead of
        // min-max bounds
        unordered_set<std::string> report_whitelist;

        if (priomax == priomin)
        {
            std::map<std::string, int> prios = get_repinfo_priorities();
            for (std::map<std::string, int>::const_iterator i = prios.begin();
                    i != prios.end(); ++i)
                if (i->second == priomin)
                    report_whitelist.insert(i->first);
        } else {
            // Here, prio is unset and priomin != priomax

            // Deal with priomin > priomax
            if (priomin != MISSING_INT && priomax != MISSING_INT && priomax < priomin)
                return;

            std::map<std::string, int> prios = get_repinfo_priorities();
            for (std::map<std::string, int>::const_iterator i = prios.begin();
                    i != prios.end(); ++i)
            {
                if (priomin != MISSING_INT && i->second < priomin) continue;
                if (priomax != MISSING_INT && i->second > priomax) continue;
                report_whitelist.insert(i->first);
            }
        }

        // If no report matches the given priority range, there are no results
        // and we are done
        if (report_whitelist.empty())
            return;

        // Chain a filter on report_whitelist
        dest = [this, report_whitelist, &dest](int ana_id) {
            if (report_whitelist.find(stations[ana_id].report) == report_whitelist.end())
                return;
            dest(ana_id);
        };
    }

    unique_ptr<Varmatch> match;
    if (!q.ana_filter.empty())
    {
        match = Varmatch::parse(q.ana_filter);
        Varmatch& vm(*match);
        unique_ptr<Varmatch> match(Varmatch::parse(q.ana_filter));
        dest = [this, &vm, &dest](int ana_id) {
            int data_id = station_values.get(ana_id, vm.code);
            if (data_id == -1) return;
            if (!vm(station_values.variables[ana_id])) return;
            dest(ana_id);
        };
        TRACE("Found ana filter %s\n", q.ana_filter.c_str());
    }
    if (q.block != MISSING_INT)
    {
        int block = q.block;
        dest = [this, block, &dest](int ana_id) {
            int data_id = station_values.get(ana_id, WR_VAR(0, 1, 1));
            if (data_id == -1) return;
            const Var& var = station_values.variables[ana_id];
            if (!var.isset()) return;
            if (var.enqi() != block) return;
            dest(ana_id);
        };
        TRACE("Found block filter B01001=%d\n", block);
    }
    if (q.station != MISSING_INT)
    {
        int station = q.station;
        dest = [this, station, &dest](int ana_id) {
            int data_id = station_values.get(ana_id, WR_VAR(0, 1, 2));
            if (data_id == -1) return;
            const Var& var = station_values.variables[ana_id];
            if (!var.isset()) return;
            if (var.enqi() != station) return;
            dest(ana_id);
        };
        TRACE("Found station filter B01002=%d\n", station);
    }

    stations.query(q, dest);
}

#if 0
void DB::raw_query_station_data(const core::Query& q, memdb::Results<memdb::StationValue>& res)
{
    throw error_unimplemented("querying station data is not implemented");
#if 0
    // Get a list of stations we can match
    Results<memdb::Station> res_st(memdb.stations);

    raw_query_stations(q, res_st);

    memdb.stationvalues.query(q, res_st, res);
#endif
}

void DB::raw_query_data(const core::Query& q, memdb::Results<memdb::Value>& res)
{
    throw error_unimplemented("querying data is not implemented");
#if 0
    // Get a list of stations we can match
    Results<memdb::Station> res_st(memdb.stations);
    raw_query_stations(q, res_st);

    // Get a list of stations we can match
    Results<LevTr> res_tr(memdb.levtrs);
    memdb.levtrs.query(q, res_tr);

    // Query variables
    memdb.values.query(q, res_st, res_tr, res);
#endif
}
#endif

std::unique_ptr<db::CursorStation> DB::query_stations(const Query& query)
{
    const core::Query& q = core::Query::downcast(query);
    unsigned int modifiers = q.get_modifiers();

    std::function<void(int)> dest;
    set<int> result;

    // Build var/varlist special-cased filter for station queries
    if (!q.varcodes.empty())
    {
        const set<Varcode>& varcodes = q.varcodes;

        // Iterate all the possible values, taking note of the stations for
        // variables whose varcodes are in 'varcodes'
        unordered_set<int> id_whitelist;
        for (size_t idx = 0; idx < data_values.variables.size(); ++idx)
        {
            const Var& var = data_values.variables[idx];
            if (!var.isset()) continue;
            if (varcodes.find(var.code()) != varcodes.end())
                id_whitelist.insert(idx);
        }

        IFTRACE {
            TRACE("Found var/varlist station filter: %zd items in id whitelist:", id_whitelist.size());
            for (const auto& i: id_whitelist)
                TRACE(" %d", i);
            TRACE("\n");
        }

        dest = [id_whitelist, &result](int ana_id) {
            if (id_whitelist.find(ana_id) == id_whitelist.end()) return;
            result.insert(ana_id);
        };
    } else {
        dest = [&result](int ana_id) {
            result.insert(ana_id);
        };
    }

    raw_query_stations(q, dest);
    return cursor::createStations(*this, modifiers, move(result));
}

std::unique_ptr<db::CursorStationData> DB::query_station_data(const Query& query)
{
    throw error_unimplemented("querying station data is not implemented");
#if 0
    const core::Query& q = core::Query::downcast(query);
    unsigned int modifiers = q.get_modifiers();
    if (modifiers & DBA_DB_MODIFIER_BEST)
    {
        throw error_unimplemented("best queries of station vars");
#warning TODO
    } else {
        Results<StationValue> res(memdb.stationvalues);
        raw_query_station_data(q, res);
        return cursor::createStationData(*this, modifiers, res);
    }
#endif
}

std::unique_ptr<db::CursorData> DB::query_data(const Query& query)
{
    throw error_unimplemented("querying data is not implemented");
#if 0
    const core::Query& q = core::Query::downcast(query);
    unsigned int modifiers = q.get_modifiers();
    Results<Value> res(memdb.values);
    raw_query_data(q, res);
    if (modifiers & DBA_DB_MODIFIER_BEST)
    {
        return cursor::createDataBest(*this, modifiers, res);
    } else {
        return cursor::createData(*this, modifiers, res);
    }
#endif
}

std::unique_ptr<db::CursorSummary> DB::query_summary(const Query& query)
{
    throw error_unimplemented("querying summaries is not implemented");
#if 0
    const core::Query& q = core::Query::downcast(query);
    unsigned int modifiers = q.get_modifiers();
    Results<Value> res(memdb.values);
    raw_query_data(q, res);
    return cursor::createSummary(*this, modifiers, res);
#endif
}

void DB::attr_query_station(int data_id, std::function<void(std::unique_ptr<wreport::Var>)>&& dest)
{
    throw error_unimplemented("querying station attributes is not implemented");
#if 0
    ValueBase* v = memdb.stationvalues.get_checked(data_id);
    if (!v) error_notfound::throwf("no station variable found with data id %d", data_id);
    v->query_attrs(dest);
#endif
}
void DB::attr_query_data(int data_id, std::function<void(std::unique_ptr<wreport::Var>)>&& dest)
{
    throw error_unimplemented("querying data attributes is not implemented");
#if 0
    ValueBase* v = memdb.values.get_checked(data_id);
    if (!v) error_notfound::throwf("no data variable found with data id %d", data_id);
    v->query_attrs(dest);
#endif
}
void DB::attr_insert_station(int data_id, const dballe::Values& attrs)
{
    throw error_unimplemented("inserting station attributes is not implemented");
#if 0
    ValueBase* v = memdb.stationvalues.get_checked(data_id);
    if (!v) error_notfound::throwf("no station variable found with data id %d", data_id);
    v->attr_insert(attrs);
#endif
}
void DB::attr_insert_data(int data_id, const dballe::Values& attrs)
{
    throw error_unimplemented("inserting data attributes is not implemented");
#if 0
    ValueBase* v = memdb.values.get_checked(data_id);
    if (!v) error_notfound::throwf("no data variable found with data id %d", data_id);
    v->attr_insert(attrs);
#endif
}
void DB::attr_remove_station(int data_id, const db::AttrList& qcs)
{
    throw error_unimplemented("removing station attributes is not implemented");
#if 0
    ValueBase* v = memdb.stationvalues.get_checked(data_id);
    if (!v) error_notfound::throwf("no station variable found with data id %d", data_id);
    v->attr_remove(qcs);
#endif
}
void DB::attr_remove_data(int data_id, const db::AttrList& qcs)
{
    throw error_unimplemented("removing data attributes is not implemented");
#if 0
    ValueBase* v = memdb.values.get_checked(data_id);
    if (!v) error_notfound::throwf("no data variable found with data id %d", data_id);
    v->attr_remove(qcs);
#endif
}

bool DB::is_station_variable(int data_id, wreport::Varcode varcode)
{
    throw error_unimplemented("is_station_variable is not implemented");
#if 0
    // FIXME: this is hackish, and has unexpected results if we have data
    // values and station values with the same id_var and id_data. Giving that
    // measured values are usually different than the station values, the case
    // should be rare enough that we can work around the issue in this way
    // rather than breaking the Fortran API.
    ValueBase* v = memdb.values.get_checked(data_id);
    if (v && v->var->code() == varcode) return false;
    v = memdb.stationvalues.get_checked(data_id);
    if (v && v->var->code() == varcode) return true;
    error_notfound::throwf("variable B%02d%03d not found at data id %d", WR_VAR_X(varcode), WR_VAR_Y(varcode), data_id);
#endif
}

void DB::dump(FILE* out)
{
    fprintf(out, "repinfo data:\n");
    repinfo.dump(out);
    stations.dump(out);
    station_values.dump(out);
    data_values.dump(out);
}

void DB::import_msg(const Message& msg, const char* repmemo, int flags)
{
    throw error_unimplemented("import_msg is not implemented");
#if 0
    memdb.insert(Msg::downcast(msg),
            flags | DBA_IMPORT_OVERWRITE,
            flags | DBA_IMPORT_FULL_PSEUDOANA,
            flags | DBA_IMPORT_ATTRS,
            repmemo);
#endif
}

#if 0
namespace {

struct CompareForExport
{
    // Return an inverse comparison, so that the priority queue gives us the
    // smallest items first
    bool operator() (const Value* x, const Value* y) const
    {
        // Compare station and report
        if (x->station.id < y->station.id) return false;
        if (x->station.id > y->station.id) return true;
        // Compare datetime
        return x->datetime > y->datetime;
    }
};

}
#endif

bool DB::export_msgs(const Query& query_gen, std::function<bool(std::unique_ptr<Message>&&)> dest)
{
    throw error_unimplemented("export_msgs is not implemented");
#if 0
    const core::Query& query = core::Query::downcast(query_gen);
    Results<Value> res(memdb.values);
    raw_query_data(query, res);

    // Sorted value IDs
    priority_queue<const Value*, vector<const Value*>, CompareForExport> values;
    res.copy_valptrs_to(stl::pusher(values));

    TRACE("export_msgs: %zd values in priority queue\n", values.size());

    // Message being built
    unique_ptr<Msg> msg;

    // Last value seen, used to detect when we can move on to the next message
    const Value* old_val = 0;

    // Iterate all results, sorted
    for ( ; !values.empty(); values.pop())
    {
        const Value* val = values.top();
        TRACE("Got %zd %04d-%02d-%02d %02d:%02d:%02d B%02d%03d %d,%d, %d,%d %d,%d,%d %s\n",
                val->station.id,
                (int)val->datetime.date.year, (int)val->datetime.date.month, (int)val->datetime.date.day,
                (int)val->datetime.time.hour, (int)val->datetime.time.minute, (int)val->datetime.time.second,
                WR_VAR_X(val->var->code()), WR_VAR_Y(val->var->code()),
                val->levtr.level.ltype1, val->levtr.level.l1, val->levtr.level.ltype2, val->levtr.level.l2,
                val->levtr.trange.pind, val->levtr.trange.p1, val->levtr.trange.p2,
                val->var->value());

        // See if we have the start of a new message
        if (!old_val || old_val->station.id != val->station.id ||
                old_val->datetime != val->datetime)
        {
            // Flush current message
            TRACE("New message\n");
            if (msg.get() != NULL)
            {
                //TRACE("Sending old message to consumer\n");
                if (msg->type == MSG_PILOT || msg->type == MSG_TEMP || msg->type == MSG_TEMP_SHIP)
                {
                    unique_ptr<Msg> copy(new Msg);
                    msg->sounding_pack_levels(*copy);
                    if (!dest(move(copy)))
                        return false;
                } else
                    if (!dest(move(msg)))
                        return false;
            }

            // Start writing a new message
            msg.reset(new Msg);

            // Fill datetime
            msg->set_datetime(val->datetime);

            // Fill station info
            msg::Context& c_st = val->station.fill_msg(*msg);

            // Fill station vars
            memdb.stationvalues.fill_msg(val->station, c_st);

            // Update last value seen info
            old_val = val;
        }

        TRACE("Inserting var B%02d%03d (%s)\n", WR_VAR_X(val->var->code()), WR_VAR_Y(val->var->code()), val->var->value());
        msg::Context& ctx = msg->obtain_context(val->levtr.level, val->levtr.trange);
        ctx.set(*val->var);
    }

    if (msg.get() != NULL)
    {
        TRACE("Inserting leftover old message\n");
        if (msg->type == MSG_PILOT || msg->type == MSG_TEMP || msg->type == MSG_TEMP_SHIP)
        {
            unique_ptr<Msg> copy(new Msg);
            msg->sounding_pack_levels(*copy);
            if (!dest(move(copy)))
                return false;
        } else
            if (!dest(move(msg)))
                return false;
    }
    return true;
#endif
}

}
}
}
