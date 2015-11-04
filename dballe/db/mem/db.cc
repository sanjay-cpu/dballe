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
    // Obtain the station
    int ana_id = vals.info.ana_id = stations.obtain(vals.info, station_can_add);

    // Insert all the variables we find
    for (auto& i: vals.values)
        i.second.data_id = station_values.insert(*i.second.var, can_replace, ana_id);
}

void DB::insert_data(dballe::DataValues& vals, bool can_replace, bool station_can_add)
{
    // Obtain the station
    int ana_id = vals.info.ana_id = stations.obtain(vals.info, station_can_add);

    // Insert all the variables we find
    for (auto& i: vals.values)
        i.second.data_id = data_values.insert(*i.second.var, can_replace, ana_id, vals.info.datetime, vals.info.level, vals.info.trange);
}

void DB::remove_station_data(const Query& query)
{
    const core::Query& q = core::Query::downcast(query);
    std::vector<StationValues::Ptr> results;
    raw_query_station_data(q, [&results](StationValues::Ptr i) { results.push_back(i); });
    for (const auto& v: results)
        station_values.remove(v);
}

void DB::remove(const Query& query)
{
    const core::Query& q = core::Query::downcast(query);
    std::vector<DataValues::Ptr> results;
    raw_query_data(q, [&results](DataValues::Ptr i) { results.push_back(i); });
    for (const auto& v: results)
        data_values.remove(v);
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
        dest = [this, report_whitelist, dest](int ana_id) {
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
        dest = [this, &vm, dest](int ana_id) {
            int data_id = station_values.get(vm.code, ana_id);
            if (data_id == -1) return;
            if (!vm(station_values.variables[data_id])) return;
            dest(ana_id);
        };
        TRACE("Found ana filter %s\n", q.ana_filter.c_str());
    }
    if (q.block != MISSING_INT)
    {
        int block = q.block;
        dest = [this, block, dest](int ana_id) {
            int data_id = station_values.get(WR_VAR(0, 1, 1), ana_id);
            if (data_id == -1) return;
            const Var& var = station_values.variables[data_id];
            if (!var.isset()) return;
            if (var.enqi() != block) return;
            dest(ana_id);
        };
        TRACE("Found block filter B01001=%d\n", block);
    }
    if (q.station != MISSING_INT)
    {
        int station = q.station;
        dest = [this, station, dest](int ana_id) {
            int data_id = station_values.get(WR_VAR(0, 1, 2), ana_id);
            if (data_id == -1) return;
            const Var& var = station_values.variables[data_id];
            if (!var.isset()) return;
            if (var.enqi() != station) return;
            dest(ana_id);
        };
        TRACE("Found station filter B01002=%d\n", station);
    }

    stations.query(q, dest);
}

namespace {

bool query_selects_all_stations(const core::Query& q)
{
    if (!Stations::query_selects_all(q)) return false;
    if (q.prio_min != MISSING_INT || q.prio_max != MISSING_INT) return false;
    if (!q.ana_filter.empty()) return false;
    if (q.block != MISSING_INT) return false;
    if (q.station != MISSING_INT) return false;
    return true;
}

}

void DB::raw_query_station_data(const core::Query& q, std::function<void(StationValues::Ptr)> dest)
{
    dest = station_values.wrap_filter(q, dest);

    if (query_selects_all_stations(q))
    {
        for (StationValues::Ptr i = station_values.values.begin(); i != station_values.values.end(); ++i)
            dest(i);
    } else {
        raw_query_stations(q, [this, &dest](int ana_id) {
            station_values.query(ana_id, dest);
        });
    }
}

void DB::raw_query_data(const core::Query& q, std::function<void(DataValues::Ptr)> dest)
{
    dest = data_values.wrap_filter(q, dest);

    if (q.datetime.is_missing())
    {
        if (query_selects_all_stations(q))
        {
            for (DataValues::Ptr i = data_values.values.begin(); i != data_values.values.end(); ++i)
                dest(i);
        } else {
            raw_query_stations(q, [this, &dest](int ana_id) {
                data_values.query(ana_id, dest);
            });
        }
    } else {
        if (query_selects_all_stations(q))
        {
            for (unsigned ana_id = 0; ana_id < stations.size(); ++ana_id)
                data_values.query(ana_id, q.datetime, dest);
        } else {
            raw_query_stations(q, [this, &q, &dest](int ana_id) {
                data_values.query(ana_id, q.datetime, dest);
            });
        }
    }
}

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
    const core::Query& q = core::Query::downcast(query);
    unsigned int modifiers = q.get_modifiers();
    if (modifiers & DBA_DB_MODIFIER_BEST)
    {
        throw error_unimplemented("best queries of station vars");
#warning TODO
    } else {
        std::vector<StationValues::Ptr> results;
        raw_query_station_data(q, [&results](StationValues::Ptr i) { results.push_back(i); });
        return cursor::createStationData(*this, modifiers, move(results));
    }
}

std::unique_ptr<db::CursorData> DB::query_data(const Query& query)
{
    const core::Query& q = core::Query::downcast(query);
    unsigned int modifiers = q.get_modifiers();
    std::vector<DataValues::Ptr> results;
    raw_query_data(q, [&results](DataValues::Ptr i) { results.push_back(i); });
    if (modifiers & DBA_DB_MODIFIER_BEST)
    {
        return cursor::createDataBest(*this, modifiers, move(results));
    } else {
        return cursor::createData(*this, modifiers, move(results));
    }
}

std::unique_ptr<db::CursorSummary> DB::query_summary(const Query& query)
{
    const core::Query& q = core::Query::downcast(query);
    unsigned int modifiers = q.get_modifiers();
    std::vector<DataValues::Ptr> results;
    raw_query_data(q, [&results](DataValues::Ptr i) { results.push_back(i); });
    return cursor::createSummary(*this, modifiers, move(results));
}

void DB::attr_query_station(int data_id, std::function<void(std::unique_ptr<wreport::Var>)>&& dest)
{
    station_values.query_attrs(data_id, dest);
}
void DB::attr_query_data(int data_id, std::function<void(std::unique_ptr<wreport::Var>)>&& dest)
{
    data_values.query_attrs(data_id, dest);
}
void DB::attr_insert_station(int data_id, const dballe::Values& attrs)
{
    station_values.attr_insert(data_id, attrs);
}
void DB::attr_insert_data(int data_id, const dballe::Values& attrs)
{
    data_values.attr_insert(data_id, attrs);
}
void DB::attr_remove_station(int data_id, const db::AttrList& qcs)
{
    station_values.attr_remove(data_id, qcs);
}
void DB::attr_remove_data(int data_id, const db::AttrList& qcs)
{
    data_values.attr_remove(data_id, qcs);
}

bool DB::is_station_variable(int data_id, wreport::Varcode varcode)
{
    // FIXME: this is hackish, and has unexpected results if we have data
    // values and station values with the same id_var and id_data. Giving that
    // measured values are usually different than the station values, the case
    // should be rare enough that we can work around the issue in this way
    // rather than breaking the Fortran API.

    if (data_id < 0)
        error_notfound::throwf("variable B%02d%03d not found at data id %d", WR_VAR_X(varcode), WR_VAR_Y(varcode), data_id);

    if ((unsigned)data_id < station_values.variables.size() && station_values.variables[data_id].code() == varcode)
        return true;

    if ((unsigned)data_id < data_values.variables.size() && data_values.variables[data_id].code() == varcode)
        return false;

    error_notfound::throwf("variable B%02d%03d not found at data id %d", WR_VAR_X(varcode), WR_VAR_Y(varcode), data_id);
}

void DB::dump(FILE* out)
{
    fprintf(out, "repinfo data:\n");
    repinfo.dump(out);
    stations.dump(out);
    station_values.dump(out);
    data_values.dump(out);
}

void DB::import_msg(const Message& msg_gen, const char* force_report, int flags)
{
    const Msg& msg = Msg::downcast(msg_gen);
    bool replace = flags | DBA_IMPORT_OVERWRITE;
    bool with_station_info = flags | DBA_IMPORT_FULL_PSEUDOANA;
    bool with_attrs = flags | DBA_IMPORT_ATTRS;

    const msg::Context* l_ana = msg.find_context(Level(), Trange());
    if (!l_ana)
        throw error_consistency("cannot import into the database a message without station information");

    // Coordinates
    Coords coord;
    if (const Var* var = l_ana->find_by_id(DBA_MSG_LATITUDE))
        coord.lat = var->enqi();
    else
        throw error_notfound("latitude not found in data to import");
    if (const Var* var = l_ana->find_by_id(DBA_MSG_LONGITUDE))
        coord.lon = var->enqi();
    else
        throw error_notfound("longitude not found in data to import");

    // Report code
    string report;
    if (force_report != NULL)
        report = force_report;
    else if (const Var* var = msg.get_rep_memo_var())
        report = var->enqc();
    else
        report = Msg::repmemo_from_type(msg.type);

    int station_id;
    if (const Var* var = l_ana->find_by_id(DBA_MSG_IDENT))
    {
        // Mobile station
        station_id = stations.obtain(report, coord, var->enqc(), true);
    }
    else
    {
        // Fixed station
        station_id = stations.obtain(report, coord, Ident(), true);
    }

    //const Station& station = stations[station_id];

    if (with_station_info || station_values.has_variables_for(station_id))
    {
        // Insert the rest of the station information
        for (const auto& srcvar : l_ana->data)
        {
            Varcode code = srcvar->code();
            // Do not import datetime in the station info context
            if (code >= WR_VAR(0, 4, 1) && code <= WR_VAR(0, 4, 6))
                continue;

            unique_ptr<Var> var;
            if (with_attrs)
                var.reset(new Var(*srcvar));
            else
            {
                var.reset(new Var(srcvar->info()));
                var->setval(*srcvar);
            }
            station_values.insert(std::move(var), replace, station_id);
        }
    }

    // Fill up the common context information for the rest of the data

    // Date and time
    if (msg.get_datetime().is_missing())
        throw error_notfound("date/time informations not found (or incomplete) in message to insert");

    // Insert the rest of the data
    for (size_t i = 0; i < msg.data.size(); ++i)
    {
        const msg::Context& ctx = *msg.data[i];
        bool is_ana_level = ctx.level == Level() && ctx.trange == Trange();
        // Skip the station info level
        if (is_ana_level) continue;

        for (const auto& srcvar : ctx.data)
        {
            if (not srcvar->isset()) continue;

            unique_ptr<Var> var;
            if (with_attrs)
                var.reset(new Var(*srcvar));
            else
            {
                var.reset(new Var(srcvar->info()));
                var->setval(*srcvar);
            }
            data_values.insert(std::move(var), replace, station_id, msg.get_datetime(), ctx.level, ctx.trange);
        }
    }
}

namespace {

struct CompareForExport
{
    bool operator() (const DataValues::Ptr& x, const DataValues::Ptr& y) const
    {
        // Compare station and report
        if (x->first.ana_id < y->first.ana_id) return true;
        if (x->first.ana_id > y->first.ana_id) return false;
        // Compare datetime
        return x->first.datetime < y->first.datetime;
    }
};

}

bool DB::export_msgs(const Query& query, std::function<bool(std::unique_ptr<Message>&&)> dest)
{
    const core::Query& q = core::Query::downcast(query);
    std::vector<DataValues::Ptr> results;
    raw_query_data(q, [&results](DataValues::Ptr i) { results.push_back(i); });

    // Sort results so that data for a single message are clustered together
    std::sort(results.begin(), results.end(), CompareForExport());

    TRACE("export_msgs: %zd values in priority queue\n", values.size());

    // Message being built
    unique_ptr<Msg> msg;

    // Last value seen, used to detect when we can move on to the next message
    DataValues::Ptr old_val = data_values.values.end();

    // Iterate all results, sorted
    for (const auto& val: results)
    {
        TRACE("Got %zd %04d-%02d-%02d %02d:%02d:%02d B%02d%03d %d,%d, %d,%d %d,%d,%d %s\n",
                val.first.ana_id,
                (int)val->first.datetime.date.year, (int)val->first.datetime.date.month, (int)val->first.datetime.date.day,
                (int)val->first.datetime.time.hour, (int)val->first.datetime.time.minute, (int)val->first.datetime.time.second,
                WR_VAR_X(val->var->code()), WR_VAR_Y(val->var->code()),
                val->levtr.level.ltype1, val->levtr.level.l1, val->levtr.level.ltype2, val->levtr.level.l2,
                val->levtr.trange.pind, val->levtr.trange.p1, val->levtr.trange.p2,
                val->var->value());

        // See if we have the start of a new message
        if (old_val == data_values.values.end() || old_val->first.ana_id != val->first.ana_id ||
                old_val->first.datetime != val->first.datetime)
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
            msg->set_datetime(val->first.datetime);

            // Fill station info
            msg::Context& c_st = stations.fill_msg(val->first.ana_id, *msg);

            // Fill station vars
            station_values.fill_msg(val->first.ana_id, c_st);

            // Update last value seen info
            old_val = val;
        }

        TRACE("Inserting var B%02d%03d (%s)\n", WR_VAR_X(val->var->code()), WR_VAR_Y(val->var->code()), val->var->value());
        msg::Context& ctx = msg->obtain_context(val->first.level, val->first.trange);
        ctx.set(data_values.variables[val->second]);
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
}

}
}
}
