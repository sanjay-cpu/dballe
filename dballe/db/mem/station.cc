#include "station.h"
#include "dballe/record.h"
#include "dballe/core/query.h"
#include "dballe/core/stlutils.h"
#include "dballe/core/values.h"
#include "dballe/msg/msg.h"
#include "dballe/msg/context.h"
#include "dballe/db/trace.h"
#include <algorithm>
#include <iostream>
#include <cstdlib>

using namespace std;
using namespace wreport;

namespace dballe {
namespace db {
namespace mem {

msg::Context& Station::fill_msg(Msg& msg) const
{
    msg::Context& c_st = msg.obtain_station_context();

    // Fill in report information
    msg.type = Msg::type_from_repmemo(report.c_str());
    c_st.set_rep_memo(report.c_str());

    // Fill in the basic station values
    c_st.seti(WR_VAR(0, 5, 1), coords.lat);
    c_st.seti(WR_VAR(0, 6, 1), coords.lon);
    if (!ident.is_missing())
        c_st.set_ident(ident);

    return c_st;
}

Stations::Stations() {}

size_t Stations::obtain_fixed(const Coords& coords, const std::string& report, bool create)
{
    // Search
    for (const auto& s: *this)
    {
        if (s.coords == coords && s.report == report && s.ident.is_missing())
            return s.id;
    }

    if (!create)
        error_notfound::throwf("%s station not found at %f,%f", report.c_str(), coords.dlat(), coords.dlon());

    // Station not found, create it
    size_t pos = size();
    emplace_back(pos, coords, report);

    // And return it
    return pos;
}

size_t Stations::obtain_mobile(const Coords& coords, const std::string& ident, const std::string& report, bool create)
{
    // Search
    for (const auto& s: *this)
    {
        if (s.coords == coords && s.report == report && s.ident == ident)
            return s.id;
    }

    if (!create)
        error_notfound::throwf("%s station %s not found at %f,%f", report.c_str(), ident.c_str(), coords.dlat(), coords.dlon());

    // Station not found, create it
    size_t pos = size();
    emplace_back(pos, coords, ident.c_str(), report);

    // And return it
    return pos;
}

size_t Stations::obtain(const Record& rec, bool create)
{
    // Shortcut by ana_id
    if (const Var* var = rec.get("ana_id"))
    {
        size_t res = var->enqi();
        if (res > size())
            error_notfound::throwf("ana_id %zd is invalid", res);
        return res;
    }

    // Lookup by lat, lon and ident
    int s_lat;
    if (const Var* var = rec.get("lat"))
        s_lat = var->enqi();
    else
        throw error_notfound("record with no latitude, looking up a memdb Station");

    int s_lon;
    if (const Var* var = rec.get("lon"))
        s_lon = var->enqi();
    else
        throw error_notfound("record with no longitude, looking up a memdb Station");

    const char* s_report = nullptr;
    if (const Var* var = rec.get("rep_memo"))
        if (var->isset())
            s_report = var->enqc();
    if (!s_report)
        throw error_notfound("record with no rep_memo, looking up a memdb Station");

    const Var* var_ident = rec.get("ident");
    if (var_ident and var_ident->isset())
        return obtain_mobile(Coords(s_lat, s_lon), var_ident->enqc(), s_report, create);
    else
        return obtain_fixed(Coords(s_lat, s_lon), s_report, create);
}

size_t Stations::obtain(const dballe::Station& st, bool create)
{
    // Shortcut by ana_id
    if (st.ana_id != MISSING_INT)
    {
        if ((unsigned)st.ana_id > size())
            error_notfound::throwf("ana_id %d is invalid", st.ana_id);
        return st.ana_id;
    }

    if (st.ident.is_missing())
        return obtain_fixed(st.coords, st.report, create);
    else
        return obtain_mobile(st.coords, st.ident, st.report, create);
}

std::unordered_set<size_t> Stations::query(const core::Query& q) const
{
    std::function<void(std::function<void(const Station&)>)> generate;

    if (q.ana_id != MISSING_INT)
    {
        //trace_query("Found ana_id %d\n", q.ana_id);
        size_t pos = q.ana_id;
        if (pos >= size())
        {
            //trace_query(" set to empty result set\n");
            return std::unordered_set<size_t>();
        }

        //trace_query(" intersect with %zu\n", pos);
        generate = [=](std::function<void(const Station&)> cons) {
            cons((*this)[pos]);
        };
    } else {
        generate = [=](std::function<void(const Station&)> cons) {
            for (const auto& s: *this)
                cons(s);
        };
    }

    std::unordered_set<size_t> res;
    generate([&](const Station& s) {
        if (!q.rep_memo.empty() && s.report != q.rep_memo) return;
        if (!q.latrange.contains(s.coords.lat)) return;
        if (!q.lonrange.contains(s.coords.lon)) return;
        if (q.mobile != MISSING_INT && (bool)q.mobile == s.ident.is_missing())
            return;
        if (!q.ident.is_missing() && s.ident != q.ident) return;
        res.insert(s.id);
    });
    return res;
}

void Stations::dump(FILE* out) const
{
    fprintf(out, "Stations:\n");
    for (const auto& s: *this)
    {
        fprintf(out, " %4zu %d %d %s %s\n",
                s.id, s.coords.lat, s.coords.lon,
                s.report.c_str(), (const char*)s.ident);
    }
};

}
}
}
