#include "station.h"
#include "dballe/record.h"
#include "dballe/core/query.h"
#include "dballe/core/stlutils.h"
#include "dballe/core/values.h"
#include "dballe/db/trace.h"
#include <algorithm>
#include <iostream>
#include <cstdlib>

using namespace std;
using namespace wreport;

namespace dballe {
namespace db {
namespace mem {

#if 0
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
#endif

Stations::Stations() {}

int Stations::obtain(const std::string& report, const Coords& coords, const Ident& ident, bool create)
{
    // Search
    for (const auto& s: *this)
    {
        if (s.coords == coords && s.report == report && s.ident == ident)
            return s.ana_id;
    }

    if (!create)
        error_notfound::throwf("%s station %s not found at %f,%f", report.c_str(), (const char*)ident, coords.dlat(), coords.dlon());

    // Station not found, create it
    int pos = size();
    emplace_back(pos, report, coords, ident);

    // And return it
    return pos;
}

int Stations::obtain(const Record& rec, bool create)
{
    // Shortcut by ana_id
    if (const Var* var = rec.get("ana_id"))
    {
        int res = var->enqi();
        if (res < 0 || (unsigned)res > size())
            error_notfound::throwf("ana_id %d is invalid", res);
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
        return obtain(s_report, Coords(s_lat, s_lon), Ident(var_ident->enqc()), create);
    else
        return obtain(s_report, Coords(s_lat, s_lon), Ident(), create);
}

int Stations::obtain(const dballe::Station& st, bool create)
{
    // Shortcut by ana_id
    if (st.ana_id != MISSING_INT)
    {
        if ((unsigned)st.ana_id > size())
            error_notfound::throwf("ana_id %d is invalid", st.ana_id);
        return st.ana_id;
    }

    return obtain(st.report, st.coords, st.ident, create);
}

void Stations::query(const core::Query& q, std::function<void(int)> dest) const
{
    std::function<void(std::function<void(const Station&)>)> generate;

    if (q.ana_id != MISSING_INT)
    {
        //trace_query("Found ana_id %d\n", q.ana_id);
        int pos = q.ana_id;
        if (pos < 0 || (unsigned)pos >= size())
        {
            //trace_query(" set to empty result set\n");
            return;
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

    generate([&](const Station& s) {
        if (!q.rep_memo.empty() && s.report != q.rep_memo) return;
        if (!q.latrange.contains(s.coords.lat)) return;
        if (!q.lonrange.contains(s.coords.lon)) return;
        if (q.mobile != MISSING_INT && (bool)q.mobile == s.ident.is_missing())
            return;
        if (!q.ident.is_missing() && s.ident != q.ident) return;
        dest(s.ana_id);
    });
}

void Stations::dump(FILE* out) const
{
    fprintf(out, "Stations:\n");
    for (const auto& s: *this)
    {
        fputc(' ', out);
        s.print(out);
    }
};

}
}
}
