#include "value.h"
#include "dballe/core/query.h"
#include "dballe/core/varmatch.h"
#include <memory>
#include <iomanip>
#include <ostream>
#include <sstream>
#include <cstdlib>
#include <cstring>

using namespace std;
using namespace wreport;

namespace dballe {
namespace db {
namespace mem {

int StationValue::compare(const StationValue& v) const
{
    if (int res = ana_id - v.ana_id) return res;
    return code - v.code;
}

void StationValue::dump(FILE* out) const
{
    stringstream buf;
    buf << ana_id
        << "\t" << varcode_format(code);
    fputs(buf.str().c_str(), out);
}

int DataValue::compare(const DataValue& v) const
{
    if (int res = ana_id - v.ana_id) return res;
    if (int res = level.compare(v.level)) return res;
    if (int res = trange.compare(v.trange)) return res;
    if (int res = datetime.compare(v.datetime)) return res;
    return code - v.code;
}

void DataValue::dump(FILE* out) const
{
    stringstream buf;
    buf << ana_id
        << "\t" << level
        << "\t" << trange
        << "\t" << datetime
        << "\t" << varcode_format(code);
    fputs(buf.str().c_str(), out);
}

std::function<void(StationValues::Ptr)> StationValues::wrap_filter(const core::Query& q, std::function<void(StationValues::Ptr)> dest) const
{
    switch (q.varcodes.size())
    {
        case 0: break;
        case 1:
        {
            Varcode code = *q.varcodes.begin();
            dest = [dest, code](StationValues::Ptr cur) {
                if (cur->first.code != code) return;
                dest(cur);
            };
            break;
        }
        default:
            dest = [dest, &q](StationValues::Ptr cur) {
                if (q.varcodes.find(cur->first.code) == q.varcodes.end()) return;
                dest(cur);
            };
            break;
    }

    if (!q.data_filter.empty())
    {
        std::shared_ptr<Varmatch> data_filter(Varmatch::parse(q.data_filter));
        dest = [this, dest, data_filter](StationValues::Ptr cur) {
            const Var& var = variables[cur->second];
            if (!var.isset()) return;
            if (!(*data_filter)(var)) return;
            dest(cur);
        };
    }

    if (!q.attr_filter.empty())
    {
        std::shared_ptr<Varmatch> attr_filter(Varmatch::parse(q.attr_filter));
        dest = [this, dest, attr_filter](StationValues::Ptr cur) {
            const Var& var = variables[cur->second];
            if (!var.isset()) return;
            const Var* a = var.enqa(attr_filter->code);
            if (!(*attr_filter)(*a)) return;
            dest(cur);
        };
    }

    return dest;
}

void StationValues::query(int ana_id, std::function<void(StationValues::Ptr)> dest) const
{
    StationValues::Ptr cur = values.lower_bound(StationValue(ana_id, 0));
    StationValues::Ptr end = values.upper_bound(StationValue(ana_id, 0xffff));
    for ( ; cur != end; ++cur)
    {
        dest(cur);
    }
}

void StationValues::fill_record(int ana_id, Record& rec)
{
    query(ana_id, [this, &rec](StationValues::Ptr cur) {
        if (!variables[cur->second].isset()) return;
        rec.set(variables[cur->second]);
    });
}

/*
void Values::erase(size_t idx)
{
    const Value& val = *(*this)[idx];
    by_station[&val.station].erase(idx);
    by_levtr[&val.levtr].erase(idx);
    by_date[val.datetime.date()].erase(idx);
    value_remove(idx);
}
*/

#if 0
namespace {

struct MatchDateExact : public Match<Value>
{
    Datetime dt;
    MatchDateExact(const Datetime& dt) : dt(dt) {}
    virtual bool operator()(const Value& val) const
    {
        return val.datetime == dt;
    }
};
struct MatchDateMin : public Match<Value>
{
    Datetime dt;
    MatchDateMin(const Datetime& dt) : dt(dt) {}
    virtual bool operator()(const Value& val) const
    {
        return !(val.datetime < dt);
    }
};
struct MatchDateMax : public Match<Value>
{
    Datetime dt;
    MatchDateMax(const Datetime& dt) : dt(dt) {}
    virtual bool operator()(const Value& val) const
    {
        return !(val.datetime > dt);
    }
};
struct MatchDateMinMax : public Match<Value>
{
    Datetime dtmin;
    Datetime dtmax;
    MatchDateMinMax(const Datetime& dtmin, const Datetime& dtmax) : dtmin(dtmin), dtmax(dtmax) {}
    virtual bool operator()(const Value& val) const
    {
        return !(dtmin > val.datetime) && !(val.datetime > dtmax);
    }
};


}

void Values::query(const core::Query& q, Results<Station>& stations, Results<LevTr>& levtrs, Results<Value>& res) const
{
    if (q.data_id != MISSING_INT)
    {
        trace_query("Found data_id %d\n", q.data_id);
        size_t pos = q.data_id;
        if (pos >= 0 && pos < values.size() && values[pos])
        {
            trace_query(" intersect with %zu\n", pos);
            res.add_singleton(pos);
        } else {
            trace_query(" set to empty result set\n");
            res.set_to_empty();
            return;
        }
    }

    if (!stations.is_select_all())
    {
        trace_query("Adding selected stations to strategy\n");
        match::SequenceBuilder<Station> lookup_by_station(by_station);
        stations.copy_valptrs_to(stl::trivial_inserter(lookup_by_station));
        if (!lookup_by_station.found_items_in_index())
        {
            trace_query(" no matching stations found, setting empty result\n");
            res.set_to_empty();
            return;
        }
        // OR the results together into a single sequence
        res.add_union(lookup_by_station.release_sequences());
    }

    if (!levtrs.is_select_all())
    {
        trace_query("Adding selected levtrs to strategy\n");
        match::SequenceBuilder<LevTr> lookup_by_levtr(by_levtr);
        levtrs.copy_valptrs_to(stl::trivial_inserter(lookup_by_levtr));
        if (!lookup_by_levtr.found_items_in_index())
        {
            trace_query(" no matching levtrs found, setting empty result\n");
            res.set_to_empty();
            return;
        }
        // OR the results together into a single sequence
        res.add_union(lookup_by_levtr.release_sequences());
    }

    if (!q.datetime.is_missing())
    {
        const Datetime& mind = q.datetime.min;
        const Datetime& maxd = q.datetime.max;
        unique_ptr< stl::Sequences<size_t> > sequences(new stl::Sequences<size_t>);
        if (mind.date() == maxd.date())
        {
            Date d = mind.date();
            const set<size_t>* s = by_date.search(d);
            trace_query("Found exact date %04d-%02d-%02d\n", d.year, d.month, d.day);
            if (!s)
            {
                trace_query(" date not found in index, setting to the empty result set\n");
                res.set_to_empty();
                return;
            }
            if (by_date.size() == 1)
            {
                trace_query(" date matches the whole index: no point in adding a filter\n");
            } else {
                res.add_set(*s);
            }
            if (mind.time() == maxd.time())
                res.add(new MatchDateExact(mind));
            else
                res.add(new MatchDateMinMax(mind, maxd));
        } else {
            bool found;
            unique_ptr< stl::Sequences<size_t> > sequences;
            unique_ptr< Match<Value> > extra_match;

            if (maxd.is_missing()) {
                Date d = mind.date();
                sequences = by_date.search_from(d, found);
                extra_match.reset(new MatchDateMin(mind));
                trace_query("Found date min %04d-%02d-%02d\n", d.year, d.month, d.day);
            } else if (mind.is_missing()) {
                // FIXME: we need to add 1 second to maxd, as it is right extreme excluded
                Date d = maxd.date();
                sequences = by_date.search_to(d, found);
                extra_match.reset(new MatchDateMax(maxd));
                trace_query("Found date max %04d-%02d-%02d\n", d.year, d.month, d.day);
            } else {
                // FIXME: we need to add 1 second to maxd, as it is right extreme excluded
                Date dmin = mind.date();
                Date dmax = maxd.date();
                sequences = by_date.search_between(dmin, dmax, found);
                extra_match.reset(new MatchDateMinMax(mind, maxd));
                trace_query("Found date range %04d-%02d-%02d to %04d-%02d-%02d\n",
                        dmin.year, dmin.month, dmin.day, dmax.year, dmax.month, dmax.day);
            }

            if (!found)
            {
                trace_query(" no matching dates found, setting to the empty result set\n");
                res.set_to_empty();
                return;
            }

            if (sequences.get())
                res.add_union(std::move(sequences));
            else
                trace_query(" date range matches the whole index: no point in adding a filter\n");

            res.add(extra_match.release());
        }
    }

    switch (q.varcodes.size())
    {
        case 0: break;
        case 1:
            trace_query("Found varcode=%01d%02d%03d\n",
                    WR_VAR_F(*q.varcodes.begin()),
                    WR_VAR_X(*q.varcodes.begin()),
                    WR_VAR_Y(*q.varcodes.begin()));
            res.add(new match::Varcode<Value>(*q.varcodes.begin()));
        default:
            res.add(new match::Varcodes<Value>(q.varcodes));
            break;
    }

    if (!q.data_filter.empty())
    {
        trace_query("Found data_filter=%s\n", q.data_filter.c_str());
        res.add(new match::DataFilter<Value>(q.data_filter));
    }

    if (!q.attr_filter.empty())
    {
        trace_query("Found attr_filter=%s\n", q.attr_filter.c_str());
        res.add(new match::AttrFilter<Value>(q.attr_filter));
    }

    //trace_query("Strategy activated, %zu results\n", res.size());
}
#endif

}
}
}
