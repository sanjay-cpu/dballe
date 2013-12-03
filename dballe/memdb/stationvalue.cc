/*
 * memdb/stationvalue - In memory representation of station values
 *
 * Copyright (C) 2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */
#include "stationvalue.h"
#include "station.h"
#include "dballe/msg/context.h"
#include <iostream>

using namespace wreport;
using namespace std;

namespace dballe {
namespace memdb {

StationValue::~StationValue()
{
    delete var;
}

void StationValue::replace(std::auto_ptr<Var> var)
{
    delete this->var;
    this->var = var.release();
}

void StationValues::clear()
{
    by_station.clear();
    ValueStorage<StationValue>::clear();
}

const StationValue* StationValues::get(const Station& station, wreport::Varcode code) const
{
    Positions res = by_station.search(&station);
    for (Positions::const_iterator i = res.begin(); i != res.end(); ++i)
    {
        const StationValue* s = (*this)[*i];
        if (s && s->var->code() == code)
            return s;
    }
    return 0;
}

size_t StationValues::insert(const Station& station, std::auto_ptr<Var> var, bool replace)
{
    Positions res = by_station.search(&station);
    for (Positions::const_iterator i = res.begin(); i != res.end(); ++i)
    {
        StationValue* s = (*this)[*i];
        if (s && s->var->code() == var->code())
        {
            if (!replace)
                throw error_consistency("cannot replace an existing station value");
            s->replace(var);
            return *i;
        }
    }

    // Value not found, create it
    size_t pos = value_add(new StationValue(station, var));
    // Index it
    by_station[&station].insert(pos);
    // And return it
    return pos;

}

size_t StationValues::insert(const Station& station, const Var& var, bool replace)
{
    auto_ptr<Var> copy(new Var(var));
    return insert(station, copy, replace);
}

bool StationValues::remove(const Station& station, Varcode code)
{
    Positions res = by_station.search(&station);
    for (Positions::const_iterator i = res.begin(); i != res.end(); ++i)
    {
        const StationValue* s = (*this)[*i];
        if (s && !s->var->code() == code)
        {
            by_station[&station].erase(*i);
            value_remove(*i);
            return true;
        }
    }
    return false;
}

void StationValues::fill_msg(const Station& station, msg::Context& ctx) const
{
    Positions res = by_station.search(&station);
    for (Positions::const_iterator i = res.begin(); i != res.end(); ++i)
    {
        const StationValue* s = (*this)[*i];
        ctx.set(*s->var);
    }
}

template class Index<const Station*>;
template class ValueStorage<StationValue>;

}
}

#include "core.tcc"