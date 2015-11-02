#ifndef DBA_DB_MEM_VALUE_H
#define DBA_DB_MEM_VALUE_H

#include <dballe/types.h>
#include <dballe/core/defs.h>
#include <wreport/var.h>
#include <memory>
#include <vector>
#include <map>
#include <cstdio>

namespace dballe {
struct Record;

namespace core {
struct Query;
}

namespace db {
namespace mem {

/// A value about a station
struct StationValue
{
    int ana_id;
    wreport::Varcode code;

    StationValue(int ana_id, wreport::Varcode code)
        : ana_id(ana_id), code(code) {}

    /**
     * Generic comparison
     *
     * Returns a negative number if *this < other
     * Returns zero if *this == other
     * Returns a positive number if *this > other
     */
    int compare(const StationValue&) const;

    bool operator<(const StationValue& v) const { return compare(v) < 0; }

    void dump(FILE* out) const;
};

/// A value measured by a station
struct DataValue
{
    int ana_id;
    Datetime datetime;
    Level level;
    Trange trange;
    wreport::Varcode code;

    DataValue(int ana_id, const Datetime& datetime, const Level& level, const Trange& trange, wreport::Varcode code)
        : ana_id(ana_id), datetime(datetime), level(level), trange(trange), code(code) {}

    /**
     * Generic comparison
     *
     * Returns a negative number if *this < other
     * Returns zero if *this == other
     * Returns a positive number if *this > other
     */
    int compare(const DataValue&) const;

    bool operator<(const DataValue& v) const { return compare(v) < 0; }

    void dump(FILE* out) const;
};

/// Storage and index for measured values
template<typename Key>
class ValuesBase
{
public:
    /**
     * Append only list of variables.
     *
     * The index of a Var in the list gives its data_id
     */
    std::vector<wreport::Var> variables;

    /**
     * Maps value context information to data_id
     *
     * Order used by station queryes: ana_id
     * Order used by data queries: ana_id, datetime, level, trange, report, var
     * Order used by export queries: ana_id, report, datetime, level, trange, var
     */
    std::map<Key, int> values;

    void clear()
    {
        variables.clear();
        values.clear();
    }

    /// Insert a new value, or replace an existing one. Return the data_id.
    template<typename... Args>
    int insert(std::unique_ptr<wreport::Var>&& var, bool replace, Args&&... args)
    {
        Key key(std::forward<Args>(args)..., var->code());
        typename std::map<Key, int>::const_iterator i = values.find(key);

        if (i == values.end())
        {
            // Value not found, append it
            int data_id = variables.size();
            variables.emplace_back(std::move(*var));
            values.emplace(std::make_pair(key, data_id));
            return data_id;
        } else {
            // Value found
            if (!replace)
                throw wreport::error_consistency("cannot replace an existing value");

            int data_id = i->second;
            // TODO: implement a way to move variable values in wreport
            variables[data_id].setval(*var);
            return data_id;
        }
    }

    /// Insert a new value, or replace an existing one. Return the data_id.
    template<typename... Args>
    int insert(const wreport::Var& var, bool replace, Args&&... args)
    {
        Key key(std::forward<Args>(args)..., var.code());
        typename std::map<Key, int>::const_iterator i = values.find(key);

        if (i == values.end())
        {
            // Value not found, append it
            int data_id = variables.size();
            variables.emplace_back(var);
            values.emplace(std::make_pair(key, data_id));
            return data_id;
        } else {
            // Value found
            if (!replace)
                throw wreport::error_consistency("cannot replace an existing value");

            int data_id = i->second;
            // TODO: implement a way to move variable values in wreport
            variables[data_id].setval(var);
            return data_id;
        }
    }

    /**
     * Remove a value.
     *
     * Returns true if found and removed, false if it was not found.
     */
    template<typename... Args>
    bool remove(wreport::Varcode code, Args&&... args)
    {
        Key key(std::forward<Args>(args)..., code);
        typename std::map<Key, int>::iterator i = values.find(key);
        if (i == values.end())
            return false;

        // TODO: implement a Var::clear() in wreport that also deallocates the value
        variables[i->second].unset();
        variables[i->second].clear_attrs();
        values.erase(i);
        return true;
    }

    /// Removes a value, by index
    //void erase(size_t idx);

    /// Query values returning the IDs
    //void query(const core::Query& q, Results<Station>& stations, Results<LevTr>& levtrs, Results<Value>& res) const;

    void dump(FILE* out) const
    {
        fprintf(out, "Values:\n");
        for (const auto& i: values)
        {
            fprintf(out, " %4u: ", i.second);
            i.first.dump(out);
            // TODO: print attrs
        }
    };
};

struct StationValues : public ValuesBase<StationValue>
{
};

struct DataValues : public ValuesBase<DataValue>
{
};

}
}
}

#endif
