#ifndef DBA_DB_MEM_VALUE_H
#define DBA_DB_MEM_VALUE_H

#include <dballe/types.h>
#include <dballe/var.h>
#include <dballe/core/defs.h>
#include <dballe/core/values.h>
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

namespace msg {
struct Context;
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

    void dump(FILE* out, const char* end="\n") const;
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

    void dump(FILE* out, const char* end="\n") const;
};

/// Storage and index for measured values
template<typename Key>
class ValuesBase
{
public:
    typedef typename std::map<Key, int>::const_iterator Ptr;

    /**
     * Append only list of variables.
     *
     * The index of a Var in the list gives its data_id
     */
    std::vector<wreport::Var> variables;

    /**
     * Maps value context information to data_id
     */
    std::map<Key, int> values;

    void clear()
    {
        variables.clear();
        values.clear();
    }

    wreport::Var& get_checked(int data_id)
    {
        if (data_id < 0 || data_id > variables.size())
            wreport::error_notfound::throwf("cannot find variable for data_id %d", data_id);
        return variables[data_id];
    }

    const wreport::Var& get_checked(int data_id) const
    {
        if (data_id < 0 || data_id > variables.size())
            wreport::error_notfound::throwf("cannot find variable for data_id %d", data_id);
        return variables[data_id];
    }

    void query_attrs(int data_id, std::function<void(std::unique_ptr<wreport::Var>)> dest)
    {
        const wreport::Var& var = get_checked(data_id);
        for (const wreport::Var* a = var.next_attr(); a != NULL; a = a->next_attr())
            dest(newvar(*a));
    }

    void attr_insert(int data_id, const Values& attrs)
    {
        wreport::Var& var = get_checked(data_id);
        for (const auto& i: attrs)
            var.seta(*i.second.var);
    }

    void attr_remove(int data_id, const std::vector<wreport::Varcode>& qcs)
    {
        wreport::Var& var = get_checked(data_id);

        // FIXME: if qcs is empty, remove all?
        if (qcs.empty())
        {
            var.clear_attrs();
        } else {
            for (const auto& i: qcs)
                var.unseta(i);
        }
    }

    /**
     * Look up a data_id by its metadata.
     *
     * Returns -1 if the value was not found.
     */
    template<typename... Args>
    int get(wreport::Varcode code, Args&&... args)
    {
        Key key(std::forward<Args>(args)..., code);
        typename std::map<Key, int>::const_iterator i = values.find(key);
        if (i == values.end()) return -1;
        return i->second;
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

    /**
     * Remove a value
     */
    void remove(Ptr val)
    {
        variables[val->second].unset();
        variables[val->second].clear_attrs();
        values.erase(val);
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
            i.first.dump(out, "\t");
            variables[i.second].print(out);
            // TODO: print attrs
        }
    };
};

struct StationValues : public ValuesBase<StationValue>
{
    void fill_record(int ana_id, Record& rec) const;
    void fill_msg(int ana_id, msg::Context& ctx) const;

    /**
     * Wrap a consumer for station values with a filter function that filters entries based on q.
     *
     * Only q.varcodes, q.data_filter and q.attr_filter will be considered.
     */
    std::function<void(StationValues::Ptr)> wrap_filter(const core::Query& q, std::function<void(StationValues::Ptr)> dest) const;

    /**
     * Query the station values of the given station
     */
    void query(int ana_id, std::function<void(StationValues::Ptr)> dest) const;
};

struct DataValues : public ValuesBase<DataValue>
{
    /**
     * Wrap a consumer for data values with a filter function that filters entries based on q.
     *
     * Only q.level, q.trange, q.varcodes, q.data_filter and q.attr_filter will be considered.
     */
    std::function<void(DataValues::Ptr)> wrap_filter(const core::Query& q, std::function<void(DataValues::Ptr)> dest) const;

    /**
     * Query the station values of the given station, with no filter on datetimes
     */
    void query(int ana_id, std::function<void(DataValues::Ptr)> dest) const;

    /**
     * Query the station values of the given station and datetime range
     */
    void query(int ana_id, const DatetimeRange& dtr, std::function<void(DataValues::Ptr)> dest) const;
};

}
}
}

#endif
