#ifndef DBALLE_DB_V7_LEVTR_H
#define DBALLE_DB_V7_LEVTR_H

#include <dballe/core/defs.h>
#include <dballe/db/v7/state.h>
#include <dballe/db/v7/cache.h>
#include <memory>
#include <set>
#include <cstdio>
#include <functional>

namespace dballe {
struct Record;
struct Msg;

namespace msg {
struct Context;
}

namespace db {
namespace v7 {

/**
 * Precompiled queries to manipulate the lev_tr table
 */
struct LevTr
{
protected:
    LevTrCache cache;
    virtual void _dump(std::function<void(int, const Level&, const Trange&)> out) = 0;

public:
    virtual ~LevTr();

    /**
     * Invalidate the LevTrEntry cache.
     *
     * Further accesses will be done via the database, and slowly repopulate
     * the cache from scratch.
     */
    void clear_cache();

    /**
     * Given a set of IDs, load LevTr information for them and add it to the cache.
     */
    virtual void prefetch_ids(const std::set<int>& ids) = 0;

    /**
     * Get/create a Context in the Msg for this level/timerange.
     *
     * @returns the context, or 0 if the id is not valid.
     */
    msg::Context* to_msg(int id, Msg& msg);

    /// Look up a LevTr from the database given its ID.
    virtual const LevTrEntry* lookup_id(int id) = 0;

    /**
     * Look up a LevTr from the database given its description. Insert a new
     * one if not found.
     */
    virtual int obtain_id(const LevTrEntry& desc) = 0;

    /// Dump the entire contents of the table to an output stream
    void dump(FILE* out);
};

}
}
}
#endif
