#include "levtr.h"
#include "dballe/core/defs.h"
#include "dballe/msg/msg.h"
#include "dballe/sql/querybuf.h"
#include "dballe/sql/postgresql.h"
#include <map>
#include <sstream>
#include <cstring>
#include <sqltypes.h>
#include <sql.h>

using namespace wreport;
using namespace std;
using dballe::sql::PostgreSQLConnection;

namespace dballe {
namespace db {
namespace v7 {
namespace postgresql {

namespace {

Level to_level(const dballe::sql::postgresql::Result& res, unsigned row, int first_id)
{
    return Level(
            res.get_int4(row, first_id),
            res.get_int4(row, first_id + 1),
            res.get_int4(row, first_id + 2),
            res.get_int4(row, first_id + 3));
}

Trange to_trange(const dballe::sql::postgresql::Result& res, unsigned row, int first_id)
{
    return Trange(
            res.get_int4(row, first_id),
            res.get_int4(row, first_id + 1),
            res.get_int4(row, first_id + 2));
}

}

PostgreSQLLevTr::PostgreSQLLevTr(PostgreSQLConnection& conn)
    : conn(conn)
{
    conn.prepare("v7_levtr_select_id", R"(
        SELECT id FROM levtr WHERE ltype1=$1::int4 AND l1=$2::int4 AND ltype2=$3::int4 AND l2=$4::int4
                               AND pind=$5::int4 AND p1=$6::int4 AND p2=$7::int4
    )");
    conn.prepare("v7_levtr_select_data", "SELECT ltype1, l1, ltype2, l2, pind, p1, p2 FROM levtr WHERE id=$1::int4");
    conn.prepare("v7_levtr_insert", R"(
        INSERT INTO levtr (id, ltype1, l1, ltype2, l2, pind, p1, p2)
             VALUES (DEFAULT, $1::int4, $2::int4, $3::int4, $4::int4, $5::int4, $6::int4, $7::int4)
          RETURNING id
    )");
}

PostgreSQLLevTr::~PostgreSQLLevTr()
{
}

void PostgreSQLLevTr::prefetch_ids(const std::set<int>& ids, std::map<int, LevTrDesc>& data)
{
    if (ids.empty()) return;

    sql::Querybuf qb;
    if (ids.size() < 100)
    {
        qb.append("SELECT id, ltype1, l1, ltype2, l2, pind, p1, p2 FROM levtr WHERE id IN (");
        qb.start_list(",");
        for (auto id: ids)
            qb.append_listf("%d", id);
        qb.append(")");
    } else
        qb.append("SELECT id, ltype1, l1, ltype2, l2, pind, p1, p2 FROM levtr");

    auto res = conn.exec(qb);
    for (unsigned row = 0; row < res.rowcount(); ++row)
        data.insert(make_pair(res.get_int4(row, 0), LevTrDesc(to_level(res, row, 1), to_trange(res, row, 5))));
}

levtrs_t::iterator PostgreSQLLevTr::lookup_id(State& st, int id)
{
    using namespace dballe::sql::postgresql;

    auto cached = st.levtr_ids.find(id);
    if (cached != st.levtr_ids.end())
        return cached->second;

    auto res = conn.exec_prepared("v7_levtr_select_data", id);
    switch (res.rowcount())
    {
        case 0: error_notfound::throwf("levtr with id %d not found in the database", id);
        case 1: return st.add_levtr(LevTrDesc(to_level(res, 0, 0), to_trange(res, 0, 4)), LevTrState(id, false));
        default: error_consistency::throwf("select levtr data query returned %u results", res.rowcount());
    }
}

levtrs_t::iterator PostgreSQLLevTr::obtain_id(State& state, const LevTrDesc& desc)
{
    using namespace dballe::sql::postgresql;

    auto cached = state.levtrs.find(desc);
    if (cached != state.levtrs.end())
        return cached;

    Result res = conn.exec_prepared("v7_levtr_select_id",
            desc.level.ltype1, desc.level.l1, desc.level.ltype2, desc.level.l2,
            desc.trange.pind, desc.trange.p1, desc.trange.p2);
    switch (res.rowcount())
    {
        case 0:
            return state.add_levtr(desc, LevTrState(
                conn.exec_prepared_one_row("v7_levtr_insert",
                        desc.level.ltype1, desc.level.l1, desc.level.ltype2, desc.level.l2,
                        desc.trange.pind, desc.trange.p1, desc.trange.p2).get_int4(0, 0), true));
        case 1: return state.add_levtr(desc, LevTrState(res.get_int4(0, 0), false));
        default: error_consistency::throwf("select levtr ID query returned %u results", res.rowcount());

    }
}

void PostgreSQLLevTr::_dump(std::function<void(int, const Level&, const Trange&)> out)
{
    auto res = conn.exec("SELECT id, ltype1, l1, ltype2, l2, pind, p1, p2 FROM levtr ORDER BY ID");
    for (unsigned row = 0; row < res.rowcount(); ++row)
        out(res.get_int4(row, 0), to_level(res, row, 1), to_trange(res, row, 5));
}

}
}
}
}