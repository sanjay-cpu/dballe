/*
 * db/sql/runqueryv6 - db-independent support for specific v6 queries
 *
 * Copyright (C) 2005--2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "driver.h"
#include "config.h"
#include "dballe/db/sqlite/driver.h"
#include "dballe/db/sqlite/internals.h"
#ifdef HAVE_LIBPQ
#include "dballe/db/postgresql/driver.h"
#include "dballe/db/postgresql/internals.h"
#endif
#ifdef HAVE_MYSQL
#include "dballe/db/mysql/driver.h"
#include "dballe/db/mysql/internals.h"
#endif
#ifdef HAVE_ODBC
#include "dballe/db/odbc/driver.h"
#include "dballe/db/odbc/internals.h"
#endif
#include <cstring>
#include <sstream>

using namespace wreport;
using namespace std;

namespace dballe {
namespace db {
namespace sql {

bool SQLRecordV6::querybest_fields_are_the_same(const SQLRecordV6& r)
{
    if (out_ana_id != r.out_ana_id) return false;
    if (out_id_ltr != r.out_id_ltr) return false;
    if (out_datetime != r.out_datetime) return false;
    if (out_varcode != r.out_varcode) return false;
    return true;
}

void SQLRecordV6::dump(FILE* out)
{
    fprintf(out, "st: %d %6d %6d ", out_ana_id, out_lat, out_lon);
    if (out_ident_size == -1)
        fputs("fixed, ", out);
    else
        fprintf(out, "%.*s, ", out_ident_size, out_ident);
    fprintf(out, "rc: %d %d, ltr %d, did %d, ", out_rep_cod, priority, out_id_ltr, out_id_data);
    stringstream s;
    s << "dt: " << out_datetime << " " << out_datetimemax << ", ";
    fputs(s.str().c_str(), out);
    fprintf(out, "%d%02d%03d %s\n", WR_VAR_F(out_varcode), WR_VAR_X(out_varcode), WR_VAR_Y(out_varcode), out_value);
}

Driver::~Driver()
{
}

void Driver::create_tables(db::Format format)
{
    switch (format)
    {
        case V5: throw error_unimplemented("database version V5 is not supported anymore");
        case V6: create_tables_v6(); break;
        default: throw wreport::error_consistency("cannot create tables on the given DB format");
    }
}

void Driver::delete_tables(db::Format format)
{
    switch (format)
    {
        case V5: throw error_unimplemented("database version V5 is not supported anymore");
        case V6: delete_tables_v6(); break;
        default: throw wreport::error_consistency("cannot delete tables on the given DB format");
    }
}

void Driver::remove_all(db::Format format)
{
    switch (format)
    {
        case V5: throw error_unimplemented("database version V5 is not supported anymore");
        case V6: remove_all_v6(); break;
        default: throw wreport::error_consistency("cannot empty a database with the given format");
    }
}

void Driver::remove_all_v6()
{
    exec_no_data("DELETE FROM attr");
    exec_no_data("DELETE FROM data");
    exec_no_data("DELETE FROM lev_tr");
    exec_no_data("DELETE FROM station");
}

void Driver::explain(const std::string& query)
{
    fprintf(stderr, "Explaining query %s is not supported on this db.\n", query.c_str());
}

std::unique_ptr<Driver> Driver::create(Connection& conn)
{
    if (SQLiteConnection* c = dynamic_cast<SQLiteConnection*>(&conn))
        return unique_ptr<Driver>(new sqlite::Driver(*c));
#ifdef HAVE_ODBC
    else if (ODBCConnection* c = dynamic_cast<ODBCConnection*>(&conn))
        return unique_ptr<Driver>(new odbc::Driver(*c));
#endif
#ifdef HAVE_LIBPQ
    else if (PostgreSQLConnection* c = dynamic_cast<PostgreSQLConnection*>(&conn))
        return unique_ptr<Driver>(new postgresql::Driver(*c));
#endif
#ifdef HAVE_MYSQL
    else if (MySQLConnection* c = dynamic_cast<MySQLConnection*>(&conn))
        return unique_ptr<Driver>(new mysql::Driver(*c));
#endif
    else
        throw error_unimplemented("DB drivers only implemented for "
#ifdef HAVE_LIBPQ
                "PostgreSQL, "
#endif
#ifdef HAVE_MYSQL
                "MySQL, "
#endif
#ifdef HAVE_ODBC
                "ODBC, "
#endif
                " and SQLite connectors");
}

}
}
}
