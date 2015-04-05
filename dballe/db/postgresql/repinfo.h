/*
 * db/postgresql/repinfo - repinfo table management
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

#ifndef DBALLE_DB_POSTGRESQL_REPINFO_H
#define DBALLE_DB_POSTGRESQL_REPINFO_H

/** @file
 * @ingroup db
 *
 * Repinfo table management used by the db module.
 */

#include <dballe/db/v5/repinfo.h>
#include <vector>
#include <string>
#include <map>

namespace dballe {
struct Record;

namespace db {
struct PostgreSQLConnection;

namespace v5 {

/**
 * Fast cached access to the repinfo table
 */
struct PostgreSQLRepinfo : public Repinfo
{
    /**
     * DB connection. The pointer is assumed always valid during the
     * lifetime of the object
     */
    PostgreSQLConnection& conn;

    PostgreSQLRepinfo(PostgreSQLConnection& conn);
    PostgreSQLRepinfo(const PostgreSQLRepinfo&) = delete;
    PostgreSQLRepinfo(const PostgreSQLRepinfo&&) = delete;
    virtual ~PostgreSQLRepinfo();
    PostgreSQLRepinfo& operator=(const PostgreSQLRepinfo&) = delete;

    void update(const char* deffile, int* added, int* deleted, int* updated) override;
    void dump(FILE* out) override;

protected:
    /// Return how many time this ID is used in the database
    virtual int id_use_count(unsigned id, const char* name);

    void read_cache() override;
    void insert_auto_entry(const char* memo) override;
};

}

namespace v6 {

struct PostgreSQLRepinfo : public v5::PostgreSQLRepinfo
{
    PostgreSQLRepinfo(PostgreSQLConnection& conn);

protected:
    int id_use_count(unsigned id, const char* name) override;
};

}
}
}
#endif

