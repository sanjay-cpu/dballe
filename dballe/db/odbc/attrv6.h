/*
 * db/odbc/attrv6 - attribute table management
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

#ifndef DBALLE_DB_ODBC_ATTRV6_H
#define DBALLE_DB_ODBC_ATTRV6_H

#include <dballe/db/sql/attrv6.h>
#include <dballe/db/odbc/internals.h>

namespace dballe {
namespace db {
namespace odbc {

/**
 * Precompiled queries to manipulate the attr table
 */
class ODBCAttrV6 : public sql::AttrV6
{
protected:
    /** DB connection. */
    ODBCConnection& conn;

    /** Precompiled select statement */
    ODBCStatement* sstm;

    /** id_data SQL parameter */
    int id_data;
    /** attribute id SQL parameter */
    wreport::Varcode type;
    /** attribute value SQL parameter */
    char value[255];
    /** attribute value indicator */
    SQLLEN value_ind;

public:
    ODBCAttrV6(ODBCConnection& conn);
    ~ODBCAttrV6();

    void insert(Transaction& t, sql::bulk::InsertAttrsV6& vars, UpdateMode update_mode=UPDATE) override;

    /**
     * Load from the database all the attributes for var
     *
     * @param var
     *   wreport::Var to which the resulting attributes will be added
     * @return
     *   The error indicator for the function (See @ref error.h)
     */
    void read(int id_data, std::function<void(std::unique_ptr<wreport::Var>)> dest) override;

    /**
     * Dump the entire contents of the table to an output stream
     */
    void dump(FILE* out) override;

private:
    // disallow copy
    ODBCAttrV6(const ODBCAttrV6&);
    ODBCAttrV6& operator=(const ODBCAttrV6&);
};

}
}
}
#endif
