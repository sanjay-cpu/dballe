/*
 * db/sql/attrv6 - v6 implementation of attr table
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

#ifndef DBALLE_DB_SQL_ATTRV6_H
#define DBALLE_DB_SQL_ATTRV6_H

#include <dballe/db/sql/datav6.h>
#include <wreport/var.h>
#include <functional>
#include <vector>
#include <memory>
#include <cstdio>

namespace dballe {
struct Record;

namespace db {
namespace sql {
struct AttributeList;

namespace bulk {
struct InsertAttrsV6;
}

/**
 * Precompiled queries to manipulate the attr table
 */
struct AttrV6
{
public:
    enum UpdateMode {
        UPDATE,
        IGNORE,
        ERROR,
    };

    virtual ~AttrV6();

    /// Insert all attributes of the given variable
    void insert_attributes(Transaction& t, int id_data, const wreport::Var& var, UpdateMode update_mode=UPDATE);

    /// Bulk attribute insert
    virtual void insert(Transaction& t, sql::bulk::InsertAttrsV6& vars, UpdateMode update_mode=UPDATE) = 0;

    /**
     * Load from the database all the attributes for var
     *
     * @param var
     *   wreport::Var to which the resulting attributes will be added
     * @return
     *   The error indicator for the function (See @ref error.h)
     */
    virtual void read(int id_data, std::function<void(std::unique_ptr<wreport::Var>)> dest) = 0;

    /**
     * Dump the entire contents of the table to an output stream
     */
    virtual void dump(FILE* out) = 0;
};

namespace bulk {

/**
 * Workflow information about an attribute variable listed for bulk
 * insert/update
 */
struct AttrV6 : public Item
{
    int id_data;
    const wreport::Var* attr;

    AttrV6(const wreport::Var* attr, int id_data=-1)
        : id_data(id_data), attr(attr)
    {
    }
    bool operator<(const AttrV6& v) const
    {
        if (int d = id_data - v.id_data) return d < 0;
        return attr->code() < v.attr->code();
    }

    void dump(FILE* out) const;
};

struct InsertAttrsV6 : public std::vector<AttrV6>
{
    void add(const wreport::Var* attr, int id_data)
    {
        emplace_back(attr, id_data);
    }

    // Add all attributes of the given variable
    void add_all(const wreport::Var& var, int id_data);

    void dump(FILE* out) const;
};

/**
 * Helper class for annotating InsertV6 variables with the current status of
 * the database.
 */
struct AnnotateAttrsV6
{
    InsertAttrsV6& attrs;
    InsertAttrsV6::iterator iter;
    bool do_insert = false;
    bool do_update = false;

    AnnotateAttrsV6(InsertAttrsV6& attrs);

    bool annotate(int id_data, wreport::Varcode code, const char* value);
    void annotate_end();

    void dump(FILE* out) const;
};

}


}
}
}

#endif

