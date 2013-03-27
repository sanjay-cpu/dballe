/*
 * dballe/db - Archive for point-based meteorological data
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

#include "db.h"
#include "v5/db.h"
#include "v6/db.h"
#include "internals.h"
#include <wreport/error.h>
#include <cstring>
#include <cstdlib>

using namespace dballe::db;
using namespace std;
using namespace wreport;

namespace dballe {
namespace db {

static Format default_format = V5;

Cursor::~Cursor()
{
}

}

DB::~DB()
{
}

Format DB::get_default_format() { return default_format; }
void DB::set_default_format(Format format) { default_format = format; }

bool DB::is_url(const char* str)
{
    if (strncmp(str, "sqlite:", 7) == 0) return true;
    if (strncmp(str, "odbc://", 7) == 0) return true;
    if (strncmp(str, "test:", 5) == 0) return true;
    return false;
}

auto_ptr<DB> DB::instantiate_db(auto_ptr<Connection>& conn)
{
    // Autodetect format
    Format format = default_format;

    bool found = true;

    // Try with reading it from the settings table
    string version = conn->get_setting("version");
    if (version == "V5")
        format = V5;
    else if (version == "V6")
        format = V6;
    else if (version == "")
        found = false;// Some other key exists, but the version has not been set
    else
        error_consistency::throwf("unsupported database version: '%s'", version.c_str());
   
    // If it failed, try looking at the existing table structure
    if (!found)
    {
        if (conn->has_table("lev_tr"))
            format = V6;
        else if (conn->has_table("context"))
            format = V5;
        else
            format = default_format;
    }

    switch (format)
    {
        case V5: return auto_ptr<DB>(new v5::DB(conn));
        case V6: return auto_ptr<DB>(new v6::DB(conn));
    }
}

auto_ptr<DB> DB::connect(const char* dsn, const char* user, const char* password)
{
    auto_ptr<Connection> conn(new Connection);
    conn->connect(dsn, user, password);
    return instantiate_db(conn);
}

auto_ptr<DB> DB::connect_from_file(const char* pathname)
{
    auto_ptr<Connection> conn(new Connection);
    conn->connect_file(pathname);
    return instantiate_db(conn);
}

auto_ptr<DB> DB::connect_from_url(const char* url)
{
    if (strncmp(url, "sqlite://", 9) == 0)
    {
        return connect_from_file(url + 9);
    }
    if (strncmp(url, "sqlite:", 7) == 0)
    {
        return connect_from_file(url + 7);
    }
    if (strncmp(url, "odbc://", 7) == 0)
    {
        string buf(url + 7);
        size_t pos = buf.find('@');
        if (pos == string::npos)
        {
            return connect(buf.c_str(), "", ""); // odbc://dsn
        }
        // Split the string at '@'
        string userpass = buf.substr(0, pos);
        string dsn = buf.substr(pos + 1);

        pos = userpass.find(':');
        if (pos == string::npos)
        {
            return connect(dsn.c_str(), userpass.c_str(), ""); // odbc://user@dsn
        }

        string user = userpass.substr(0, pos);
        string pass = userpass.substr(pos + 1);

        return connect(dsn.c_str(), user.c_str(), pass.c_str()); // odbc://user:pass@dsn
    }
    if (strncmp(url, "test:", 5) == 0)
    {
        return connect_test();
    }
    error_consistency::throwf("unknown url \"%s\"", url);
}

auto_ptr<DB> DB::connect_test()
{
    const char* envurl = getenv("DBA_DB");
    if (envurl != NULL)
        return connect_from_url(envurl);
    else
        return connect_from_file("test.sqlite");
}


}

/* vim:set ts=4 sw=4: */
