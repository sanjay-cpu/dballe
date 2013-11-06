/*
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

#ifndef TUT_TEST_BODY

#include "dbapi-tut.h"

using namespace std;
using namespace dballe;

namespace dballe {
namespace tests {

void dbapi_tests::test_vars()
{
    fortran::DbAPI api(*db, "write", "write", "write");
    populate_variables(api);

    // Query stations
    api.unsetall();
    ensure_equals(api.quantesono(), 1);
    api.elencamele();
    ensure_equals(api.enqd("lat"), 44.5);
    ensure_equals(api.enqd("lon"), 11.5);

    // Query variables
    api.unsetall();
    ensure_equals(api.voglioquesto(), 2);
    ensure_equals(string(api.dammelo()), "B12101");
    ensure_equals(api.enqd("B12101"), 21.5);
    ensure_equals(string(api.dammelo()), "B11002");
    ensure_equals(api.enqd("lat"), 44.5);
    ensure_equals(api.enqd("lon"), 11.5);
    ensure_equals(api.enqd("B11002"), 2.4);

    // Delete variables
    api.unsetall();
    api.setc("var", "B12101");
    api.dimenticami();
    ensure_equals(api.voglioquesto(), 0);
}

void dbapi_tests::test_attrs()
{
    fortran::DbAPI api(*db, "write", "write", "write");
    populate_variables(api);

    int reference_id;

    // Query a variable
    api.setc("var", "B12101");
    ensure_equals(api.voglioquesto(), 1);
    api.dammelo();

    // Store its context info to access attributes of this variable later
    reference_id = api.enqi("context_id");
    ensure(reference_id > 0);

    // It has no attributes
    ensure_equals(api.voglioancora(), 0);

    // Set one attribute after a dammelo
    api.seti("*B33007", 50);
    api.critica();

    // It now has one attribute
    ensure_equals(api.voglioancora(), 1);


    // Query a different variable, it has no attributes
    api.setc("var", "B11002");
    ensure_equals(api.voglioquesto(), 1);
    api.dammelo();
    ensure_equals(api.voglioancora(), 0);


    // Query the first variable using its stored reference id
    api.seti("*context_id", reference_id);
    api.setc("*var_related", "B12101");
    ensure_equals(api.voglioancora(), 1);
    ensure_equals(api.enqi("*B33007"), 50);
}

void dbapi_tests::test_attrs_prendilo()
{
    fortran::DbAPI api(*db, "write", "write", "write");
    populate_variables(api);

    // Set one attribute after a prendilo
    api.setd("lat", 44.5);
    api.setd("lon", 11.5);
    api.setc("rep_memo", "synop");
    api.setdate(2013, 4, 25, 12, 0, 0);
    api.setlevel(1, MISSING_INT, MISSING_INT, MISSING_INT);
    api.settimerange(254, 0, 0);
    api.setd("B10004", 100000.0);
    api.prendilo(); // Pressure at ground level
    api.seti("*B33007", 60);
    api.critica();

    // Query it again
    api.unsetall();
    api.setc("var", "B10004");
    ensure_equals(api.voglioquesto(), 1);
    api.dammelo();
    ensure_equals(api.voglioancora(), 1);
    ensure_equals(api.enqi("*B33007"), 60);
}

void dbapi_tests::test_prendilo_anaid()
{
    fortran::DbAPI api(*db, "write", "write", "write");
    populate_variables(api);

    // Run a prendilo
    api.setd("lat", 44.5);
    api.setd("lon", 11.5);
    api.setc("rep_memo", "synop");
    api.setdate(2013, 4, 25, 12, 0, 0);
    api.setlevel(1, MISSING_INT, MISSING_INT, MISSING_INT);
    api.settimerange(254, 0, 0);
    api.setd("B10004", 100000.0);
    api.prendilo(); // Pressure at ground level

    ensure(api.enqi("*ana_id") > 0);
}

}
}

#else

template<> template<> void to::test<1>() { test_vars(); }
template<> template<> void to::test<2>() { test_attrs(); }
template<> template<> void to::test<3>() { test_attrs_prendilo(); }
template<> template<> void to::test<4>() { test_prendilo_anaid(); }

#endif
