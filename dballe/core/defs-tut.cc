/*
 * Copyright (C) 2005--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "core/test-utils-core.h"
#include "core/defs.h"

using namespace dballe;
using namespace wibble::tests;
using namespace std;

namespace tut {

struct defs_shar
{
};
TESTGRP(defs);

// Try to get descriptions for all the layers
template<> template<>
void to::test<1>()
{
	for (int i = 0; i < 261; ++i)
	{
		Level(i).describe();
		Level(i, 0).describe();
		Level(i, MISSING_INT, i, MISSING_INT).describe();
		Level(i, 0, i, 0).describe();
	}
}

// Try to get descriptions for all the time ranges
template<> template<>
void to::test<2>()
{
	for (int i = 0; i < 256; ++i)
	{
		Trange(i).describe();
		Trange(i, 0).describe();
		Trange(i, 0, 0).describe();
	}
}

// Verify some well-known descriptions
template<> template<>
void to::test<3>()
{
	ensure_equals(Level().describe(), "-");
	ensure_equals(Level(103, 2000).describe(), "2.000m above ground");
	ensure_equals(Level(103, 2000, 103, 4000).describe(),
			"Layer from [2.000m above ground] to [4.000m above ground]");
	ensure_equals(Trange(254, 86400).describe(),
			"Instantaneous value");
	ensure_equals(Trange(2, 0, 43200).describe(), "Maximum over 12h at forecast time 0");
	ensure_equals(Trange(3, 194400, 43200).describe(), "Minimum over 12h at forecast time 2d 6h");
}

// Test Coord
template<> template<>
void to::test<4>()
{
    wassert(actual(Coord(44.0, 11.0)) == Coord(44.0, 360.0+11.0));
}

// Test Date and Datetime
template<> template<>
void to::test<5>()
{
    wassert(actual(Date(2013, 1, 1)) < Date(2014, 1, 1));
    wassert(actual(Date(2013, 1, 1)) < Date(2013, 2, 1));
    wassert(actual(Date(2013, 1, 1)) < Date(2013, 1, 2));
    wassert(actual(Date(1945, 4, 25)) != Date(1945, 4, 26));
    wassert(actual(Datetime(2013, 1, 1, 0, 0, 0)) < Datetime(2014, 1, 1, 0, 0, 0));
    wassert(actual(Datetime(2013, 1, 1, 0, 0, 0)) < Datetime(2013, 2, 1, 0, 0, 0));
    wassert(actual(Datetime(2013, 1, 1, 0, 0, 0)) < Datetime(2013, 1, 2, 0, 0, 0));
    wassert(actual(Datetime(2013, 1, 1, 0, 0, 0)) < Datetime(2013, 1, 1, 1, 0, 0));
    wassert(actual(Datetime(2013, 1, 1, 0, 0, 0)) < Datetime(2013, 1, 1, 0, 1, 0));
    wassert(actual(Datetime(2013, 1, 1, 0, 0, 0)) < Datetime(2013, 1, 1, 0, 0, 1));
    wassert(actual(Datetime(1945, 4, 25, 8, 0, 0)) != Datetime(1945, 4, 26, 8, 0, 0));

}

}

/* vim:set ts=4 sw=4: */