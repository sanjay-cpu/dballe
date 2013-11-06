/*
 * Copyright (C) 2005--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "db/db-export-tut.h"
#include "db/db.h"
#include "core/record.h"

using namespace dballe;
using namespace wreport;
using namespace std;

namespace dballe {
namespace tests {

void db_export_tests::populate_database()
{
    // Insert some data
    Record rec;
    rec.set(DBA_KEY_LAT, 12.34560);
    rec.set(DBA_KEY_LON, 76.54321);
    rec.set(DBA_KEY_MOBILE, 0);

    rec.set(DBA_KEY_YEAR, 1945);
    rec.set(DBA_KEY_MONTH, 4);
    rec.set(DBA_KEY_DAY, 25);
    rec.set(DBA_KEY_HOUR, 8);
    rec.set(DBA_KEY_MIN, 0);

    rec.set(DBA_KEY_LEVELTYPE1, 1);
    rec.set(DBA_KEY_L1, 2);
    rec.set(DBA_KEY_LEVELTYPE2, 0);
    rec.set(DBA_KEY_L2, 3);
    rec.set(DBA_KEY_PINDICATOR, 4);
    rec.set(DBA_KEY_P1, 5);
    rec.set(DBA_KEY_P2, 6);

    rec.set(DBA_KEY_REP_MEMO, "synop");

    rec.set(WR_VAR(0, 1, 12), 500);

    db->insert(rec, false, true);

    rec.unset(DBA_KEY_ANA_ID);
    rec.unset(DBA_KEY_CONTEXT_ID);
    rec.set(DBA_KEY_DAY, 26);
    rec.set(WR_VAR(0, 1, 12), 400);
    db->insert(rec, false, true);

    rec.unset(DBA_KEY_ANA_ID);
    rec.unset(DBA_KEY_CONTEXT_ID);
    rec.set(DBA_KEY_MOBILE, 1);
    rec.set(DBA_KEY_IDENT, "ciao");
    rec.set(WR_VAR(0, 1, 12), 300);
    db->insert(rec, false, true);

    rec.unset(DBA_KEY_ANA_ID);
    rec.unset(DBA_KEY_CONTEXT_ID);
    rec.set(DBA_KEY_REP_MEMO, "metar");
    rec.set(WR_VAR(0, 1, 12), 200);
    db->insert(rec, false, true);
}

namespace {
struct MsgCollector : public vector<Msg*>, public MsgConsumer
{
    ~MsgCollector()
    {
        for (iterator i = begin(); i != end(); ++i)
            delete *i;
    }
    void operator()(auto_ptr<Msg> msg)
    {
        push_back(msg.release());
    }
};
}

void db_export_tests::test_simple_export()
{
    populate_database();

    // Put some data in the database and check that it gets exported properly
	// Query back the data
    Record query;

	MsgCollector msgs;
    db->export_msgs(query, msgs);
	ensure_equals(msgs.size(), 4u);

	if (msgs[2]->type == MSG_METAR)
	{
		// Since the order here is not determined, enforce one
		Msg* tmp = msgs[2];
		msgs[2] = msgs[3];
		msgs[3] = tmp;
	}

	ensure_equals(msgs[0]->type, MSG_SYNOP);
	ensure_var_equals(want_var(*msgs[0], DBA_MSG_LATITUDE), 12.34560);
	ensure_var_equals(want_var(*msgs[0], DBA_MSG_LONGITUDE), 76.54321);
	ensure_msg_undef(*msgs[0], DBA_MSG_IDENT);
	ensure_var_equals(want_var(*msgs[0], DBA_MSG_YEAR), 1945);
	ensure_var_equals(want_var(*msgs[0], DBA_MSG_MONTH), 4);
	ensure_var_equals(want_var(*msgs[0], DBA_MSG_DAY), 25);
	ensure_var_equals(want_var(*msgs[0], DBA_MSG_HOUR), 8);
	ensure_var_equals(want_var(*msgs[0], DBA_MSG_MINUTE), 0);
	ensure_var_equals(want_var(*msgs[0], WR_VAR(0, 1, 12), Level(1, 2, 0, 3), Trange(4, 5, 6)), 500);

	ensure_equals(msgs[1]->type, MSG_SYNOP);
	ensure_var_equals(want_var(*msgs[1], DBA_MSG_LATITUDE), 12.34560);
	ensure_var_equals(want_var(*msgs[1], DBA_MSG_LONGITUDE), 76.54321);
	ensure_msg_undef(*msgs[1], DBA_MSG_IDENT);
	ensure_var_equals(want_var(*msgs[1], DBA_MSG_YEAR), 1945);
	ensure_var_equals(want_var(*msgs[1], DBA_MSG_MONTH), 4);
	ensure_var_equals(want_var(*msgs[1], DBA_MSG_DAY), 26);
	ensure_var_equals(want_var(*msgs[1], DBA_MSG_HOUR), 8);
	ensure_var_equals(want_var(*msgs[1], DBA_MSG_MINUTE), 0);
	ensure_var_equals(want_var(*msgs[1], WR_VAR(0, 1, 12), Level(1, 2, 0, 3), Trange(4, 5, 6)), 400);

	ensure_equals(msgs[2]->type, MSG_SYNOP);
	ensure_var_equals(want_var(*msgs[2], DBA_MSG_LATITUDE), 12.34560);
	ensure_var_equals(want_var(*msgs[2], DBA_MSG_LONGITUDE), 76.54321);
	ensure_var_equals(want_var(*msgs[2], DBA_MSG_IDENT), "ciao");
	ensure_var_equals(want_var(*msgs[2], DBA_MSG_YEAR), 1945);
	ensure_var_equals(want_var(*msgs[2], DBA_MSG_MONTH), 4);
	ensure_var_equals(want_var(*msgs[2], DBA_MSG_DAY), 26);
	ensure_var_equals(want_var(*msgs[2], DBA_MSG_HOUR), 8);
	ensure_var_equals(want_var(*msgs[2], DBA_MSG_MINUTE), 0);
	ensure_var_equals(want_var(*msgs[2], WR_VAR(0, 1, 12), Level(1, 2, 0, 3), Trange(4, 5, 6)), 300);

	ensure_equals(msgs[3]->type, MSG_METAR);
	ensure_var_equals(want_var(*msgs[3], DBA_MSG_LATITUDE), 12.34560);
	ensure_var_equals(want_var(*msgs[3], DBA_MSG_LONGITUDE), 76.54321);
	ensure_var_equals(want_var(*msgs[3], DBA_MSG_IDENT), "ciao");
	ensure_var_equals(want_var(*msgs[3], DBA_MSG_YEAR), 1945);
	ensure_var_equals(want_var(*msgs[3], DBA_MSG_MONTH), 4);
	ensure_var_equals(want_var(*msgs[3], DBA_MSG_DAY), 26);
	ensure_var_equals(want_var(*msgs[3], DBA_MSG_HOUR), 8);
	ensure_var_equals(want_var(*msgs[3], DBA_MSG_MINUTE), 0);
	ensure_var_equals(want_var(*msgs[3], WR_VAR(0, 1, 12), Level(1, 2, 0, 3), Trange(4, 5, 6)), 200);
}

void db_export_tests::test_station_info_export()
{
    // Text exporting of extra station information

    // Import some data in the station extra information context
    Record in;
    in.set(DBA_KEY_LAT, 45.0);
    in.set(DBA_KEY_LON, 11.0);
    in.set(DBA_KEY_REP_MEMO, "synop");
    in.set_ana_context();
    in.set(WR_VAR(0, 1, 1), 10);
    db->insert(in, false, true);

    // Import one real datum
    in.clear();
    in.set(DBA_KEY_LAT, 45.0);
    in.set(DBA_KEY_LON, 11.0);
    in.set(DBA_KEY_REP_MEMO, "synop");
    in.set(DBA_KEY_YEAR, 2000);
    in.set(DBA_KEY_MONTH, 1);
    in.set(DBA_KEY_DAY, 1);
    in.set(DBA_KEY_HOUR, 0);
    in.set(DBA_KEY_MIN, 0);
    in.set(DBA_KEY_SEC, 0);
    in.set(DBA_KEY_LEVELTYPE1, 103);
    in.set(DBA_KEY_L1, 2000);
    in.set(DBA_KEY_PINDICATOR, 254);
    in.set(DBA_KEY_P1, 0);
    in.set(DBA_KEY_P2, 0);
    in.set(WR_VAR(0, 12, 101), 290.0);
    db->insert(in, false, true);

	// Query back the data
    Record query;
	MsgCollector msgs;
    db->export_msgs(query, msgs);
	ensure_equals(msgs.size(), 1u);

	ensure_equals(msgs[0]->type, MSG_SYNOP);
	ensure_var_equals(want_var(*msgs[0], DBA_MSG_LATITUDE), 45.0);
	ensure_var_equals(want_var(*msgs[0], DBA_MSG_LONGITUDE), 11.0);
	ensure_msg_undef(*msgs[0], DBA_MSG_IDENT);
	ensure_var_equals(want_var(*msgs[0], DBA_MSG_BLOCK), 10);
	ensure_var_equals(want_var(*msgs[0], DBA_MSG_TEMP_2M), 290.0);
}

}
}

#else

template<> template<> void to::test<1>() { test_simple_export(); }
template<> template<> void to::test<2>() { test_station_info_export(); }

#endif

// vim:set ts=4 sw=4:
