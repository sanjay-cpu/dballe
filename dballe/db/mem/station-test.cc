#include "db/tests.h"
#include "dballe/core/query.h"
#include "dballe/core/record.h"
#include "station.h"

using namespace dballe;
using namespace dballe::tests;
using namespace std;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override
    {
        add_method("basic", []() {
            db::mem::Stations stations;
            stations.reserve(8); // Prevent reallocations that invalidate references

            // Insert a fixed station and check that all data is there
            const Station& stf = stations[stations.obtain("synop", Coords(44.0, 11.0), Ident())];
            wassert(actual(stf.ana_id) == 0);
            wassert(actual(stf.coords.dlat()) == 44.0);
            wassert(actual(stf.coords.dlon()) == 11.0);
            wassert(actual(stf.ident) == nullptr);
            wassert(actual(stf.report) == "synop");

            // Insert a mobile station and check that all data is there
            const Station& stm = stations[stations.obtain("airep", Coords(44.0, 11.0), "LH1234")];
            wassert(actual(stm.coords.dlat()) == 44.0);
            wassert(actual(stm.coords.dlon()) == 11.0);
            wassert(actual(stm.ident) == "LH1234");
            wassert(actual(stm.report) == "airep");

            // Check that lookup returns the same element
            const Station& stf1 = stations[stations.obtain("synop", Coords(44.0, 11.0), Ident())];
            wassert(actual(stf1.ana_id) == stf.ana_id);
            const Station& stm1 = stations[stations.obtain("airep", Coords(44.0, 11.0), "LH1234")];
            wassert(actual(stm1.ana_id) == stm.ana_id);

            // Check again, looking up records
            core::Record sfrec;
            sfrec.set("lat", 44.0);
            sfrec.set("lon", 11.0);
            sfrec.set("rep_memo", "synop");
            const Station& stf2 = stations[stations.obtain(sfrec)];
            wassert(actual(stf2.ana_id) == stf.ana_id);

            core::Record smrec;
            smrec.set("lat", 44.0);
            smrec.set("lon", 11.0);
            smrec.set("ident", "LH1234");
            smrec.set("rep_memo", "airep");
            const Station& stm2 = stations[stations.obtain(smrec)];
            wassert(actual(stm2.ana_id) == stm.ana_id);
        });
        add_method("query_ana_id", []() {
            // Query by ana_id
            db::mem::Stations stations;
            int pos = stations.obtain("synop", Coords(44.0, 11.0), Ident());

            core::Query query;

            {
                query.ana_id = pos;
                unordered_set<int> items = stations.query(query);
                wassert(actual(items.size()) == 1);
                wassert(actual(*items.begin()) == pos);
            }

            {
                query.ana_id = 100;
                unordered_set<int> items = stations.query(query);
                wassert(actual(items.empty()).istrue());
            }

            stations.obtain("synop", Coords(45.0, 12.0), Ident());

            {
                query.ana_id = pos;
                unordered_set<int> items = stations.query(query);
                wassert(actual(items.size()) == 1);
                wassert(actual(*items.begin()) == pos);
            }
        });
        add_method("query_latlon", []() {
            // Query by lat,lon
            db::mem::Stations stations;
            int pos = stations.obtain("synop", Coords(44.0, 11.0), Ident());
            stations.obtain("synop", Coords(45.0, 12.0), Ident());

            unordered_set<int> items = stations.query(tests::core_query_from_string("lat=44.0, lon=11.0"));
            wassert(actual(items.size()) == 1);
            wassert(actual(*items.begin()) == pos);
        });
        add_method("query_all", []() {
            // Query everything
            db::mem::Stations stations;
            int pos1 = stations.obtain("synop", Coords(44.0, 11.0), Ident());
            int pos2 = stations.obtain("synop", Coords(45.0, 12.0), Ident());

            unordered_set<int> items = stations.query(core::Query());
            wassert(actual(items.size()) == 2);
        });
        add_method("query_multi_latitudes", []() {
            // Query latitudes matching multiple index entries
            db::mem::Stations stations;
            int pos1 = stations.obtain("synop", Coords(44.0, 11.0), Ident());
            int pos2 = stations.obtain("synop", Coords(45.0, 11.0), Ident());
            int pos3 = stations.obtain("synop", Coords(46.0, 11.0), Ident());

            unordered_set<int> items = stations.query(tests::core_query_from_string("latmin=45.0"));
            wassert(actual(items.size()) == 2);
            wassert(actual(items.find(pos2) != items.end()));
            wassert(actual(items.find(pos3) != items.end()));
        });
    }
} test("db_mem_station");

}
