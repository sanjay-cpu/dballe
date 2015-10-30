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
            const db::mem::Station& stf = stations[stations.obtain_fixed(Coords(44.0, 11.0), "synop")];
            wassert(actual(stf.id) == 0);
            wassert(actual(stf.coords.dlat()) == 44.0);
            wassert(actual(stf.coords.dlon()) == 11.0);
            wassert(actual(stf.ident) == nullptr);
            wassert(actual(stf.report) == "synop");

            // Insert a mobile station and check that all data is there
            const db::mem::Station& stm = stations[stations.obtain_mobile(Coords(44.0, 11.0), "LH1234", "airep")];
            wassert(actual(stm.coords.dlat()) == 44.0);
            wassert(actual(stm.coords.dlon()) == 11.0);
            wassert(actual(stm.ident) == "LH1234");
            wassert(actual(stm.report) == "airep");

            // Check that lookup returns the same element
            const db::mem::Station& stf1 = stations[stations.obtain_fixed(Coords(44.0, 11.0), "synop")];
            wassert(actual(stf1.id) == stf.id);
            const db::mem::Station& stm1 = stations[stations.obtain_mobile(Coords(44.0, 11.0), "LH1234", "airep")];
            wassert(actual(stm1.id) == stm.id);

            // Check again, looking up records
            core::Record sfrec;
            sfrec.set("lat", 44.0);
            sfrec.set("lon", 11.0);
            sfrec.set("rep_memo", "synop");
            const db::mem::Station& stf2 = stations[stations.obtain(sfrec)];
            wassert(actual(stf2.id) == stf.id);

            core::Record smrec;
            smrec.set("lat", 44.0);
            smrec.set("lon", 11.0);
            smrec.set("ident", "LH1234");
            smrec.set("rep_memo", "airep");
            const db::mem::Station& stm2 = stations[stations.obtain(smrec)];
            wassert(actual(stm2.id) == stm.id);
        });
        add_method("query_ana_id", []() {
            // Query by ana_id
            db::mem::Stations stations;
            size_t pos = stations.obtain_fixed(Coords(44.0, 11.0), "synop");

            core::Query query;

            {
                query.ana_id = pos;
                unordered_set<size_t> items = stations.query(query);
                wassert(actual(items.size()) == 1);
                wassert(actual(*items.begin()) == pos);
            }

            {
                query.ana_id = 100;
                unordered_set<size_t> items = stations.query(query);
                wassert(actual(items.empty()).istrue());
            }

            stations.obtain_fixed(Coords(45.0, 12.0), "synop");

            {
                query.ana_id = pos;
                unordered_set<size_t> items = stations.query(query);
                wassert(actual(items.size()) == 1);
                wassert(actual(*items.begin()) == pos);
            }
        });
        add_method("query_latlon", []() {
            // Query by lat,lon
            db::mem::Stations stations;
            size_t pos = stations.obtain_fixed(Coords(44.0, 11.0), "synop");
            stations.obtain_fixed(Coords(45.0, 12.0), "synop");

            unordered_set<size_t> items = stations.query(tests::core_query_from_string("lat=44.0, lon=11.0"));
            wassert(actual(items.size()) == 1);
            wassert(actual(*items.begin()) == pos);
        });
        add_method("query_all", []() {
            // Query everything
            db::mem::Stations stations;
            size_t pos1 = stations.obtain_fixed(Coords(44.0, 11.0), "synop");
            size_t pos2 = stations.obtain_fixed(Coords(45.0, 12.0), "synop");

            unordered_set<size_t> items = stations.query(core::Query());
            wassert(actual(items.size()) == 2);
        });
        add_method("query_multi_latitudes", []() {
            // Query latitudes matching multiple index entries
            db::mem::Stations stations;
            size_t pos1 = stations.obtain_fixed(Coords(44.0, 11.0), "synop");
            size_t pos2 = stations.obtain_fixed(Coords(45.0, 11.0), "synop");
            size_t pos3 = stations.obtain_fixed(Coords(46.0, 11.0), "synop");

            unordered_set<size_t> items = stations.query(tests::core_query_from_string("latmin=45.0"));
            wassert(actual(items.size()) == 2);
            wassert(actual(items.find(pos2) != items.end()));
            wassert(actual(items.find(pos3) != items.end()));
        });
    }
} test("db_mem_station");

}
