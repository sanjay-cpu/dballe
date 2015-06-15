#include "memdb/tests.h"
#include "results.h"
#include "station.h"

using namespace std;
using namespace wibble::tests;
using namespace dballe;

namespace {

struct Fixture : public dballe::tests::Fixture
{
    memdb::Stations stations;
    size_t pos[10];

    Fixture()
    {
        // 10 stations in a line from latitude 40.0 to 50.0
        for (unsigned i = 0; i < 10; ++i)
            pos[i] = stations.obtain_fixed(Coords(40.0 + i, 11.0), "synop");
    }
};

typedef dballe::tests::test_group<Fixture> test_group;
typedef test_group::Test Test;

std::vector<Test> tests {
    Test("all", [](Fixture& f) {
        // Test not performing any selection: all should be selected
        memdb::Results<memdb::Station> res(f.stations);

        wassert(actual(res.is_select_all()).istrue());
        wassert(actual(res.is_empty()).isfalse());

        vector<const memdb::Station*> items;
        res.copy_valptrs_to(back_inserter(items));
        wassert(actual(items.size()) == 10);
    }),
    Test("none", [](Fixture& f) {
        // Test setting to no results
        memdb::Results<memdb::Station> res(f.stations);
        res.set_to_empty();

        wassert(actual(res.is_select_all()).isfalse());
        wassert(actual(res.is_empty()).istrue());

        vector<const memdb::Station*> items;
        res.copy_valptrs_to(back_inserter(items));
        wassert(actual(items.size()) == 0);
    }),
    Test("single", [](Fixture& f) {
        // Test selecting a singleton
        memdb::Results<memdb::Station> res(f.stations);
        res.add_singleton(f.pos[0]);

        wassert(actual(res.is_select_all()).isfalse());
        wassert(actual(res.is_empty()).isfalse());

        vector<const memdb::Station*> items;
        res.copy_valptrs_to(back_inserter(items));
        wassert(actual(items.size()) == 1);
        wassert(actual(items[0]->id) == f.pos[0]);
    }),
};

test_group newtg("memdb_results", tests);

}

#include "results.tcc"
