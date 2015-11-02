#include "db/tests.h"
#include "dballe/var.h"
#include "station.h"
#include "value.h"

using namespace dballe;
using namespace dballe::tests;
using namespace std;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override
    {
        add_method("insert", []() {
            db::mem::Stations stations;
            const Station& stf = stations[stations.obtain("synop", Coords(44.0, 11.0), Ident())];

            Level level(1);
            Trange trange(Trange::instant());
            Datetime datetime(2013, 10, 30, 23);

            // Insert a station value and check that all data is there
            db::mem::DataValues values;
            int data_id = values.insert(newvar(WR_VAR(0, 12, 101), 28.5), true, stf.ana_id, datetime, level, trange);
            wassert(actual(data_id) >= 0);
            wassert(actual(data_id) < values.variables.size());
            wassert(actual(values.variables[data_id]) == 28.5);

            // Replacing a value should reuse an existing one
            int data_id1 = values.insert(newvar(WR_VAR(0, 12, 101), 29.5), true, stf.ana_id, datetime, level, trange);
            wassert(actual(data_id1) == data_id);
            wassert(actual(values.variables[data_id]) == 29.5);

            // Reinserting a value with replace=false should fail
            try {
                values.insert(newvar(WR_VAR(0, 12, 101), 30.5), false, stf.ana_id, datetime, level, trange);
                wassert(throw TestFailed("Reinserting a value with replace=false should fail"));
            } catch (wreport::error_consistency&) {
            }
        });
    }
} test("db_mem_value");

}
