#include "core/tests.h"
#include "internals.h"

using namespace std;
using namespace dballe;
using namespace dballe::db;
using namespace dballe::tests;
using namespace wreport;

namespace {

struct SQLiteFixture : Fixture
{
    SQLiteConnection conn;

    SQLiteFixture()
    {
        conn.open_memory();
    }

    void test_setup()
    {
        Fixture::test_setup();
        conn.drop_table_if_exists("dballe_test");
        conn.exec("CREATE TABLE dballe_test (val INTEGER NOT NULL)");
    }
};

class Tests : public FixtureTestCase<SQLiteFixture>
{
    using FixtureTestCase::FixtureTestCase;

    void register_tests() override
    {
        add_method("query_int", [](Fixture& f) {
            // Test querying int values
            f.conn.exec("INSERT INTO dballe_test VALUES (1)");
            f.conn.exec("INSERT INTO dballe_test VALUES (2)");

            auto s = f.conn.sqlitestatement("SELECT val FROM dballe_test");

            int val = 0;
            unsigned count = 0;
            s->execute([&]() {
                val += s->column_int(0);
                ++count;
            });
            wassert(actual(count) == 2);
            wassert(actual(val) == 3);
        });
        add_method("query_int_null", [](Fixture& f) {
            // Test querying int values, with potential NULLs
            f.conn.exec("CREATE TABLE dballe_testnull (val INTEGER)");
            f.conn.exec("INSERT INTO dballe_testnull VALUES (NULL)");
            f.conn.exec("INSERT INTO dballe_testnull VALUES (42)");

            auto s = f.conn.sqlitestatement("SELECT val FROM dballe_testnull");

            int val = 0;
            unsigned count = 0;
            unsigned countnulls = 0;
            s->execute([&]() {
                if (s->column_isnull(0))
                    ++countnulls;
                else
                    val += s->column_int(0);
                ++count;
            });

            wassert(actual(val) == 42);
            wassert(actual(count) == 2);
            wassert(actual(countnulls) == 1);
        });
        add_method("query_unsigned", [](Fixture& f) {
            // Test querying unsigned values
            f.conn.exec("INSERT INTO dballe_test VALUES (0xFFFFFFFE)");

            auto s = f.conn.sqlitestatement("SELECT val FROM dballe_test");

            unsigned val = 0;
            unsigned count = 0;
            s->execute([&]() {
                val += s->column_int64(0);
                ++count;
            });
            wassert(actual(count) == 1);
            wassert(actual(val) == 0xFFFFFFFE);
        });
        add_method("query_unsigned_short", [](Fixture& f) {
            // Test querying unsigned short values
            char buf[200];
            snprintf(buf, 200, "INSERT INTO dballe_test VALUES (%d)", (int)WR_VAR(3, 1, 12));
            f.conn.exec(buf);

            auto s = f.conn.sqlitestatement("SELECT val FROM dballe_test");

            Varcode val = 0;
            unsigned count = 0;
            s->execute([&]() {
                val = (Varcode)s->column_int(0);
                ++count;
            });
            wassert(actual(count) == 1);
            wassert(actual(val) == WR_VAR(3, 1, 12));
        });
        add_method("query_has_tables", [](Fixture& f) {
            // Test has_tables
            wassert(actual(f.conn.has_table("this_should_not_exist")).isfalse());
            wassert(actual(f.conn.has_table("dballe_test")).istrue());
        });
        add_method("query_settings", [](Fixture& f) {
            // Test settings
            f.conn.drop_table_if_exists("dballe_settings");
            wassert(actual(f.conn.has_table("dballe_settings")).isfalse());

            wassert(actual(f.conn.get_setting("test_key")) == "");

            f.conn.set_setting("test_key", "42");
            wassert(actual(f.conn.has_table("dballe_settings")).istrue());

            wassert(actual(f.conn.get_setting("test_key")) == "42");
        });
        add_method("auto_increment", [](Fixture& f) {
            // Test auto_increment
            f.conn.exec("CREATE TABLE dballe_testai (id INTEGER PRIMARY KEY, val INTEGER)");
            f.conn.exec("INSERT INTO dballe_testai (val) VALUES (42)");
            wassert(actual(f.conn.get_last_insert_id()) == 1);
            f.conn.exec("INSERT INTO dballe_testai (val) VALUES (43)");
            wassert(actual(f.conn.get_last_insert_id()) == 2);
        });
    }
};

Tests tests("db_internals_sqlite");

}