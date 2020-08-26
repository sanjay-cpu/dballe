#include "dballe/msg/msg.h"
#include "dballe/db/tests.h"
#include "dballe/core/error.h"
#include "dballe/db.h"
#include "v7/repinfo.h"
#include "v7/db.h"
#include "v7/transaction.h"
#include "config.h"
#include <cstring>
#include <unistd.h>
#include <wreport/utils/subprocess.h>

using namespace dballe;
using namespace dballe::db;
using namespace dballe::tests;
using namespace wreport;
using namespace std;

namespace {

template<typename DB>
class Tests : public FixtureTestCase<EmptyTransactionFixture<DB>>
{
    typedef EmptyTransactionFixture<DB> Fixture;
    using FixtureTestCase<Fixture>::FixtureTestCase;

    void register_tests() override;
};

template<typename DB>
class CommitTests : public FixtureTestCase<DBFixture<DB>>
{
    typedef DBFixture<DB> Fixture;
    using FixtureTestCase<Fixture>::FixtureTestCase;

    void register_tests() override;
};

Tests<V7DB> tg2("db_basic_tr_v7_sqlite", "SQLITE");
#ifdef HAVE_LIBPQ
Tests<V7DB> tg4("db_basic_tr_v7_postgresql", "POSTGRESQL");
#endif
#ifdef HAVE_MYSQL
Tests<V7DB> tg6("db_basic_tr_v7_mysql", "MYSQL");
#endif

CommitTests<V7DB> ct2("db_basic_db_v7_sqlite", "SQLITE");
#ifdef HAVE_LIBPQ
CommitTests<V7DB> ct4("db_basic_db_v7_postgresql", "POSTGRESQL");
#endif
#ifdef HAVE_MYSQL
CommitTests<V7DB> ct6("db_basic_db_v7_mysql", "MYSQL");
#endif

template<typename DB>
void Tests<DB>::register_tests()
{

this->add_method("repinfo", [](Fixture& f) {
    // Test repinfo-related functions
    std::map<std::string, int> prios = f.tr->repinfo().get_priorities();
    wassert(actual(prios.find("synop") != prios.end()).istrue());
    wassert(actual(prios["synop"]) == 101);

    int added, deleted, updated;
    f.tr->update_repinfo((string(getenv("DBA_TESTDATA")) + "/test-repinfo1.csv").c_str(), &added, &deleted, &updated);

    wassert(actual(added) == 3);
    wassert(actual(deleted) == 11);
    wassert(actual(updated) == 2);

    prios = f.tr->repinfo().get_priorities();
    wassert(actual(prios.find("fixspnpo") != prios.end()).istrue());
    wassert(actual(prios["fixspnpo"]) == 200);
});

this->add_method("simple", [](Fixture& f) {
    // Test remove_all
    f.tr->remove_all();
    std::unique_ptr<Cursor> cur = f.tr->query_data(core::Query());
    wassert(actual(cur->remaining()) == 0);

    // Check that it is idempotent
    f.tr->remove_all();
    cur = f.tr->query_data(core::Query());
    wassert(actual(cur->remaining()) == 0);

    // Insert something
    OldDballeTestDataSet data_set;
    wassert(f.populate(data_set));

    cur = f.tr->query_data(core::Query());
    wassert(actual(cur->remaining()) == 4);

    f.tr->remove_all();

    cur = f.tr->query_data(core::Query());
    wassert(actual(cur->remaining()) == 0);
});

this->add_method("stationdata", [](Fixture& f) {
    // Test adding station data for different networks

    // Insert two values in two networks
    core::Data vals;
    vals.station.coords = Coords(12.077, 44.600);
    vals.station.report = "synop";
    vals.level = Level(103, 2000);
    vals.trange = Trange::instant();
    vals.datetime = Datetime(2014, 1, 1, 0, 0, 0);
    vals.values.set("B12101", 273.15);
    f.tr->insert_data(vals);
    vals.clear_ids();
    vals.station.report = "temp";
    vals.values.set("B12101", 274.15);
    f.tr->insert_data(vals);

    // Insert station names in both networks
    core::Data svals_camse;
    svals_camse.station.coords = vals.station.coords;
    svals_camse.station.report = "synop";
    svals_camse.values.set("B01019", "Camse");
    f.tr->insert_station_data(svals_camse);
    core::Data svals_esmac;
    svals_esmac.station.coords = vals.station.coords;
    svals_esmac.station.report = "temp";
    svals_esmac.values.set("B01019", "Esmac");
    f.tr->insert_station_data(svals_esmac);

    // Query back all the data
    auto cur = f.tr->query_stations(core::Query());

    // Check results
    switch (DB::format)
    {
        case Format::V7:
        {
            bool have_temp = false;
            bool have_synop = false;

            // For v7 databases, we get one record per (station, network)
            // combination
            for (unsigned i = 0; i < 2; ++i)
            {
                wassert(actual(cur->next()).istrue());
                DBStation station = cur->get_station();
                DBValues values = cur->get_values();
                if (station.report == "temp")
                {
                    wassert(actual(station.id) == svals_esmac.station.id);
                    wassert(actual(values.var(WR_VAR(0, 1, 19))) == "Esmac");
                    have_temp = true;
                } else if (station.report == "synop") {
                    wassert(actual(station.id) == svals_camse.station.id);
                    wassert(actual(values.var(WR_VAR(0, 1, 19))) == "Camse");
                    have_synop = true;
                }
            }
            wassert(actual(cur->next()).isfalse());
            wassert(actual(have_temp).istrue());
            wassert(actual(have_synop).istrue());
            break;
        }
        default: throw error_unimplemented("testing stations_without_data on unsupported database");
    }

    impl::Messages msgs;
    auto cursor = f.tr->query_messages(core::Query());
    while (cursor->next())
        msgs.push_back(cursor->get_message());
    wassert(actual(msgs.size()) == 2);

    //msgs.print(stderr);

    wassert(actual(impl::Message::downcast(msgs[0])->get_rep_memo_var()->enqc()) == "synop");
    wassert(actual(impl::Message::downcast(msgs[0])->get_st_name_var()->enqc()) == "Camse");
    wassert(actual(impl::Message::downcast(msgs[0])->get_temp_2m_var()->enqd()) == 273.15);
    wassert(actual(impl::Message::downcast(msgs[1])->get_rep_memo_var()->enqc()) == "temp");
    wassert(actual(impl::Message::downcast(msgs[1])->get_st_name_var()->enqc()) == "Esmac");
    wassert(actual(impl::Message::downcast(msgs[1])->get_temp_2m_var()->enqd()) == 274.15);
});

this->add_method("query_ident", [](Fixture& f) {
    // Insert a mobile station
    core::Data vals;
    vals.station.report = "synop";
    vals.station.coords = Coords(44.10, 11.50);
    vals.station.ident = "foo";
    vals.level = Level(1);
    vals.trange = Trange::instant();
    vals.datetime = Datetime(2015, 4, 25, 12, 30, 45);
    vals.values.set("B12101", 295.1);
    f.tr->insert_data(vals);

    wassert(actual(f.tr).try_station_query("ident=foo", 1));
    wassert(actual(f.tr).try_station_query("ident=bar", 0));
    wassert(actual(f.tr).try_station_query("mobile=1", 1));
    wassert(actual(f.tr).try_station_query("mobile=0", 0));
    wassert(actual(f.tr).try_data_query("ident=foo", 1));
    wassert(actual(f.tr).try_data_query("ident=bar", 0));
    wassert(actual(f.tr).try_data_query("mobile=1", 1));
    wassert(actual(f.tr).try_data_query("mobile=0", 0));
});

this->add_method("ident_case", [](Fixture& f) {
    // Insert a mobile station
    core::Data vals;
    vals.station.report = "synop";
    vals.station.coords = Coords(44.10, 11.50);
    vals.station.ident = "TeSt";
    vals.level = Level(1);
    vals.trange = Trange::instant();
    vals.datetime = Datetime(2015, 4, 25, 12, 30, 45);
    vals.values.set("B12101", 295.1);
    f.tr->insert_data(vals);

    wassert(actual(f.tr).try_station_query("ident=test", 0));
    wassert(actual(f.tr).try_station_query("ident=TeSt", 1));
    wassert(actual(f.tr).try_station_query("ident=TEST", 0));
});

this->add_method("ident_encoding", [](Fixture& f) {
    // Insert a mobile station
    core::Data vals;
    vals.station.report = "synop";
    vals.station.coords = Coords(44.10, 11.50);
    vals.station.ident = "🍕";
    vals.level = Level(1);
    vals.trange = Trange::instant();
    vals.datetime = Datetime(2015, 4, 25, 12, 30, 45);
    vals.values.set("B12101", 295.1);
    f.tr->insert_data(vals);

    wassert(actual(f.tr).try_station_query("ident=🍕", 1));
    wassert(actual(f.tr).try_station_query("ident=?", 0));
    wassert(actual(f.tr).try_station_query("ident=test", 0));
});

this->add_method("missing_repmemo", [](Fixture& f) {
    // Test querying with a missing rep_memo
    core::Query query;
    query.report = "nonexisting";
    wassert(actual(f.tr->query_stations(query)->remaining()) == 0);
    wassert(actual(f.tr->query_station_data(query)->remaining()) == 0);
    wassert(actual(f.tr->query_data(query)->remaining()) == 0);
    wassert(actual(f.tr->query_summary(query)->remaining()) == 0);
});

this->add_method("update_with_ana_id", [](Fixture& f) {
    {
        core::Data vals;
        vals.station.report = "synop";
        vals.station.coords = Coords(44.10, 11.50);
        vals.station.ident = "foo";
        vals.level = Level(1);
        vals.trange = Trange::instant();
        vals.datetime = Datetime(2015, 4, 25, 12, 30, 45);
        vals.values.set("B12101", 295.1);
        f.tr->insert_data(vals);
    }

    core::Query query;
    auto cur = f.tr->query_stations(query);
    wassert(actual(cur->remaining()) == 1u);
    wassert(cur->next());
    int ana_id = cur->get_station().id;

    // Replace by ana_id
    {
        core::Data vals;
        vals.station.id = ana_id;
        vals.level = Level(1);
        vals.trange = Trange::instant();
        vals.datetime = Datetime(2015, 4, 25, 12, 30, 45);
        vals.values.set("B12101", 296.2);
        impl::DBInsertOptions opts;
        opts.can_replace = true; opts.can_add_stations = false;
        wassert(f.tr->insert_data(vals, opts));
    }

    auto dcur = f.tr->query_data(query);
    wassert(actual(dcur->remaining()) == 1u);
    wassert(dcur->next());

    auto var = dcur->get_var();
    wassert(actual(var.code()) == WR_VAR(0, 12, 101));
    wassert(actual(var.enqd()) == 296.2);

    // Remove by ana_id
    query.clear();
    query.ana_id = ana_id;
    query.level = Level(1);
    query.trange = Trange::instant();
    wassert(f.tr->remove_data(query));

    query.clear();
    dcur = f.tr->query_data(query);
    wassert(actual(dcur->remaining()) == 0u);

    // Insert by ana_id
    {
        core::Data vals;
        vals.station.id = ana_id;
        vals.level = Level(1);
        vals.trange = Trange::instant();
        vals.datetime = Datetime(2015, 4, 25, 12, 30, 45);
        vals.values.set("B12101", 296.2);
        impl::DBInsertOptions opts;
        opts.can_replace = false; opts.can_add_stations = false;
        wassert(f.tr->insert_data(vals, opts));
    }

    dcur = f.tr->query_data(query);
    wassert(actual(dcur->remaining()) == 1u);
    wassert(dcur->next());

    var = dcur->get_var();
    wassert(actual(var.code()) == WR_VAR(0, 12, 101));
    wassert(actual(var.enqd()) == 296.2);
});

this->add_method("discriminate_ana_id", [](Fixture& f) {
    // Generate two station IDs
    core::Data vals1;
    vals1.station.report = "synop";
    vals1.station.coords = Coords(44.10, 11.50);
    vals1.values.set("B01001", 10);
    f.tr->insert_station_data(vals1);

    core::Data vals2;
    vals2.station.report = "metar";
    vals2.station.coords = Coords(44.10, 11.50);
    vals2.values.set("B01001", 11);
    f.tr->insert_station_data(vals2);

    wassert(actual(vals1.station.id) != vals2.station.id);

    // Insert by ana_id on both stations
    {
        core::Data vals;
        vals.station.id = vals1.station.id;
        vals.values.set("B01002", 101);
        impl::DBInsertOptions opts;
        opts.can_replace = false; opts.can_add_stations = false;
        wassert(f.tr->insert_station_data(vals, opts));
    }

    {
        core::Data vals;
        vals.station.id = vals2.station.id;
        vals.values.set("B01002", 102);
        impl::DBInsertOptions opts;
        opts.can_replace = false; opts.can_add_stations = false;
        wassert(f.tr->insert_station_data(vals, opts));
    }

    core::Query query;
    auto cur = f.tr->query_station_data(query);
    wassert(actual(cur->remaining()) == 4u);

    wassert(cur->next());
    wassert(actual(cur->get_station().id) == vals1.station.id);
    wassert(actual(cur->get_var().code()) == WR_VAR(0, 1, 1));
    wassert(actual(cur->get_var().enqd()) == 10);

    wassert(cur->next());
    wassert(actual(cur->get_station().id) == vals1.station.id);
    wassert(actual(cur->get_var().code()) == WR_VAR(0, 1, 2));
    wassert(actual(cur->get_var().enqd()) == 101);

    wassert(cur->next());
    wassert(actual(cur->get_station().id) == vals2.station.id);
    wassert(actual(cur->get_var().code()) == WR_VAR(0, 1, 1));
    wassert(actual(cur->get_var().enqd()) == 11);

    wassert(cur->next());
    wassert(actual(cur->get_station().id) == vals2.station.id);
    wassert(actual(cur->get_var().code()) == WR_VAR(0, 1, 2));
    wassert(actual(cur->get_var().enqd()) == 102);
});

this->add_method("foreign_ana_id", [](Fixture& f) {
    // Insert a station data
    core::Data vals1;
    vals1.station.report = "synop";
    vals1.station.coords = Coords(44.10, 11.50);
    vals1.values.set("B01001", 10);
    wassert(f.tr->insert_station_data(vals1));

    // Insert a station data with an ana_id from another datatabase
    core::Data vals2;
    vals2.station.id = vals1.station.id + 42;
    vals2.station.report = "synop";
    vals2.station.coords = Coords(44.10, 11.50);
    vals2.values.set("B01002", 100);
    wassert(f.tr->insert_station_data(vals2));

    // The right ana_id has been picked
    wassert(actual(vals1.station.id) == vals2.station.id);
});

this->add_method("delete_by_var", [](Fixture& f) {
    // See issue #141
    {
        core::Data vals;
        vals.station.report = "synop";
        vals.station.coords = Coords(44.10, 11.50);
        vals.station.ident = "foo";
        vals.level = Level(1);
        vals.trange = Trange::instant();
        vals.datetime = Datetime(2015, 4, 25, 12, 30, 45);
        vals.values.set("B12101", 295.1);
        vals.values.set("B12103", 295.2);
        f.tr->insert_data(vals);
        f.tr->insert_station_data(vals);
    }

    // Try deleting station data
    {
        auto cur = f.tr->query_station_data(core::Query());
        wassert(actual(cur->remaining()) == 2u);
    }

    {
        core::Query query;
        query.varcodes.insert(WR_VAR(0, 12, 101));
        f.tr->remove_station_data(query);
    }

    {
        auto cur = f.tr->query_station_data(core::Query());
        wassert(actual(cur->remaining()) == 1u);
        wassert_true(cur->next());
        wassert(actual(cur->get_varcode()) == WR_VAR(0, 12, 103));
    }

    // Try deleting normal data
    {
        auto cur = f.tr->query_data(core::Query());
        wassert(actual(cur->remaining()) == 2u);
    }

    {
        core::Query query;
        query.varcodes.insert(WR_VAR(0, 12, 101));
        f.tr->remove_data(query);
    }

    {
        auto cur = f.tr->query_data(core::Query());
        wassert(actual(cur->remaining()) == 1u);
        wassert_true(cur->next());
        wassert(actual(cur->get_varcode()) == WR_VAR(0, 12, 103));
    }
});

}

template<typename DB>
void CommitTests<DB>::register_tests() {

// Test simple queries
this->add_method("reset", [](Fixture& f) {
    // Run twice to see if it is idempotent
    auto& db = *f.db;
    db.reset();
    db.reset();
});

this->add_method("vacuum", [](Fixture& f) {
    TestDataSet data;
    data.stations["s1"].station.report = "synop";
    data.stations["s1"].station.coords = Coords(12.34560, 76.54320);
    data.stations["s1"].values.set("B01019", "Station 1");

    data.stations["s2"].station.report = "metar";
    data.stations["s2"].station.coords = Coords(23.45670, 65.43210);
    data.stations["s2"].values.set("B01019", "Station 2");

    data.data["s1"].station = data.stations["s1"].station;
    data.data["s1"].level = Level(10, 11, 15, 22);
    data.data["s1"].trange = Trange(20, 111, 122);
    data.data["s1"].datetime = Datetime(1945, 4, 25, 8);
    data.data["s1"].values.set("B01011", "Data 1");

    data.data["s2"].station = data.stations["s2"].station;
    data.data["s2"].level = Level(10, 11, 15, 22);
    data.data["s2"].trange = Trange(20, 111, 122);
    data.data["s2"].datetime = Datetime(1945, 4, 25, 8);
    data.data["s2"].values.set("B01011", "Data 2");

    // Insert some data
    wassert(f.populate_database(data));

    // Invoke vacuum
    auto& db = *f.db;
    wassert(db.vacuum());

    // Stations are still there
    {
        core::Query q;
        auto c = db.query_stations(q);
        wassert(actual(c->remaining()) == 2);
    }

    // Delete all measured values, but not station values
    {
        core::Query q;
        q.ana_id = data.stations["s1"].station.id;
        db.remove_data(q);
    }

    {
        core::Query q;

        // Stations are still there before vacuum
        auto c = db.query_stations(q);
        wassert(actual(c->remaining()) == 2);
    }

    // Invoke vacuum
    wassert(db.vacuum());

    // Station 1 is gone
    {
        core::Query q;
        q.ana_id = data.stations["s1"].station.id;
        auto c = db.query_stations(q);
        wassert(actual(c->remaining()) == 0);
    }

    // Station 2 is still there with all its data
    {
        auto tr = db.transaction();
        core::Query q;
        q.ana_id = data.stations["s2"].station.id;
        auto c = wcallchecked(tr->query_stations(q));
        wassert(actual(c->remaining()) == 1);

        auto sd = wcallchecked(tr->query_station_data(q));
        wassert(actual(sd->remaining()) == 1);

        auto dd = wcallchecked(tr->query_data(q));
        wassert(actual(dd->remaining()) == 1);
    }
});

// Test simple queries
this->add_method("wipe", [](Fixture& f) {
    // We are connected to an empty database

    // Insert some data
    core::Data data;
    data.station.report = "synop";
    data.station.coords = Coords(12.34560, 76.54320);
    data.values.set("B01019", "Station 1");
    wassert(f.db->insert_station_data(data));

    // Disconnect
    f.db.reset();

    // Reconnect with wipe
    auto db = DB::create_db(f.backend, true);

    // Test that is is empty
    wassert(actual(db->query_station_data(core::Query())->remaining()) == 0u);

    // Reinsert
    data.clear_ids();
    wassert(db->insert_station_data(data));

    // Disconnect
    db.reset();

    // Reconnect without wipe
    db = DB::create_db(f.backend, false);

    // Test that the data is there
    wassert(actual(db->query_station_data(core::Query())->remaining()) == 1u);
});

this->add_method("transaction_create_error", [](Fixture& f) {
    f.destroys_db = true;
    f.db->disappear();
    // TODO: update wobble to use std::current_exception in wassert_throws
    try {
        auto t = f.db->transaction();
        throw TestFailed("db->transaction() should throw");
    } catch (dballe::error_db& e) {
        wassert(actual(e.what()).matches("^cannot compile query|relation \"repinfo\" does not exist|Table 'test\\.repinfo' doesn't exist"));
    }

    try {
        auto t = f.db->transaction();
        throw TestFailed("db->transaction() should throw");
    } catch (dballe::error_db& e) {
        wassert(actual(e.what()).matches("^cannot compile query|relation \"repinfo\" does not exist|Table 'test\\.repinfo' doesn't exist"));
    }
});

this->add_method("transactions_after_fork", [](Fixture& f) {
    class TestChild: public subprocess::Child
    {
    protected:
        std::shared_ptr<dballe::DB> db;

        int main() noexcept override
        {
            try {
                auto t = db->transaction();
                fprintf(stderr, "Opening a transaction in forked process unexpectedly succeeded\n");
                return 1;
            } catch (std::exception& e) {
                std::string msg(e.what());
                if (msg.find("database connections cannot be used after forking") != std::string::npos)
                    return 0;
                fprintf(stderr, "Unexpected error starting a transaction in test child: %s", e.what());
                return 2;
            }
        }

    public:
        TestChild(std::shared_ptr<dballe::DB> db)
            : db(db)
        {
        }
    };

    TestChild child1(f.db);
    TestChild child2(f.db);

    child1.fork();
    child2.fork();

    wassert(actual(child1.wait()) == 0);
    wassert(actual(child2.wait()) == 0);

    // Try to use the DB after the child processes ended
    f.db->transaction();
});

}

}
