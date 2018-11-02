#include "dbadb.h"
#include "dballe/message.h"
#include "dballe/msg/msg.h"
#include "dballe/db/db.h"

#include <cstdlib>

using namespace wreport;
using namespace std;

// extern int op_verbose;

namespace dballe {
namespace cmdline {

namespace {

struct Importer : public Action
{
    dballe::DB& db;
    DBImportMessageOptions opts;
    std::shared_ptr<dballe::Transaction> transaction;

    Importer(db::DB& db) : db(db) {}

    virtual bool operator()(const cmdline::Item& item);
    void commit()
    {
        if (transaction.get())
            transaction->commit();
    }
};

bool Importer::operator()(const Item& item)
{
    if (!transaction.get())
        transaction = dynamic_pointer_cast<db::Transaction>(db.transaction());

    if (item.msgs == NULL)
    {
        fprintf(stderr, "Message #%d cannot be parsed: ignored\n", item.idx);
        return false;
    }
    try {
        transaction->import_messages(*item.msgs, opts);
    } catch (std::exception& e) {
        item.processing_failed(e);
    }
    return true;
}

}

/// Query data in the database and output results as arbitrary human readable text
int Dbadb::do_dump(const Query& query, FILE* out)
{
    auto tr = dynamic_pointer_cast<db::Transaction>(db.transaction());
    unique_ptr<db::Cursor> cursor = tr->query_data(query);

    auto res = Record::create();
    for (unsigned i = 0; cursor->next(); ++i)
    {
        cursor->to_record(*res);
        fprintf(out, "#%u: -----------------------\n", i);
        res->print(out);
    }

    tr->rollback();
    return 0;
}

/// Query stations in the database and output results as arbitrary human readable text
int Dbadb::do_stations(const Query& query, FILE* out)
{
    auto tr = dynamic_pointer_cast<db::Transaction>(db.transaction());
    unique_ptr<db::Cursor> cursor = tr->query_stations(query);

    auto res = Record::create();
    for (unsigned i = 0; cursor->next(); ++i)
    {
        cursor->to_record(*res);
        fprintf(out, "#%u: -----------------------\n", i);
        res->print(out);
    }

    tr->rollback();
    return 0;
}

int Dbadb::do_export_dump(const Query& query, FILE* out)
{
    db.export_msgs(query, [&](unique_ptr<Message>&& msg) {
        msg->print(out);
        return true;
    });
    return 0;
}

int Dbadb::do_import(const list<string>& fnames, Reader& reader, const DBImportMessageOptions& opts)
{
    Importer importer(db);
    importer.opts = opts;
    reader.read(fnames, importer);
    importer.commit();
    if (reader.verbose)
        fprintf(stderr, "%u messages successfully imported, %u messages skipped\n", reader.count_successes, reader.count_failures);

    // As discussed in #101, if there are both successes and failures, return
    // success only if --rejected has been used, because in that case the
    // caller can check the size of the rejected file to detect the difference
    // between a mixed result and a complete success.
    // One can use --rejected=/dev/null to ignore partial failures.
    if (!reader.count_failures)
        return 0;
    if (!reader.count_successes)
        return 1;
    return reader.has_fail_file() ? 0 : 1;
}

int Dbadb::do_import(const std::string& fname, Reader& reader, const DBImportMessageOptions& opts)
{
    list<string> fnames;
    fnames.push_back(fname);
    return do_import(fnames, reader, opts);
}

int Dbadb::do_export(const Query& query, File& file, const char* output_template, const char* forced_repmemo)
{
    ExporterOptions opts;
    if (output_template && output_template[0] != 0)
        opts.template_name = output_template;

    if (forced_repmemo)
        forced_repmemo = forced_repmemo;
    auto exporter = Exporter::create(file.encoding(), opts);

    db.export_msgs(query, [&](unique_ptr<Message>&& msg) {
        /* Override the message type if the user asks for it */
        if (forced_repmemo != NULL)
        {
            Msg& m = Msg::downcast(*msg);
            m.type = Msg::type_from_repmemo(forced_repmemo);
            m.set_rep_memo(forced_repmemo);
        }
        std::vector<std::shared_ptr<Message>> msgs;
        msgs.emplace_back(move(msg));
        file.write(exporter->to_binary(msgs));
        return true;
    });
    return 0;
}

}
}
