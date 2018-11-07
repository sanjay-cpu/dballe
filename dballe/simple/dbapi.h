#ifndef DBALLE_SIMPLE_DBAPI_H
#define DBALLE_SIMPLE_DBAPI_H

#include "commonapi.h"
#include <dballe/file.h>

namespace dballe {
struct DB;

namespace db {
struct CursorStation;
struct Transaction;
}

namespace fortran {

struct InputFile;
struct OutputFile;

class DbAPI : public CommonAPIImplementation
{
protected:
    std::shared_ptr<db::Transaction> tr;
    CursorStation* ana_cur = nullptr;
    InputFile* input_file = nullptr;
    OutputFile* output_file = nullptr;
    int last_inserted_station_id;

    void shutdown(bool commit);

public:
    DbAPI(std::shared_ptr<db::Transaction> tr, const char* anaflag, const char* dataflag, const char* attrflag);
    DbAPI(std::shared_ptr<db::Transaction> tr, unsigned perms);
    virtual ~DbAPI();

    int enqi(const char* param) override;
    void scopa(const char* repinfofile=0) override;
    void remove_all() override;
    int quantesono() override;
    void elencamele() override;
    int voglioquesto() override;
    wreport::Varcode dammelo() override;
    void prendilo() override;
    void dimenticami() override;
    int voglioancora() override;
    void critica() override;
    void scusa() override;
    void fatto() override;
    void messages_open_input(const char* filename, const char* mode, Encoding format, bool simplified=true) override;
    void messages_open_output(const char* filename, const char* mode, Encoding format) override;
    bool messages_read_next() override;
    void messages_write_next(const char* template_name=0) override;
};

}
}

#endif
