#ifndef DBALLE_MSG_JSON_CODEC_H
#define DBALLE_MSG_JSON_CODEC_H

#include <dballe/core/fwd.h>
#include <dballe/importer.h>
#include <dballe/exporter.h>
#include <dballe/message.h>

#define DBALLE_JSON_VERSION "0.1"

namespace dballe {
namespace impl {
namespace msg {

class JsonImporter : public Importer
{
public:
    JsonImporter(const dballe::ImporterOptions& opts=dballe::ImporterOptions::defaults);
    ~JsonImporter();

    Encoding encoding() const override { return Encoding::JSON; }

    bool foreach_decoded(const BinaryMessage& msg, std::function<bool(std::shared_ptr<dballe::Message>)> dest) const override;
};


class JsonExporter : public Exporter
{
public:
    JsonExporter(const dballe::ExporterOptions& opts=dballe::ExporterOptions::defaults);
    ~JsonExporter();

    std::string to_binary(const std::vector<std::shared_ptr<dballe::Message>>& msgs) const override;
};

}
}
}
#endif
