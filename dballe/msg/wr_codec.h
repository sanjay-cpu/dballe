#ifndef DBALLE_MSG_WR_CODEC_H
#define DBALLE_MSG_WR_CODEC_H

#include <dballe/importer.h>
#include <dballe/exporter.h>
#include <dballe/core/fwd.h>
#include <dballe/msg/msg.h>
#include <wreport/varinfo.h>
#include <stdint.h>
#include <map>
#include <string>
#include <functional>

namespace wreport {
struct Bulletin;
struct Subset;
}

namespace dballe {
namespace impl {
namespace msg {

class WRImporter : public BulletinImporter
{
public:
    WRImporter(const dballe::ImporterOptions& opts);

    /**
     * Import a decoded BUFR/CREX message
     */
    std::vector<std::shared_ptr<dballe::Message>> from_bulletin(const wreport::Bulletin& msg) const override;

    /**
     * Build Message objects a decoded bulletin, calling \a dest on each
     * resulting Message.
     *
     * Return false from \a dest to stop decoding.
     *
     * @param msg
     *   Decoded bulletin.
     * @retval dest
     *   The function that consumes the interpreted messages.
     * @returns true if it got to the end of decoding, false if dest returned false.
     */
    bool foreach_decoded_bulletin(const wreport::Bulletin& msg, std::function<bool(std::shared_ptr<dballe::Message>)> dest) const;
};

class BufrImporter : public WRImporter
{
public:
    BufrImporter(const dballe::ImporterOptions& opts=dballe::ImporterOptions::defaults);
    virtual ~BufrImporter();

    Encoding encoding() const override { return Encoding::BUFR; }

    bool foreach_decoded(const BinaryMessage& msg, std::function<bool(std::shared_ptr<dballe::Message>)> dest) const override;
};

class CrexImporter : public WRImporter
{
public:
    CrexImporter(const dballe::ImporterOptions& opts=dballe::ImporterOptions::defaults);
    virtual ~CrexImporter();

    Encoding encoding() const override { return Encoding::CREX; }

    bool foreach_decoded(const BinaryMessage& msg, std::function<bool(std::shared_ptr<dballe::Message>)> dest) const override;
};

namespace wr {
class Template;
}

class WRExporter : public BulletinExporter
{
public:
    WRExporter(const dballe::ExporterOptions& opts);

    /**
     * Import a decoded BUFR/CREX message
     */
    std::unique_ptr<wreport::Bulletin> to_bulletin(const std::vector<std::shared_ptr<dballe::Message>>& msgs) const override;

    /**
     * Infer a template name from the message contents
     */
    std::unique_ptr<wr::Template> infer_template(const Messages& msgs) const;
};

class BufrExporter : public WRExporter
{
public:
    BufrExporter(const dballe::ExporterOptions& opts=dballe::ExporterOptions::defaults);
    virtual ~BufrExporter();

    virtual std::string to_binary(const Messages& msgs) const;
    virtual std::unique_ptr<wreport::Bulletin> make_bulletin() const;
};

class CrexExporter : public WRExporter
{
public:
    CrexExporter(const dballe::ExporterOptions& opts=dballe::ExporterOptions::defaults);
    virtual ~CrexExporter();

    virtual std::string to_binary(const Messages& msgs) const;
    virtual std::unique_ptr<wreport::Bulletin> make_bulletin() const;
};

namespace wr {

struct TemplateRegistry;

class Template
{
protected:
    virtual void setupBulletin(wreport::Bulletin& bulletin);
    virtual void to_subset(const Message& msg, wreport::Subset& subset);

    void add(wreport::Varcode code, const msg::Context* ctx, const Shortcut& shortcut) const;
    void add(wreport::Varcode code, const msg::Context* ctx, wreport::Varcode srccode) const;
    void add(wreport::Varcode code, const msg::Context* ctx) const;
    void add(wreport::Varcode code, const Values& values) const;
    void add(wreport::Varcode code, const Values& values, const Shortcut& shortcut) const;
    void add(wreport::Varcode code, const Shortcut& shortcut) const;
    void add(wreport::Varcode code, wreport::Varcode srccode, const Level& level, const Trange& trange) const;
    void add(wreport::Varcode code, const wreport::Var* var) const;
    // Set station name, truncating it if it's too long
    void do_station_name(wreport::Varcode dstcode) const;

    /// Find a variable in c_station, or nullptr if not found
    const wreport::Var* find_station_var(wreport::Varcode code) const;

    void do_ecmwf_past_wtr() const;
    void do_station_height() const;
    // WMO block and station numbers
    void do_D01001() const;
    void do_D01004() const;
    // Date (year, month, day)
    void do_D01011() const;
    // Time (hour and minute), return the hour
    int do_D01012() const;
    // Time (hour, minute, second)
    void do_D01013() const;
    // Latitude and longitude, high accuracy
    void do_D01021() const;
    void do_D01022() const;
    // Latitude and longitude, coarse accuracy
    void do_D01023() const;

public:
    const dballe::ExporterOptions& opts;
    const Messages& msgs;
    const Message* msg = 0;     // Message being read
    const msg::Context* c_gnd_instant = 0;
    wreport::Subset* subset = 0; // Subset being written

    Template(const dballe::ExporterOptions& opts, const Messages& msgs)
        : opts(opts), msgs(msgs) {}
    virtual ~Template() {}

    virtual const char* name() const = 0;
    virtual const char* description() const = 0;
    virtual void to_bulletin(wreport::Bulletin& bulletin);
};

struct TemplateFactory
{
    typedef std::function<std::unique_ptr<Template>(const dballe::ExporterOptions& opts, const Messages& msgs)> factory_func;

    unsigned data_category = MISSING_INT;
    std::string name;
    std::string description;
    factory_func factory;

    TemplateFactory(unsigned data_category, std::string name, std::string description, factory_func factory)
        : data_category(data_category), name(name), description(description), factory(factory) {}
};

struct TemplateRegistry : public std::map<std::string, TemplateFactory>
{
    static const TemplateRegistry& get();
    static const TemplateFactory& get(const std::string& name);

    void register_factory(
            unsigned data_category,
            const std::string& name,
            const std::string& desc,
            TemplateFactory::factory_func fac);
};

}
}
}
}
#endif
