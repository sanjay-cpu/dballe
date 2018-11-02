#ifndef DBALLE_DB_DB_H
#define DBALLE_DB_DB_H

#include <dballe/fwd.h>
#include <dballe/db.h>
#include <dballe/core/fwd.h>
#include <dballe/core/defs.h>
#include <dballe/core/values.h>
#include <dballe/db/defs.h>
#include <dballe/msg/fwd.h>
#include <dballe/sql/fwd.h>
#include <wreport/varinfo.h>
#include <wreport/var.h>
#include <string>
#include <memory>
#include <functional>

/** @file
 * @ingroup db
 *
 * Functions used to connect to DB-All.e and insert, query and delete data.
 */

namespace dballe {
struct DB;

namespace db {
struct Transaction;

/// Format a db::Format value to a string
std::string format_format(Format format);

/// Parse a formatted db::Format value
Format format_parse(const std::string& str);


/// Common interface for all kinds of cursors
struct Cursor
{
    virtual ~Cursor();

    /**
     * Get the number of rows still to be fetched
     *
     * @return
     *   The number of rows still to be queried.  The value is undefined if no
     *   query has been successfully peformed yet using this cursor.
     */
    virtual int remaining() const = 0;

    /**
     * Get a new item from the results of a query
     *
     * @returns
     *   true if a new record has been read, false if there is no more data to read
     */
    virtual bool next() = 0;

    /// Discard the results that have not been read yet
    virtual void discard_rest() = 0;

    /**
     * Fill in a record with the current contents of the cursor
     *
     * @param rec
     *   The record where to store the values
     */
    virtual void to_record(Record& rec) = 0;

    /**
     * Get the whole station data in a single call
     */
    virtual DBStation get_station() const = 0;

    /// Get the station identifier
    virtual int get_station_id() const = 0;

    /// Get the station latitude
    virtual Coords get_coords() const = 0;

    /// Get the station identifier, or NULL if missing
    virtual Ident get_ident() const = 0;

    /// Get the report name
    virtual std::string get_report() const = 0;

    /**
     * Iterate the cursor until the end, returning the number of items.
     *
     * If dump is a FILE pointer, also dump the cursor values to it
     */
    virtual unsigned test_iterate(FILE* dump=0);
};

/// Cursor iterating over stations
struct CursorStation : public Cursor
{
};

/// Common interface for cursors iterating over station or data values
struct CursorValue : public Cursor
{
    /// Get the database that created this cursor
    virtual std::shared_ptr<db::Transaction> get_transaction() const = 0;

    /// Get the variable code
    virtual wreport::Varcode get_varcode() const = 0;

    /// Get the variable
    virtual wreport::Var get_var() const = 0;

    /**
     * Return an integer value that can be used to refer to the current
     * variable for attribute access
     */
    virtual int attr_reference_id() const = 0;

    /**
     * Query/return the attributes for the current value of this cursor
     */
    virtual void attr_query(std::function<void(std::unique_ptr<wreport::Var>)>&& dest, bool force_read=false) = 0;
};

/// Cursor iterating over station data values
struct CursorStationData : public CursorValue
{
};

/// Cursor iterating over data values
struct CursorData : public CursorValue
{
    /// Get the level
    virtual Level get_level() const = 0;

    /// Get the time range
    virtual Trange get_trange() const = 0;

    /// Get the datetime
    virtual Datetime get_datetime() const = 0;
};

/// Cursor iterating over summary entries
struct CursorSummary : public Cursor
{
    /// Get the level
    virtual Level get_level() const = 0;

    /// Get the time range
    virtual Trange get_trange() const = 0;

    /// Get the variable code
    virtual wreport::Varcode get_varcode() const = 0;

    /// Get the datetime range
    virtual DatetimeRange get_datetimerange() const = 0;

    /// Get the count of elements
    virtual size_t get_count() const = 0;
};


class Transaction : public dballe::Transaction
{
public:
    virtual ~Transaction() {}

    /**
     * Clear state information cached during the transaction.
     *
     * This can be used when, for example, a command that deletes data is
     * issued that would invalidate ID information cached inside the
     * transaction.
     */
    virtual void clear_cached_state() = 0;

    /**
     * Start a query on the station variables archive.
     *
     * The cursor will iterate over unique lat, lon, ident triples, and will
     * contain all station vars. If a station var exists twice on two different
     * networks, only one will be present: the one of the network with the
     * highest priority.
     *
     * @param query
     *   The record with the query data (see @ref dba_record_keywords)
     * @return
     *   The cursor to use to iterate over the results
     */
    virtual std::unique_ptr<db::CursorStation> query_stations(const Query& query) = 0;

    /**
     * Query the station variables in the database.
     *
     * When multiple values per variable are present, the results will be presented
     * in increasing order of priority.
     *
     * @param query
     *   The record with the query data (see technical specifications, par. 1.6.4
     *   "parameter output/input")
     * @return
     *   The cursor to use to iterate over the results
     */
    virtual std::unique_ptr<db::CursorStationData> query_station_data(const Query& query) = 0;

    /**
     * Query the database.
     *
     * When multiple values per variable are present, the results will be presented
     * in increasing order of priority.
     *
     * @param query
     *   The record with the query data (see technical specifications, par. 1.6.4
     *   "parameter output/input")
     * @return
     *   The cursor to use to iterate over the results
     */
    virtual std::unique_ptr<db::CursorData> query_data(const Query& query) = 0;

    /**
     * Query a summary of what the result would be for a query.
     *
     * @param query
     *   The record with the query data (see technical specifications, par. 1.6.4
     *   "parameter output/input")
     * @return
     *   The cursor to use to iterate over the results. The results are the
     *   same as query_data, except that no context_id, datetime and value are
     *   provided, so it only gives all the available combinations of data
     *   contexts.
     */
    virtual std::unique_ptr<db::CursorSummary> query_summary(const Query& query) = 0;

    /**
     * Query attributes on a station value
     *
     * @param data_id
     *   The id (returned by Cursor::attr_reference_id()) used to refer to the value
     * @param dest
     *   The function that will be called on each resulting attribute
     */
    virtual void attr_query_station(int data_id, std::function<void(std::unique_ptr<wreport::Var>)>&& dest) = 0;

    /**
     * Query attributes on a data value
     *
     * @param data_id
     *   The id (returned by Cursor::attr_reference_id()) used to refer to the value
     * @param dest
     *   The function that will be called on each resulting attribute
     */
    virtual void attr_query_data(int data_id, std::function<void(std::unique_ptr<wreport::Var>)>&& dest) = 0;

    /**
     * Insert station values into the database
     *
     * The IDs of the station andl all variables that were inserted will be
     * stored in vals.
     *
     * @param vals
     *   The values to insert.
     * @param can_replace
     *   If true, then existing data can be rewritten, else data can only be added.
     * @param station_can_add
     *   If false, it will not create a missing station record, and only data
     *   for existing stations can be added. If true, then if we are inserting
     *   data for a station that does not yet exists in the database, it will
     *   be created.
     */
    virtual void insert_station_data(StationValues& vals, bool can_replace, bool station_can_add) = 0;

    /**
     * Insert data values into the database
     *
     * The IDs of the station andl all variables that were inserted will be
     * stored in vals.
     *
     * @param vals
     *   The values to insert.
     * @param can_replace
     *   If true, then existing data can be rewritten, else data can only be added.
     * @param station_can_add
     *   If false, it will not create a missing station record, and only data
     *   for existing stations can be added. If true, then if we are inserting
     *   data for a station that does not yet exists in the database, it will
     *   be created.
     */
    virtual void insert_data(DataValues& vals, bool can_replace, bool station_can_add) = 0;

    /**
     * Insert new attributes on a station value
     *
     * @param data_id
     *   The id (returned by Cursor::attr_reference_id()) used to refer to the value
     * @param attrs
     *   The attributes to be added
     */
    virtual void attr_insert_station(int data_id, const Values& attrs) = 0;

    /**
     * Insert new attributes on a data value
     *
     * @param data_id
     *   The id (returned by Cursor::attr_reference_id()) used to refer to the value
     * @param attrs
     *   The attributes to be added
     */
    virtual void attr_insert_data(int data_id, const Values& attrs) = 0;

    /**
     * Delete attributes from a station value
     *
     * @param data_id
     *   The id (returned by Cursor::attr_reference_id()) used to refer to the value
     * @param attrs
     *   Array of WMO codes of the attributes to delete.  If empty, all attributes
     *   associated to the value will be deleted.
     */
    virtual void attr_remove_station(int data_id, const db::AttrList& attrs) = 0;

    /**
     * Delete attributes from a data value
     *
     * @param data_id
     *   The id (returned by Cursor::attr_reference_id()) used to refer to the value
     * @param attrs
     *   Array of WMO codes of the attributes to delete.  If empty, all attributes
     *   associated to the value will be deleted.
     */
    virtual void attr_remove_data(int data_id, const db::AttrList& attrs) = 0;

    /**
     * Perform the query in `query', and send the results to dest.
     *
     * Return false from dest to interrupt the query.
     *
     * @param query
     *   The query to perform
     * @param dest
     *   The function that will handle the resulting messages
     * @returns true if the query reached its end, false if it got interrupted
     *   because dest returned false.
     */
    virtual bool export_msgs(const Query& query, std::function<bool(std::unique_ptr<Message>&&)> dest) = 0;

    /**
     * Update the repinfo table in the database, with the data found in the given
     * file.
     *
     * @param repinfo_file
     *   The name of the CSV file with the report type information data to load.
     *   The file is in CSV format with 6 columns: report code, mnemonic id,
     *   description, priority, descriptor, table A category.
     *   If repinfo_file is NULL, then the default of /etc/dballe/repinfo.csv is
     *   used.
     * @retval added
     *   The number of repinfo entries that have been added
     * @retval deleted
     *   The number of repinfo entries that have been deleted
     * @retval updated
     *   The number of repinfo entries that have been updated
     */
    virtual void update_repinfo(const char* repinfo_file, int* added, int* deleted, int* updated) = 0;

    /**
     * Dump the entire contents of the database to an output stream
     */
    virtual void dump(FILE* out) = 0;
};

class DB: public dballe::DB
{
public:
    static db::Format get_default_format();
    static void set_default_format(db::Format format);

    /**
     * Create from a SQLite file pathname
     *
     * @param pathname
     *   The pathname to a SQLite file
     */
    static std::shared_ptr<DB> connect_from_file(const char* pathname);

    /**
     * Create an in-memory database
     */
    static std::shared_ptr<DB> connect_memory();

    /**
     * Start a test session with DB-All.e
     */
    static std::shared_ptr<DB> connect_test();

    /**
     * Create a database from an open Connection
     */
    static std::shared_ptr<DB> create(std::unique_ptr<sql::Connection> conn);

    /**
     * Return TRUE if the string looks like a DB URL
     *
     * @param str
     *   The string to test
     * @return
     *   true if it looks like a URL, else false
     */
    static bool is_url(const char* str);

    /// Return the format of this DB
    virtual db::Format format() const = 0;

    /**
     * Remove all our traces from the database, if applicable.
     *
     * After this has been called, all other DB methods except for reset() will
     * fail.
     */
    virtual void disappear() = 0;

    /**
     * Reset the database, removing all existing Db-All.e tables and re-creating them
     * empty.
     *
     * @param repinfo_file
     *   The name of the CSV file with the report type information data to load.
     *   The file is in CSV format with 6 columns: report code, mnemonic id,
     *   description, priority, descriptor, table A category.
     *   If repinfo_file is NULL, then the default of /etc/dballe/repinfo.csv is
     *   used.
     */
    virtual void reset(const char* repinfo_file=0) = 0;

    /**
     * Same as transaction(), but the resulting transaction will throw an
     * exception if commit is called. Used for tests.
     */
    virtual std::shared_ptr<dballe::db::Transaction> test_transaction(bool readonly=false) = 0;

    /**
     * Insert station values into the database
     *
     * The IDs of the station andl all variables that were inserted will be
     * stored in vals.
     *
     * @param vals
     *   The values to insert.
     * @param can_replace
     *   If true, then existing data can be rewritten, else data can only be added.
     * @param station_can_add
     *   If false, it will not create a missing station record, and only data
     *   for existing stations can be added. If true, then if we are inserting
     *   data for a station that does not yet exists in the database, it will
     *   be created.
     */
    void insert_station_data(StationValues& vals, bool can_replace, bool station_can_add);

    /**
     * Insert data values into the database
     *
     * The IDs of the station andl all variables that were inserted will be
     * stored in vals.
     *
     * @param vals
     *   The values to insert.
     * @param can_replace
     *   If true, then existing data can be rewritten, else data can only be added.
     * @param station_can_add
     *   If false, it will not create a missing station record, and only data
     *   for existing stations can be added. If true, then if we are inserting
     *   data for a station that does not yet exists in the database, it will
     *   be created.
     */
    void insert_data(DataValues& vals, bool can_replace, bool station_can_add);

    /**
     * Perform database cleanup operations.
     *
     * Orphan values are currently:
     * \li context values for which no data exists
     * \li station values for which no context exists
     *
     * Depending on database size, this routine can take a few minutes to execute.
     */
    virtual void vacuum() = 0;

    /**
     * Start a query on the station variables archive.
     *
     * The cursor will iterate over unique lat, lon, ident triples, and will
     * contain all station vars. If a station var exists twice on two different
     * networks, only one will be present: the one of the network with the
     * highest priority.
     *
     * @param query
     *   The record with the query data (see @ref dba_record_keywords)
     * @return
     *   The cursor to use to iterate over the results
     */
    virtual std::unique_ptr<db::CursorStation> query_stations(const Query& query);

    /**
     * Query the station variables in the database.
     *
     * When multiple values per variable are present, the results will be presented
     * in increasing order of priority.
     *
     * @param query
     *   The record with the query data (see technical specifications, par. 1.6.4
     *   "parameter output/input")
     * @return
     *   The cursor to use to iterate over the results
     */
    virtual std::unique_ptr<db::CursorStationData> query_station_data(const Query& query);

    /**
     * Query the database.
     *
     * When multiple values per variable are present, the results will be presented
     * in increasing order of priority.
     *
     * @param query
     *   The record with the query data (see technical specifications, par. 1.6.4
     *   "parameter output/input")
     * @return
     *   The cursor to use to iterate over the results
     */
    virtual std::unique_ptr<db::CursorData> query_data(const Query& query);

    /**
     * Query a summary of what the result would be for a query.
     *
     * @param query
     *   The record with the query data (see technical specifications, par. 1.6.4
     *   "parameter output/input")
     * @return
     *   The cursor to use to iterate over the results. The results are the
     *   same as query_data, except that no context_id, datetime and value are
     *   provided, so it only gives all the available combinations of data
     *   contexts.
     */
    virtual std::unique_ptr<db::CursorSummary> query_summary(const Query& query);

    /**
     * Query attributes on a station value
     *
     * @param data_id
     *   The id (returned by Cursor::attr_reference_id()) used to refer to the value
     * @param dest
     *   The function that will be called on each resulting attribute
     */
    virtual void attr_query_station(int data_id, std::function<void(std::unique_ptr<wreport::Var>)>&& dest);

    /**
     * Query attributes on a data value
     *
     * @param data_id
     *   The id (returned by Cursor::attr_reference_id()) used to refer to the value
     * @param dest
     *   The function that will be called on each resulting attribute
     */
    virtual void attr_query_data(int data_id, std::function<void(std::unique_ptr<wreport::Var>)>&& dest);

    /**
     * Insert new attributes on a station value
     *
     * @param data_id
     *   The id (returned by Cursor::attr_reference_id()) used to refer to the value
     * @param attrs
     *   The attributes to be added
     */
    void attr_insert_station(int data_id, const Values& attrs);

    /**
     * Insert new attributes on a data value
     *
     * @param data_id
     *   The id (returned by Cursor::attr_reference_id()) used to refer to the value
     * @param attrs
     *   The attributes to be added
     */
    void attr_insert_data(int data_id, const Values& attrs);

    /**
     * Delete attributes from a station value
     *
     * @param data_id
     *   The id (returned by Cursor::attr_reference_id()) used to refer to the value
     * @param attrs
     *   Array of WMO codes of the attributes to delete.  If empty, all attributes
     *   associated to the value will be deleted.
     */
    void attr_remove_station(int data_id, const db::AttrList& attrs);

    /**
     * Delete attributes from a data value
     *
     * @param data_id
     *   The id (returned by Cursor::attr_reference_id()) used to refer to the value
     * @param attrs
     *   Array of WMO codes of the attributes to delete.  If empty, all attributes
     *   associated to the value will be deleted.
     */
    void attr_remove_data(int data_id, const db::AttrList& attrs);

    /**
     * Perform the query in `query', and send the results to dest.
     *
     * Return false from dest to interrupt the query.
     *
     * @param query
     *   The query to perform
     * @param dest
     *   The function that will handle the resulting messages
     * @returns true if the query reached its end, false if it got interrupted
     *   because dest returned false.
     */
    bool export_msgs(const Query& query, std::function<bool(std::unique_ptr<Message>&&)> dest);

    /**
     * Dump the entire contents of the database to an output stream
     */
    void dump(FILE* out);

    /// Print informations about the database to the given output stream
    virtual void print_info(FILE* out);

    /// Return the default repinfo file pathname
    static const char* default_repinfo_file();
};

}
}
#endif
