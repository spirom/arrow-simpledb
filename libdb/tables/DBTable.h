

#ifndef DB_TABLE_H
#define DB_TABLE_H


#include <columns/GenericColumnCursor.h>
#include <vector>
#include <arrow/api.h>
#include <columns/DBSchema.h>
#include "ScanTableCursor.h"
#include "DBColumnBuilder.h"

/**
 * Database table class, abstracting Arrow's Table class and other
 * metadata such as column encodings.
 */
class DBTable {
public:
    /**
     * Create a table form column names, types and encodings. All
     * argument vectors must be of the same length as the entries
     * refer to correspondign table columns.
     * @param names Column names
     * @param types Column datatypes
     * @param encodings Column encodings
     */
    explicit DBTable(
            std::vector<std::string> names,
            std::vector<std::shared_ptr<db::DataType>> types,
            std::vector<db::ColumnEncoding> encodings);

    /**
     * Start a new chunk for each column.
     */
    void endChunk();

    /**
     * Prepare the table for use.
     */
    void make();

    /**
     * Return a scan cursor for the table. Use this for querying,
     * possibly by composing other tbale cursors with it.
     * @return
     */
    std::shared_ptr<ScanTableCursor> getScanCursor();

    /**
     * Get the underlying Arrow Table.
     * @return
     */
    std::shared_ptr<arrow::Table> getTable();

    /**
     * Add a row of values -- specified in same order as columns in constructor.
     * @param values
     */
    void addRow(std::vector<std::shared_ptr<DBGenValue>> values);

    /**
     * Create int64 value suitable for passing into addRow().
     * @param i
     * @return
     */
    static std::shared_ptr<DBGenValue> long_val(int64_t i);

    /**
     * Create float64 value suitable for passing into addRow().
     * @param d
     * @return
     */
    static std::shared_ptr<DBGenValue> double_val(double d);

    /**
     * Create string value suitable for passing into addRow().
     * @param s
     * @return
     */
    static std::shared_ptr<DBGenValue> string_val(std::string s);



private:
    /**
     * Arrow table schema
     */
    std::shared_ptr<arrow::Schema> _schema;

    /**
     * Arrow table
     */
    std::shared_ptr<arrow::Table> _table;

    /**
     * Column encodings
     */
    std::vector<db::ColumnEncoding> _encodings;

    /**
     * Arrow columns
     */
    std::vector<std::shared_ptr<arrow::Column>> _columns;

    /**
     * Builders being used to construct columns
     */
    std::vector<std::shared_ptr<DBGenColumnBuilder>> _builders;
};


#endif // DB_TABLE_H
