

#ifndef DB_TABLE_H
#define DB_TABLE_H


#include <columns/GenericColumnCursor.h>
#include <vector>
#include <arrow/api.h>

#include "core/Status.h"
#include "core/DBSchema.h"

#include "ScanTableCursor.h"
#include "DBColumnBuilder.h"

namespace db {

/**
 * Database table class, abstracting Arrow's Table class and other
 * metadata such as column encodings.
 */
    class DBTable {
    public:

        /**
         * Create a table form column names, types and encodings. All
         * argument vectors must be of the same length as the entries
         * refer to corresponding table columns.
         *
         * @param names Column names
         * @param types Column datatypes
         * @param encodings Column encodings
         * @param table out paramater -- shared pointer to resulting table is successful
         * @param pool optional Arrow memory pool
         * @return UnevenArgLists (if the name, type and encoding lists differ in length), InvalidColumn
         *         (if the type and encoding of a column don't make sense) or OK
         */
        static Status create(std::vector<std::string> names,
                      std::vector<std::shared_ptr<db::DataType>> types,
                      std::vector<db::ColumnEncoding> encodings,
                      std::shared_ptr<DBTable> *table,
                      arrow::MemoryPool *pool = arrow::default_memory_pool());

        /**
         * Start a new chunk for each column.
         */
        Status endChunk();

        /**
         * Prepare the table for use.
         */
        Status make();

        /**
         * Return a scan cursor for the table. Use this for querying,
         * possibly by composing other tbale cursors with it.
         * @return
         */
        std::shared_ptr<ScanTableCursor> getScanCursor() const;

        /**
         * Get the underlying Arrow Table.
         * @return
         */
        std::shared_ptr<arrow::Table> getTable() const;

        /**
         * Add a row of values -- specified in same order as columns in constructor.
         * @param values
         */
        Status addRow(std::vector<std::shared_ptr<db::GenValue>> values);

        /**
         * Dump table for debugging
         */
        Status dump() const;

    protected:

        /**
         * Create a table form column names, types and encodings. All
         * argument vectors must be of the same length as the entries
         * refer to corresponding table columns.
         * @param names Column names
         * @param types Column datatypes
         * @param encodings Column encodings
         */
        explicit DBTable(
                std::vector<std::string> names,
                std::vector<std::shared_ptr<db::DataType>> types,
                std::vector<db::ColumnEncoding> encodings,
                arrow::MemoryPool *pool = arrow::default_memory_pool());

        /**
         * Does the given encoding make sense for the given datatype?
         * @param dataType
         * @param encoding
         * @return InvalidColumn
         *         (if the type and encoding don't make sense) or OK
         */
        static Status checkCompatible(std::shared_ptr<db::DataType> dataType,
                                      db::ColumnEncoding encoding);

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
        std::vector<std::shared_ptr<arrow::ChunkedArray>> _columns;

        /**
         * Builders being used to construct columns
         */
        std::vector<std::shared_ptr<DBGenColumnBuilder>> _builders;
    };

};


#endif // DB_TABLE_H
