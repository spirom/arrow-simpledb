
#include <iostream>
#include <tables/DBTable.h>
#include "Tables.h"

using namespace std;

using arrow::Table;
using arrow::Column;
using arrow::Field;

db::Status
Tables::createNoRows(std::shared_ptr<db::DBTable>& table,
                     arrow::MemoryPool *pool) {

    DB_RETURN_NOT_OK(db::DBTable::create(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN},
            &table,
            pool));

    DB_RETURN_NOT_OK(table->make());

    return db::Status::OK();
}

db::Status
Tables::createSmallSimpleColumns(std::shared_ptr<db::DBTable>& table,
                                 arrow::MemoryPool *pool) {

    DB_RETURN_NOT_OK(db::DBTable::create(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN},
            &table,
            pool));

    DB_RETURN_NOT_OK(table->addRow({db::long_val(11), db::double_val(21.9)}));
    DB_RETURN_NOT_OK(table->addRow({db::long_val(12), db::double_val(22.9)}));

    DB_RETURN_NOT_OK(table->make());

    return db::Status::OK();
}

db::Status
Tables::createSmallSimpleStringColumns(std::shared_ptr<db::DBTable>& table,
                                       arrow::MemoryPool *pool)
{
    DB_RETURN_NOT_OK(db::DBTable::create(
            {"foo", "bar"},
            {db::string_type(), db::string_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN},
            &table,
            pool));

    DB_RETURN_NOT_OK(table->addRow({db::string_val("eleven"), db::string_val("twenty one")}));
    DB_RETURN_NOT_OK(table->addRow({db::string_val("twelve"), db::string_val("twenty two")}));

    DB_RETURN_NOT_OK(table->make());

    return db::Status::OK();
}

db::Status
Tables::createSmallChunkedColumns(std::shared_ptr<db::DBTable>& table,
                                  arrow::MemoryPool *pool)
{
    DB_RETURN_NOT_OK(db::DBTable::create(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN},
            &table,
            pool));

    DB_RETURN_NOT_OK(table->addRow({db::long_val(11), db::double_val(21.9)}));
    DB_RETURN_NOT_OK(table->addRow({db::long_val(12), db::double_val(22.9)}));

    DB_RETURN_NOT_OK(table->endChunk());

    DB_RETURN_NOT_OK(table->addRow({db::long_val(31), db::double_val(41.9)}));
    DB_RETURN_NOT_OK(table->addRow({db::long_val(32), db::double_val(42.9)}));

    table->make();

    return db::Status::OK();
}

db::Status
Tables::createSmallChunkedColumnsWithNulls(std::shared_ptr<db::DBTable>& table,
                                           arrow::MemoryPool *pool)
{
    DB_RETURN_NOT_OK(db::DBTable::create(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN},
            &table,
            pool));

    DB_RETURN_NOT_OK(table->addRow({db::long_val(11), db::null_val()}));
    DB_RETURN_NOT_OK(table->addRow({db::long_val(12), db::double_val(22.9)}));

    DB_RETURN_NOT_OK(table->endChunk());

    DB_RETURN_NOT_OK(table->addRow({db::null_val(), db::double_val(41.9)}));
    DB_RETURN_NOT_OK(table->addRow({db::null_val(), db::null_val()}));

    DB_RETURN_NOT_OK(table->make());

    return db::Status::OK();
}

db::Status
Tables::createSimple(std::shared_ptr<db::DBTable>& table,
                     arrow::MemoryPool *pool)
{
    DB_RETURN_NOT_OK(db::DBTable::create(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN},
            &table,
            pool));

    for (int64_t i = 0; i < 100; i++) {
        DB_RETURN_NOT_OK(table->addRow({db::long_val(i), db::double_val(0.5 * i)}));
    }

    DB_RETURN_NOT_OK(table->endChunk());

    for (int64_t i = 0; i < 100; i++) {
        DB_RETURN_NOT_OK(table->addRow({db::long_val(i), db::double_val(0.5 * i)}));
    }

    DB_RETURN_NOT_OK(table->make());

    return db::Status::OK();
}

db::Status
Tables::createSmallDictionaryColumns(std::shared_ptr<db::DBTable>& table,
                                     arrow::MemoryPool *pool)
{
    DB_RETURN_NOT_OK(db::DBTable::create(
            {"id", "cost"},
            {db::long_type(), db::long_type()},
            {db::ColumnEncoding::DICT, db::ColumnEncoding::DICT},
            &table,
            pool));

    DB_RETURN_NOT_OK(table->addRow({db::long_val(11), db::long_val(23)}));
    DB_RETURN_NOT_OK(table->addRow({db::long_val(12), db::long_val(23)}));
    DB_RETURN_NOT_OK(table->addRow({db::long_val(11), db::long_val(25)}));
    DB_RETURN_NOT_OK(table->addRow({db::long_val(12), db::long_val(25)}));

    DB_RETURN_NOT_OK(table->make());

    return db::Status::OK();
}

db::Status
Tables::createSmallStringDictionaryColumns(std::shared_ptr<db::DBTable>& table,
                                           arrow::MemoryPool *pool)
{
    DB_RETURN_NOT_OK(db::DBTable::create(
            {"foo", "bar"},
            {db::string_type(), db::string_type()},
            {db::ColumnEncoding::DICT, db::ColumnEncoding::DICT},
            &table,
            pool));

    DB_RETURN_NOT_OK(table->addRow({db::string_val("eleven"), db::string_val("twenty one")}));
    DB_RETURN_NOT_OK(table->addRow({db::string_val("twelve"), db::string_val("twenty two")}));

    DB_RETURN_NOT_OK(table->make());

    return db::Status::OK();
}

db::Status
Tables::createChunkedDictionaryColumns(std::shared_ptr<db::DBTable>& table,
                                       arrow::MemoryPool *pool)
{
    DB_RETURN_NOT_OK(db::DBTable::create(
            {"id", "cost"},
            {db::long_type(), db::string_type()},
            {db::ColumnEncoding::DICT, db::ColumnEncoding::DICT},
            &table,
            pool));

    DB_RETURN_NOT_OK(table->addRow({db::long_val(11), db::string_val("twenty one")}));
    DB_RETURN_NOT_OK(table->addRow({db::long_val(12), db::string_val("twenty two")}));

    DB_RETURN_NOT_OK(table->endChunk());

    DB_RETURN_NOT_OK(table->addRow({db::long_val(31), db::string_val("forty one")}));
    DB_RETURN_NOT_OK(table->addRow({db::long_val(32), db::string_val("forty two")}));

    DB_RETURN_NOT_OK(table->make());

    return db::Status::OK();
}

db::Status
Tables::createChunkedDictionaryColumnsWithNulls(std::shared_ptr<db::DBTable>& table,
                                                arrow::MemoryPool *pool)
{
    DB_RETURN_NOT_OK(db::DBTable::create(
            {"id", "cost"},
            {db::long_type(), db::string_type()},
            {db::ColumnEncoding::DICT, db::ColumnEncoding::DICT},
            &table,
            pool));

    DB_RETURN_NOT_OK(table->addRow({db::long_val(11), db::null_val()}));
    DB_RETURN_NOT_OK(table->addRow({db::long_val(12), db::string_val("twenty two")}));

    DB_RETURN_NOT_OK(table->endChunk());

    DB_RETURN_NOT_OK(table->addRow({db::null_val(), db::string_val("forty one")}));
    DB_RETURN_NOT_OK(table->addRow({db::null_val(), db::string_val("forty two")}));

    DB_RETURN_NOT_OK(table->make());

    return db::Status::OK();
}


