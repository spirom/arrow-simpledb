
#include <iostream>
#include <tables/DBTable.h>
#include "Tables.h"

using namespace std;

using arrow::Status;
using arrow::Table;
using arrow::Column;
using arrow::Field;

arrow::Status
Tables::createNoRows(std::shared_ptr<db::DBTable>& table,
                     arrow::MemoryPool *pool) {

    db::DBTable *pTable = new db::DBTable(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN},
            pool
    );

    table.reset(pTable);

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallSimpleColumns(std::shared_ptr<db::DBTable>& table,
                                 arrow::MemoryPool *pool) {

    db::DBTable *pTable = new db::DBTable(
                {"id", "cost"},
                {db::long_type(), db::double_type()},
                {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN},
                pool
            );

    table.reset(pTable);

    table->addRow({db::long_val(11), db::double_val(21.9)});
    table->addRow({db::long_val(12), db::double_val(22.9)});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallSimpleStringColumns(std::shared_ptr<db::DBTable>& table,
                                       arrow::MemoryPool *pool)
{
    db::DBTable *pTable = new db::DBTable(
            {"foo", "bar"},
            {db::string_type(), db::string_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN},
            pool
    );

    table.reset(pTable);

    table->addRow({db::string_val("eleven"), db::string_val("twenty one")});
    table->addRow({db::string_val("twelve"), db::string_val("twenty two")});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallChunkedColumns(std::shared_ptr<db::DBTable>& table,
                                  arrow::MemoryPool *pool)
{
    db::DBTable *pTable = new db::DBTable(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN},
            pool
    );

    table.reset(pTable);

    table->addRow({db::long_val(11), db::double_val(21.9)});
    table->addRow({db::long_val(12), db::double_val(22.9)});

    table->endChunk();

    table->addRow({db::long_val(31), db::double_val(41.9)});
    table->addRow({db::long_val(32), db::double_val(42.9)});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallChunkedColumnsWithNulls(std::shared_ptr<db::DBTable>& table,
                                           arrow::MemoryPool *pool)
{
    db::DBTable *pTable = new db::DBTable(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN},
            pool
    );

    table.reset(pTable);

    table->addRow({db::long_val(11), db::null_val()});
    table->addRow({db::long_val(12), db::double_val(22.9)});

    table->endChunk();

    table->addRow({db::null_val(), db::double_val(41.9)});
    table->addRow({db::null_val(), db::null_val()});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSimple(std::shared_ptr<db::DBTable>& table,
                     arrow::MemoryPool *pool)
{
    db::DBTable *pTable = new db::DBTable(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN},
            pool
    );

    table.reset(pTable);

    for (int64_t i = 0; i < 100; i++) {
        table->addRow({db::long_val(i), db::double_val(0.5 * i)});
    }

    table->endChunk();

    for (int64_t i = 0; i < 100; i++) {
        table->addRow({db::long_val(i), db::double_val(0.5 * i)});
    }

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallDictionaryColumns(std::shared_ptr<db::DBTable>& table,
                                     arrow::MemoryPool *pool)
{
    db::DBTable *pTable = new db::DBTable(
            {"id", "cost"},
            {db::long_type(), db::long_type()},
            {db::ColumnEncoding::DICT, db::ColumnEncoding::DICT},
            pool
    );

    table.reset(pTable);

    table->addRow({db::long_val(11), db::long_val(23)});
    table->addRow({db::long_val(12), db::long_val(23)});
    table->addRow({db::long_val(11), db::long_val(25)});
    table->addRow({db::long_val(12), db::long_val(25)});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallStringDictionaryColumns(std::shared_ptr<db::DBTable>& table,
                                           arrow::MemoryPool *pool)
{
    db::DBTable *pTable = new db::DBTable(
            {"foo", "bar"},
            {db::string_type(), db::string_type()},
            {db::ColumnEncoding::DICT, db::ColumnEncoding::DICT},
            pool
    );

    table.reset(pTable);

    table->addRow({db::string_val("eleven"), db::string_val("twenty one")});
    table->addRow({db::string_val("twelve"), db::string_val("twenty two")});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createChunkedDictionaryColumns(std::shared_ptr<db::DBTable>& table,
                                       arrow::MemoryPool *pool)
{
    db::DBTable *pTable = new db::DBTable(
            {"id", "cost"},
            {db::long_type(), db::string_type()},
            {db::ColumnEncoding::DICT, db::ColumnEncoding::DICT},
            pool
    );

    table.reset(pTable);

    table->addRow({db::long_val(11), db::string_val("twenty one")});
    table->addRow({db::long_val(12), db::string_val("twenty two")});

    table->endChunk();

    table->addRow({db::long_val(31), db::string_val("forty one")});
    table->addRow({db::long_val(32), db::string_val("forty two")});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createChunkedDictionaryColumnsWithNulls(std::shared_ptr<db::DBTable>& table,
                                                arrow::MemoryPool *pool)
{
    db::DBTable *pTable = new db::DBTable(
            {"id", "cost"},
            {db::long_type(), db::string_type()},
            {db::ColumnEncoding::DICT, db::ColumnEncoding::DICT},
            pool
    );

    table.reset(pTable);

    table->addRow({db::long_val(11), db::null_val()});
    table->addRow({db::long_val(12), db::string_val("twenty two")});

    table->endChunk();

    table->addRow({db::null_val(), db::string_val("forty one")});
    table->addRow({db::null_val(), db::string_val("forty two")});

    table->make();

    return Status::OK();
}


