
#include <iostream>
#include <tables/DBTable.h>
#include "Tables.h"

using namespace std;

using arrow::Status;
using arrow::Table;
using arrow::Column;
using arrow::Field;

arrow::Status
Tables::createNoRows(std::shared_ptr<DBTable>& table) {

    DBTable *pTable = new DBTable(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {GenericColumnCursor::PLAIN, GenericColumnCursor::PLAIN}
    );

    table.reset(pTable);

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallSimpleColumns(std::shared_ptr<DBTable>& table) {

    DBTable *pTable = new DBTable(
                {"id", "cost"},
                {db::long_type(), db::double_type()},
                {GenericColumnCursor::PLAIN, GenericColumnCursor::PLAIN}
            );

    table.reset(pTable);

    table->addRow({DBTable::long_val(11), DBTable::double_val(21.9)});
    table->addRow({DBTable::long_val(12), DBTable::double_val(22.9)});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallSimpleStringColumns(std::shared_ptr<DBTable>& table)
{
    DBTable *pTable = new DBTable(
            {"foo", "bar"},
            {db::string_type(), db::string_type()},
            {GenericColumnCursor::PLAIN, GenericColumnCursor::PLAIN}
    );

    table.reset(pTable);

    table->addRow({DBTable::string_val("eleven"), DBTable::string_val("twenty one")});
    table->addRow({DBTable::string_val("twelve"), DBTable::string_val("twenty two")});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallChunkedColumns(std::shared_ptr<DBTable>& table)
{
    DBTable *pTable = new DBTable(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {GenericColumnCursor::PLAIN, GenericColumnCursor::PLAIN}
    );

    table.reset(pTable);

    table->addRow({DBTable::long_val(11), DBTable::double_val(21.9)});
    table->addRow({DBTable::long_val(12), DBTable::double_val(22.9)});

    table->endChunk();

    table->addRow({DBTable::long_val(31), DBTable::double_val(41.9)});
    table->addRow({DBTable::long_val(32), DBTable::double_val(42.9)});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSimple(std::shared_ptr<DBTable>& table)
{
    DBTable *pTable = new DBTable(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {GenericColumnCursor::PLAIN, GenericColumnCursor::PLAIN}
    );

    table.reset(pTable);

    for (int64_t i = 0; i < 100; i++) {
        table->addRow({DBTable::long_val(i), DBTable::double_val(0.5 * i)});
    }

    table->endChunk();

    for (int64_t i = 0; i < 100; i++) {
        table->addRow({DBTable::long_val(i), DBTable::double_val(0.5 * i)});
    }

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallDictionaryColumns(std::shared_ptr<DBTable>& table)
{
    DBTable *pTable = new DBTable(
            {"id", "cost"},
            {db::long_type(), db::long_type()},
            {GenericColumnCursor::DICT, GenericColumnCursor::DICT}
    );

    table.reset(pTable);

    table->addRow({DBTable::long_val(11), DBTable::long_val(23)});
    table->addRow({DBTable::long_val(12), DBTable::long_val(23)});
    table->addRow({DBTable::long_val(11), DBTable::long_val(25)});
    table->addRow({DBTable::long_val(12), DBTable::long_val(25)});

    table->make();

    /*
    std::shared_ptr<Column> c0 = table->column(0);
    std::cout << "Num chunks: " << c0->data()->num_chunks() << std::endl;
    std::cout << "Chunk 0 length: " << c0->data()->chunk(0)->length() << std::endl;
    std::shared_ptr<arrow::DictionaryArray> da = dynamic_pointer_cast<arrow::DictionaryArray>(c0->data()->chunk(0));
    std::cout << "dict array: " << da.get() << std::endl;

    std::cout << "dict array length: " << da->length() << std::endl;
    std::cout << "dict array dict length: " << da->dictionary()->length() << std::endl;
    std::cout << "dict array indices length: " << da->indices()->length() << std::endl;

    std::cout << "dict array dictionary type: " << da->dictionary()->type_id() << std::endl;

    std::cout << "dict array indices type: " << da->indices()->type_id() << std::endl;
     */


    return Status::OK();
}

arrow::Status
Tables::createSmallStringDictionaryColumns(std::shared_ptr<DBTable>& table)
{
    DBTable *pTable = new DBTable(
            {"foo", "bar"},
            {db::string_type(), db::string_type()},
            {GenericColumnCursor::DICT, GenericColumnCursor::DICT}
    );

    table.reset(pTable);

    table->addRow({DBTable::string_val("eleven"), DBTable::string_val("twenty one")});
    table->addRow({DBTable::string_val("twelve"), DBTable::string_val("twenty two")});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createChunkedDictionaryColumns(std::shared_ptr<DBTable>& table)
{
    DBTable *pTable = new DBTable(
            {"id", "cost"},
            {db::long_type(), db::string_type()},
            {GenericColumnCursor::DICT, GenericColumnCursor::DICT}
    );

    table.reset(pTable);

    table->addRow({DBTable::long_val(11), DBTable::string_val("twenty one")});
    table->addRow({DBTable::long_val(12), DBTable::string_val("twenty two")});

    table->endChunk();

    table->addRow({DBTable::long_val(31), DBTable::string_val("forty one")});
    table->addRow({DBTable::long_val(32), DBTable::string_val("forty two")});

    table->make();

    return Status::OK();
}


