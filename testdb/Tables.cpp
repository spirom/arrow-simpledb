
#include <iostream>
#include <tables/DBTable.h>
#include "Tables.h"

using namespace std;

using arrow::DoubleBuilder;
using arrow::Int64Builder;
using arrow::ListBuilder;
using arrow::Status;
using arrow::RecordBatch;
using arrow::Table;
using arrow::Column;
using arrow::Field;
using arrow::ChunkedArray;
using arrow::ArrayVector;
using arrow::TableBatchReader;
using arrow::Int64Type;
using arrow::DictionaryBuilder;
using arrow::StringDictionaryBuilder;

arrow::Status
Tables::createSmallSimpleColumns(std::shared_ptr<DBTable>& table) {

    DBTable *pTable = new DBTable(
                {"id", "cost"},
                {arrow::int64(), arrow::float64()},
                {GenericColumnCursor::PLAIN, GenericColumnCursor::PLAIN}
            );

    table.reset(pTable);

    table->addRow({DBTable::int64(11), DBTable::float64(21.9)});
    table->addRow({DBTable::int64(12), DBTable::float64(22.9)});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallSimpleStringColumns(std::shared_ptr<DBTable>& table)
{
    DBTable *pTable = new DBTable(
            {"foo", "bar"},
            {arrow::utf8(), arrow::utf8()},
            {GenericColumnCursor::PLAIN, GenericColumnCursor::PLAIN}
    );

    table.reset(pTable);

    table->addRow({DBTable::utf8("eleven"), DBTable::utf8("twenty one")});
    table->addRow({DBTable::utf8("twelve"), DBTable::utf8("twenty two")});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallChunkedColumns(std::shared_ptr<DBTable>& table)
{
    DBTable *pTable = new DBTable(
            {"id", "cost"},
            {arrow::int64(), arrow::float64()},
            {GenericColumnCursor::PLAIN, GenericColumnCursor::PLAIN}
    );

    table.reset(pTable);

    table->addRow({DBTable::int64(11), DBTable::float64(21.9)});
    table->addRow({DBTable::int64(12), DBTable::float64(22.9)});

    table->endChunk();

    table->addRow({DBTable::int64(31), DBTable::float64(41.9)});
    table->addRow({DBTable::int64(32), DBTable::float64(42.9)});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSimple(std::shared_ptr<DBTable>& table)
{
    DBTable *pTable = new DBTable(
            {"id", "cost"},
            {arrow::int64(), arrow::float64()},
            {GenericColumnCursor::PLAIN, GenericColumnCursor::PLAIN}
    );

    table.reset(pTable);

    for (int64_t i = 0; i < 100; i++) {
        table->addRow({DBTable::int64(i), DBTable::float64(0.5 * i)});
    }

    table->endChunk();

    for (int64_t i = 0; i < 100; i++) {
        table->addRow({DBTable::int64(i), DBTable::float64(0.5 * i)});
    }

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallDictionaryColumns(std::shared_ptr<DBTable>& table)
{
    DBTable *pTable = new DBTable(
            {"id", "cost"},
            {arrow::int64(), arrow::int64()},
            {GenericColumnCursor::DICT, GenericColumnCursor::DICT}
    );

    table.reset(pTable);

    table->addRow({DBTable::int64(11), DBTable::int64(23)});
    table->addRow({DBTable::int64(12), DBTable::int64(23)});
    table->addRow({DBTable::int64(11), DBTable::int64(25)});
    table->addRow({DBTable::int64(12), DBTable::int64(25)});

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
            {arrow::utf8(), arrow::utf8()},
            {GenericColumnCursor::DICT, GenericColumnCursor::DICT}
    );

    table.reset(pTable);

    table->addRow({DBTable::utf8("eleven"), DBTable::utf8("twenty one")});
    table->addRow({DBTable::utf8("twelve"), DBTable::utf8("twenty two")});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createChunkedDictionaryColumns(std::shared_ptr<DBTable>& table)
{
    DBTable *pTable = new DBTable(
            {"id", "cost"},
            {arrow::int64(), arrow::utf8()},
            {GenericColumnCursor::DICT, GenericColumnCursor::DICT}
    );

    table.reset(pTable);

    table->addRow({DBTable::int64(11), DBTable::utf8("twenty one")});
    table->addRow({DBTable::int64(12), DBTable::utf8("twenty two")});

    table->endChunk();

    table->addRow({DBTable::int64(31), DBTable::utf8("forty one")});
    table->addRow({DBTable::int64(32), DBTable::utf8("forty two")});

    table->make();

    return Status::OK();
}


