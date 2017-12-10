
#include <iostream>
#include <tables/DBTable.h>
#include "Tables.h"

using namespace std;

using arrow::Status;
using arrow::Table;
using arrow::Column;
using arrow::Field;

arrow::Status
Tables::createNoRows(std::shared_ptr<db::DBTable>& table) {

    db::DBTable *pTable = new db::DBTable(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN}
    );

    table.reset(pTable);

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallSimpleColumns(std::shared_ptr<db::DBTable>& table) {

    db::DBTable *pTable = new db::DBTable(
                {"id", "cost"},
                {db::long_type(), db::double_type()},
                {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN}
            );

    table.reset(pTable);

    table->addRow({db::long_val(11), db::double_val(21.9)});
    table->addRow({db::long_val(12), db::double_val(22.9)});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallSimpleStringColumns(std::shared_ptr<db::DBTable>& table)
{
    db::DBTable *pTable = new db::DBTable(
            {"foo", "bar"},
            {db::string_type(), db::string_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN}
    );

    table.reset(pTable);

    table->addRow({db::string_val("eleven"), db::string_val("twenty one")});
    table->addRow({db::string_val("twelve"), db::string_val("twenty two")});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createSmallChunkedColumns(std::shared_ptr<db::DBTable>& table)
{
    db::DBTable *pTable = new db::DBTable(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN}
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
Tables::createSimple(std::shared_ptr<db::DBTable>& table)
{
    db::DBTable *pTable = new db::DBTable(
            {"id", "cost"},
            {db::long_type(), db::double_type()},
            {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN}
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
Tables::createSmallDictionaryColumns(std::shared_ptr<db::DBTable>& table)
{
    db::DBTable *pTable = new db::DBTable(
            {"id", "cost"},
            {db::long_type(), db::long_type()},
            {db::ColumnEncoding::DICT, db::ColumnEncoding::DICT}
    );

    table.reset(pTable);

    table->addRow({db::long_val(11), db::long_val(23)});
    table->addRow({db::long_val(12), db::long_val(23)});
    table->addRow({db::long_val(11), db::long_val(25)});
    table->addRow({db::long_val(12), db::long_val(25)});

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
Tables::createSmallStringDictionaryColumns(std::shared_ptr<db::DBTable>& table)
{
    db::DBTable *pTable = new db::DBTable(
            {"foo", "bar"},
            {db::string_type(), db::string_type()},
            {db::ColumnEncoding::DICT, db::ColumnEncoding::DICT}
    );

    table.reset(pTable);

    table->addRow({db::string_val("eleven"), db::string_val("twenty one")});
    table->addRow({db::string_val("twelve"), db::string_val("twenty two")});

    table->make();

    return Status::OK();
}

arrow::Status
Tables::createChunkedDictionaryColumns(std::shared_ptr<db::DBTable>& table)
{
    db::DBTable *pTable = new db::DBTable(
            {"id", "cost"},
            {db::long_type(), db::string_type()},
            {db::ColumnEncoding::DICT, db::ColumnEncoding::DICT}
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


