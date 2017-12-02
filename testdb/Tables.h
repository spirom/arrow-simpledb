

#ifndef TABLES_H
#define TABLES_H

#include "arrow/api.h"

#include "columns/ChunkedColumnCursor.h"
#include "tables/ScanTableCursor.h"
#include "tables/FilterProjectTableCursor.h"
class DBTable;

class Tables {
public:
    static arrow::Status createSmallSimpleColumns(std::shared_ptr<DBTable>& table);
    static arrow::Status createSmallSimpleStringColumns(std::shared_ptr<DBTable>& table);

    static arrow::Status createSmallChunkedColumns(std::shared_ptr<arrow::Table>& table);
    static arrow::Status createSimple(std::shared_ptr<arrow::Table>& table);

    static arrow::Status createSmallDictionaryColumns(std::shared_ptr<arrow::Table>& table);
    static arrow::Status createSmallStringDictionaryColumns(std::shared_ptr<arrow::Table>& table);
    static arrow::Status createChunkedDictionaryColumns(std::shared_ptr<arrow::Table>& table);

    static arrow::Status createEmptyTable(std::shared_ptr<DBTable>& table);



};


#endif // TABLES_H
