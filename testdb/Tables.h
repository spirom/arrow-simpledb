

#ifndef TABLES_H
#define TABLES_H

#include "arrow/api.h"

#include "ChunkedColumnCursor.h"
#include "ScanTableCursor.h"
#include "FilterProjectTableCursor.h"

class Tables {
public:
    static arrow::Status createSmallSimpleColumns(std::shared_ptr<arrow::Table>& table);
    static arrow::Status createSmallChunkedColumns(std::shared_ptr<arrow::Table>& table);
    static arrow::Status createSimple(std::shared_ptr<arrow::Table>& table);
};


#endif // TABLES_H
