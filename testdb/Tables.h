

#ifndef TABLES_H
#define TABLES_H

#include "arrow/api.h"

#include "columns/ChunkedColumnCursor.h"
#include "tables/ScanTableCursor.h"
#include "tables/FilterProjectTableCursor.h"
class DBTable;

class Tables {
public:
    static arrow::Status createNoRows(std::shared_ptr<db::DBTable>& table,
                                      arrow::MemoryPool *pool = arrow::default_memory_pool());

    static arrow::Status createSmallSimpleColumns(std::shared_ptr<db::DBTable>& table,
                                                  arrow::MemoryPool *pool = arrow::default_memory_pool());
    static arrow::Status createSmallSimpleStringColumns(std::shared_ptr<db::DBTable>& table,
                                                        arrow::MemoryPool *pool = arrow::default_memory_pool());

    static arrow::Status createSmallChunkedColumns(std::shared_ptr<db::DBTable>& table,
                                                   arrow::MemoryPool *pool = arrow::default_memory_pool());
    static arrow::Status createSmallChunkedColumnsWithNulls(std::shared_ptr<db::DBTable>& table,
                                                            arrow::MemoryPool *pool = arrow::default_memory_pool());

    static arrow::Status createSimple(std::shared_ptr<db::DBTable>& table,
                                      arrow::MemoryPool *pool = arrow::default_memory_pool());

    static arrow::Status createSmallDictionaryColumns(std::shared_ptr<db::DBTable>& table,
                                                      arrow::MemoryPool *pool = arrow::default_memory_pool());
    static arrow::Status createSmallStringDictionaryColumns(std::shared_ptr<db::DBTable>& table,
                                                            arrow::MemoryPool *pool = arrow::default_memory_pool());
    static arrow::Status createChunkedDictionaryColumns(std::shared_ptr<db::DBTable>& table,
                                                        arrow::MemoryPool *pool = arrow::default_memory_pool());
    static arrow::Status createChunkedDictionaryColumnsWithNulls(std::shared_ptr<db::DBTable>& table,
                                                                 arrow::MemoryPool *pool = arrow::default_memory_pool());


};


#endif // TABLES_H
