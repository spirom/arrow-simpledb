

#ifndef TABLES_H
#define TABLES_H

#include "arrow/api.h"

#include "columns/ChunkedColumnCursor.h"
#include "tables/ScanTableCursor.h"
#include "tables/FilterProjectTableCursor.h"
class DBTable;

class Tables {
public:
    static db::Status createNoRows(std::shared_ptr<db::DBTable>& table,
                                      arrow::MemoryPool *pool = arrow::default_memory_pool());

    static db::Status createSmallSimpleColumns(std::shared_ptr<db::DBTable>& table,
                                                  arrow::MemoryPool *pool = arrow::default_memory_pool());
    static db::Status createSmallSimpleStringColumns(std::shared_ptr<db::DBTable>& table,
                                                        arrow::MemoryPool *pool = arrow::default_memory_pool());

    static db::Status createSmallChunkedColumns(std::shared_ptr<db::DBTable>& table,
                                                   arrow::MemoryPool *pool = arrow::default_memory_pool());
    static db::Status createSmallChunkedColumnsWithNulls(std::shared_ptr<db::DBTable>& table,
                                                            arrow::MemoryPool *pool = arrow::default_memory_pool());

    static db::Status createSimple(std::shared_ptr<db::DBTable>& table,
                                      arrow::MemoryPool *pool = arrow::default_memory_pool());

    static db::Status createSmallDictionaryColumns(std::shared_ptr<db::DBTable>& table,
                                                      arrow::MemoryPool *pool = arrow::default_memory_pool());
    static db::Status createSmallStringDictionaryColumns(std::shared_ptr<db::DBTable>& table,
                                                            arrow::MemoryPool *pool = arrow::default_memory_pool());
    static db::Status createChunkedDictionaryColumns(std::shared_ptr<db::DBTable>& table,
                                                        arrow::MemoryPool *pool = arrow::default_memory_pool());
    static db::Status createChunkedDictionaryColumnsWithNulls(std::shared_ptr<db::DBTable>& table,
                                                                 arrow::MemoryPool *pool = arrow::default_memory_pool());


};


#endif // TABLES_H
