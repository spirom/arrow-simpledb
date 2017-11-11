
#include <filters/GreaterThanFilter.h>
#include <filters/AndFilter.h>
#include <filters/LessThanFilter.h>
#include "arrow/api.h"

#include "TableTest.h"
#include "Tables.h"

using arrow::Table;
using arrow::Status;

TEST_F(TableTest, Simple) {
    std::shared_ptr<Table> table;
    EXPECT_EQ(Status::OK().code(), Tables::createSimple(table).code());
    EXPECT_EQ(200, table->num_rows());
    EXPECT_EQ(2, table->num_columns());
}

TEST_F(TableTest, Cursor) {
    std::shared_ptr<Table> table;
    EXPECT_EQ(Status::OK().code(), Tables::createSimple(table).code());
    ScanTableCursor tc(table);
    uint64_t count = 0;
    while (tc.hasMore()) {
        EXPECT_EQ(count, tc.getPosition());
        count++;
    }
    EXPECT_EQ(200u, count);
}

TEST_F(TableTest, SmallSimpleColumns) {
    std::shared_ptr<Table> table;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallSimpleColumns(table).code());
    EXPECT_EQ(2, table->num_rows());
    EXPECT_EQ(2, table->num_columns());
    ScanTableCursor tc(table);
    auto id_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::Int64Array>>(
            tc.getColumn(std::string("id")));
    auto cost_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::DoubleArray>>(
            tc.getColumn(std::string("cost")));
    EXPECT_TRUE(tc.hasMore());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_EQ(21.9, cost_cursor->get());
    EXPECT_TRUE(tc.hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ(22.9, cost_cursor->get());
    EXPECT_FALSE(tc.hasMore());
}

TEST_F(TableTest, SmallChunkedColumns) {
    std::shared_ptr<Table> table;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(table).code());
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());
    ScanTableCursor tc(table);
    auto id_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::Int64Array>>(
            tc.getColumn(std::string("id")));
    auto cost_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::DoubleArray>>(
            tc.getColumn(std::string("cost")));
    EXPECT_TRUE(tc.hasMore());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_EQ(21.9, cost_cursor->get());
    EXPECT_TRUE(tc.hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ(22.9, cost_cursor->get());
    EXPECT_TRUE(tc.hasMore());
    EXPECT_EQ(31, id_cursor->get());
    EXPECT_EQ(41.9, cost_cursor->get());
    EXPECT_TRUE(tc.hasMore());
    EXPECT_EQ(32, id_cursor->get());
    EXPECT_EQ(42.9, cost_cursor->get());
    EXPECT_FALSE(tc.hasMore());
}

TEST_F(TableTest, ComplexColumns) {
    std::shared_ptr<Table> table;
    EXPECT_EQ(Status::OK().code(), Tables::createSimple(table).code());
    ScanTableCursor tc(table);
    uint64_t count = 0;
    auto id_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::Int64Array>>(
            tc.getColumn(std::string("id")));
    auto cost_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::DoubleArray>>(
            tc.getColumn(std::string("cost")));
    while (tc.hasMore()) {
        EXPECT_EQ(count, tc.getPosition());
        int64_t val = count % 100;
        EXPECT_EQ(val, id_cursor->get());
        EXPECT_EQ(0.5 * val, cost_cursor->get());
        count++;
    }
    EXPECT_EQ(200u, count);
}

TEST_F(TableTest, SimpleFilter) {
    std::shared_ptr<Table> table;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(table).code());
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());
    ScanTableCursor tc(table);

    std::shared_ptr<Filter> filter = std::make_shared<GreaterThanFilter<arrow::Int64Array>>("id", 31);
    FilterProjectTableCursor fptc(tc, filter);

    auto id_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::Int64Array>>(
            fptc.getColumn(std::string("id")));
    auto cost_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::DoubleArray>>(
            fptc.getColumn(std::string("cost")));
    EXPECT_TRUE(fptc.hasMore());

    EXPECT_EQ(32, id_cursor->get());
    EXPECT_EQ(42.9, cost_cursor->get());
    EXPECT_FALSE(fptc.hasMore());
}

TEST_F(TableTest, ConjunctiveFilter) {
    std::shared_ptr<Table> table;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(table).code());
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());
    ScanTableCursor tc(table);

    std::shared_ptr<Filter> leftFilter = std::make_shared<GreaterThanFilter<arrow::Int64Array>>("id", 11);
    std::shared_ptr<Filter> rightFilter = std::make_shared<LessThanFilter<arrow::DoubleArray>>("cost", 42);
    std::shared_ptr<Filter> andFilter =
            std::make_shared<AndFilter>("id", leftFilter, rightFilter);

    FilterProjectTableCursor fptc(tc, andFilter);

    auto id_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::Int64Array>>(
            fptc.getColumn(std::string("id")));
    auto cost_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::DoubleArray>>(
            fptc.getColumn(std::string("cost")));
    EXPECT_TRUE(fptc.hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ(22.9, cost_cursor->get());
    EXPECT_TRUE(fptc.hasMore());
    EXPECT_EQ(31, id_cursor->get());
    EXPECT_EQ(41.9, cost_cursor->get());
    EXPECT_FALSE(fptc.hasMore());
}

TEST_F(TableTest, NeverTrueFilter) {
    std::shared_ptr<Table> table;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(table).code());
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());
    ScanTableCursor tc(table);

    std::shared_ptr<Filter> leftFilter = std::make_shared<GreaterThanFilter<arrow::Int64Array>>("id", 31);
    std::shared_ptr<Filter> rightFilter = std::make_shared<LessThanFilter<arrow::DoubleArray>>("cost", 22);
    std::shared_ptr<Filter> andFilter =
            std::make_shared<AndFilter>("id", leftFilter, rightFilter);

    FilterProjectTableCursor fptc(tc, andFilter);

    auto id_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::Int64Array>>(
            fptc.getColumn(std::string("id")));
    auto cost_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::DoubleArray>>(
            fptc.getColumn(std::string("cost")));
    EXPECT_FALSE(fptc.hasMore());
}

TEST_F(TableTest, TwoTypesSameFilter) {
    std::shared_ptr<Table> table;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(table).code());
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());
    ScanTableCursor tc(table);

    std::shared_ptr<Filter> leftFilter = std::make_shared<GreaterThanFilter<arrow::Int64Array>>("id", 31);
    std::shared_ptr<Filter> rightFilter = std::make_shared<GreaterThanFilter<arrow::DoubleArray>>("cost", 100);
    std::shared_ptr<Filter> andFilter =
            std::make_shared<AndFilter>("id", leftFilter, rightFilter);

    FilterProjectTableCursor fptc(tc, andFilter);

    auto id_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::Int64Array>>(
            fptc.getColumn(std::string("id")));
    auto cost_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::DoubleArray>>(
            fptc.getColumn(std::string("cost")));
    EXPECT_FALSE(fptc.hasMore());
}

TEST_F(TableTest, FilterComposition) {
    std::shared_ptr<Table> table;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(table).code());
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());
    ScanTableCursor tc(table);

    std::shared_ptr<Filter> first_filter = std::make_shared<GreaterThanFilter<arrow::Int64Array>>("id", 11);
    FilterProjectTableCursor first_cursor(tc, first_filter);

    std::shared_ptr<Filter> second_filter = std::make_shared<LessThanFilter<arrow::DoubleArray>>("cost", 42);
    FilterProjectTableCursor second_cursor(first_cursor, second_filter);

    auto id_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::Int64Array>>(
            second_cursor.getColumn(std::string("id")));
    auto cost_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::DoubleArray>>(
            second_cursor.getColumn(std::string("cost")));
    EXPECT_TRUE(second_cursor.hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ(22.9, cost_cursor->get());
    EXPECT_TRUE(second_cursor.hasMore());
    EXPECT_EQ(31, id_cursor->get());
    EXPECT_EQ(41.9, cost_cursor->get());
    EXPECT_FALSE(second_cursor.hasMore());
}