
#include <filters/GreaterThanFilter.h>
#include <filters/AndFilter.h>
#include <filters/LessThanFilter.h>
#include <tables/DBTable.h>
#include "arrow/api.h"

#include <columns/DBSchema.h>
#include <filters/IsNullFilter.h>
#include <filters/NotFilter.h>

#include "TableTest.h"
#include "Tables.h"

using arrow::Table;
using arrow::Status;


TEST_F(TableTest, Simple) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSimple(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(200, table->num_rows());
    EXPECT_EQ(2, table->num_columns());
}

TEST_F(TableTest, Cursor) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSimple(dbTable).code());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();

    uint64_t count = 0;

    while (tc->hasMore()) {
        count++;
    }
    EXPECT_EQ(200u, count);
}

TEST_F(TableTest, SmallSimpleColumns) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallSimpleColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(2, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();
    auto id_cursor = tc->getLongColumn(std::string("id"));
    auto cost_cursor = tc->getDoubleColumn(std::string("cost"));
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_EQ(21.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ(22.9, cost_cursor->get());
    EXPECT_FALSE(tc->hasMore());
}


TEST_F(TableTest, SmallSimpleStringColumns) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallSimpleStringColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(2, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();
    auto foo_cursor = tc->getStringColumn(std::string("foo"));
    auto bar_cursor = tc->getStringColumn(std::string("bar"));
    EXPECT_TRUE(tc->hasMore());
    EXPECT_STREQ("eleven", foo_cursor->get().c_str());
    EXPECT_STREQ("twenty one", bar_cursor->get().c_str());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_STREQ("twelve", foo_cursor->get().c_str());
    EXPECT_STREQ("twenty two", bar_cursor->get().c_str());
    EXPECT_FALSE(tc->hasMore());
}

TEST_F(TableTest, SmallChunkedColumns) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();
    auto id_cursor = tc->getLongColumn(std::string("id"));
    auto cost_cursor = tc->getDoubleColumn(std::string("cost"));
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_EQ(21.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ(22.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(31, id_cursor->get());
    EXPECT_EQ(41.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(32, id_cursor->get());
    EXPECT_EQ(42.9, cost_cursor->get());
    EXPECT_FALSE(tc->hasMore());
}

TEST_F(TableTest, ComplexColumns) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSimple(dbTable).code());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();
    auto id_cursor = tc->getLongColumn(std::string("id"));
    auto cost_cursor = tc->getDoubleColumn(std::string("cost"));

    uint64_t count = 0;

    while (tc->hasMore()) {
        int64_t val = count % 100;
        EXPECT_EQ(val, id_cursor->get());
        EXPECT_EQ(0.5 * val, cost_cursor->get());
        count++;
    }
    EXPECT_EQ(200u, count);
}

TEST_F(TableTest, SimpleFilter) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();

    std::shared_ptr<db::Filter> filter = std::make_shared<db::GreaterThanFilter<db::LongType>>("id", 31);
    db::FilterProjectTableCursor fptc(*tc, filter);

    auto id_cursor = fptc.getLongColumn(std::string("id"));
    auto cost_cursor = fptc.getDoubleColumn(std::string("cost"));
    EXPECT_TRUE(fptc.hasMore());

    EXPECT_EQ(32, id_cursor->get());
    EXPECT_EQ(42.9, cost_cursor->get());
    EXPECT_FALSE(fptc.hasMore());
}

TEST_F(TableTest, ConjunctiveFilter) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();

    std::shared_ptr<db::Filter> leftFilter = std::make_shared<db::GreaterThanFilter<db::LongType>>("id", 11);
    std::shared_ptr<db::Filter> rightFilter = std::make_shared<db::LessThanFilter<db::DoubleType>>("cost", 42);
    std::shared_ptr<db::Filter> andFilter =
            std::make_shared<db::AndFilter>(leftFilter, rightFilter);

    db::FilterProjectTableCursor fptc(*tc, andFilter);

    auto id_cursor = fptc.getLongColumn(std::string("id"));
    auto cost_cursor = fptc.getDoubleColumn(std::string("cost"));
    EXPECT_TRUE(fptc.hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ(22.9, cost_cursor->get());
    EXPECT_TRUE(fptc.hasMore());
    EXPECT_EQ(31, id_cursor->get());
    EXPECT_EQ(41.9, cost_cursor->get());
    EXPECT_FALSE(fptc.hasMore());
}

TEST_F(TableTest, NeverTrueFilter) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();

    std::shared_ptr<db::Filter> leftFilter = std::make_shared<db::GreaterThanFilter<db::LongType>>("id", 31);
    std::shared_ptr<db::Filter> rightFilter = std::make_shared<db::LessThanFilter<db::DoubleType>>("cost", 22);
    std::shared_ptr<db::Filter> andFilter =
            std::make_shared<db::AndFilter>(leftFilter, rightFilter);

    db::FilterProjectTableCursor fptc(*tc, andFilter);

    auto id_cursor = fptc.getLongColumn(std::string("id"));
    auto cost_cursor = fptc.getDoubleColumn(std::string("cost"));
    EXPECT_FALSE(fptc.hasMore());
}

TEST_F(TableTest, TwoTypesSameFilter) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();

    std::shared_ptr<db::Filter> leftFilter = std::make_shared<db::GreaterThanFilter<db::LongType>>("id", 31);
    std::shared_ptr<db::Filter> rightFilter = std::make_shared<db::GreaterThanFilter<db::DoubleType>>("cost", 100);
    std::shared_ptr<db::Filter> andFilter =
            std::make_shared<db::AndFilter>(leftFilter, rightFilter);

    db::FilterProjectTableCursor fptc(*tc, andFilter);

    auto id_cursor = fptc.getLongColumn(std::string("id"));
    auto cost_cursor = fptc.getDoubleColumn(std::string("cost"));
    EXPECT_FALSE(fptc.hasMore());
}

TEST_F(TableTest, FilterComposition) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();

    std::shared_ptr<db::Filter> first_filter = std::make_shared<db::GreaterThanFilter<db::LongType>>("id", 11);
    db::FilterProjectTableCursor first_cursor(*tc, first_filter);

    std::shared_ptr<db::Filter> second_filter = std::make_shared<db::LessThanFilter<db::DoubleType>>("cost", 42);
    db::FilterProjectTableCursor second_cursor(first_cursor, second_filter);

    auto id_cursor = second_cursor.getLongColumn(std::string("id"));
    auto cost_cursor = second_cursor.getDoubleColumn(std::string("cost"));
    EXPECT_TRUE(second_cursor.hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ(22.9, cost_cursor->get());
    EXPECT_TRUE(second_cursor.hasMore());
    EXPECT_EQ(31, id_cursor->get());
    EXPECT_EQ(41.9, cost_cursor->get());
    EXPECT_FALSE(second_cursor.hasMore());
}


TEST_F(TableTest, SmallDictionaryColumns) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallDictionaryColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    EXPECT_EQ(arrow::Type::INT64, table->column(0)->type()->id());
    EXPECT_EQ(arrow::Type::INT64, table->column(1)->type()->id());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();
    auto id_cursor = tc->getLongColumn(std::string("id"));
    auto cost_cursor = tc->getLongColumn(std::string("cost"));
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_EQ(23, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ(23, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_EQ(25, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ(25, cost_cursor->get());
    EXPECT_FALSE(tc->hasMore());
}

TEST_F(TableTest, SmallStringDictionaryColumns) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallStringDictionaryColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(2, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    EXPECT_EQ(arrow::Type::STRING, table->column(0)->type()->id());
    EXPECT_EQ(arrow::Type::STRING, table->column(1)->type()->id());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();
    auto foo_cursor = tc->getStringColumn(std::string("foo"));
    auto bar_cursor = tc->getStringColumn(std::string("bar"));
    EXPECT_TRUE(tc->hasMore());
    EXPECT_STREQ("eleven", foo_cursor->get().c_str());
    EXPECT_STREQ("twenty one", bar_cursor->get().c_str());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_STREQ("twelve", foo_cursor->get().c_str());
    EXPECT_STREQ("twenty two", bar_cursor->get().c_str());
    EXPECT_FALSE(tc->hasMore());

}

TEST_F(TableTest, ChunkedDictionaryColumns) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createChunkedDictionaryColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();

    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());
    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();
    auto id_cursor = tc->getLongColumn(std::string("id"));
    auto cost_cursor = tc->getStringColumn(std::string("cost"));
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_EQ("twenty one", cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ("twenty two", cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(31, id_cursor->get());
    EXPECT_EQ("forty one", cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(32, id_cursor->get());
    EXPECT_EQ("forty two", cost_cursor->get());
    EXPECT_FALSE(tc->hasMore());
}

TEST_F(TableTest, ResetCursor) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();
    auto id_cursor = tc->getLongColumn(std::string("id"));
    auto cost_cursor = tc->getDoubleColumn(std::string("cost"));
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_EQ(21.9, cost_cursor->get());

    tc->reset();

    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_EQ(21.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ(22.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(31, id_cursor->get());
    EXPECT_EQ(41.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(32, id_cursor->get());
    EXPECT_EQ(42.9, cost_cursor->get());
    EXPECT_FALSE(tc->hasMore());

    tc->reset();

    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_EQ(21.9, cost_cursor->get());

}

TEST_F(TableTest, AddAfterMake) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumns(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    dbTable->addRow({db::long_val(51), db::double_val(61.9)});
    dbTable->addRow({db::long_val(52), db::double_val(62.9)});

    dbTable->make();

    table = dbTable->getTable();
    EXPECT_EQ(6, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();
    auto id_cursor = tc->getLongColumn(std::string("id"));
    auto cost_cursor = tc->getDoubleColumn(std::string("cost"));
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_EQ(21.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_EQ(22.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(31, id_cursor->get());
    EXPECT_EQ(41.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(32, id_cursor->get());
    EXPECT_EQ(42.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(51, id_cursor->get());
    EXPECT_EQ(61.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_EQ(52, id_cursor->get());
    EXPECT_EQ(62.9, cost_cursor->get());
    EXPECT_FALSE(tc->hasMore());
}

TEST_F(TableTest, NoRows) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createNoRows(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(0, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();
    auto id_cursor = tc->getLongColumn(std::string("id"));
    auto cost_cursor = tc->getDoubleColumn(std::string("cost"));
    EXPECT_FALSE(tc->hasMore());
}

TEST_F(TableTest, Nulls) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createSmallChunkedColumnsWithNulls(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();
    auto id_cursor = tc->getLongColumn(std::string("id"));
    auto cost_cursor = tc->getDoubleColumn(std::string("cost"));
    EXPECT_TRUE(tc->hasMore());
    EXPECT_FALSE(id_cursor->isNull());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_TRUE(cost_cursor->isNull());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_FALSE(id_cursor->isNull());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_FALSE(cost_cursor->isNull());
    EXPECT_EQ(22.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_TRUE(id_cursor->isNull());
    EXPECT_FALSE(cost_cursor->isNull());
    EXPECT_EQ(41.9, cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_TRUE(id_cursor->isNull());
    EXPECT_TRUE(cost_cursor->isNull());
    EXPECT_FALSE(tc->hasMore());
}

TEST_F(TableTest, StringDictNulls) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createChunkedDictionaryColumnsWithNulls(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();
    auto id_cursor = tc->getLongColumn(std::string("id"));
    auto cost_cursor = tc->getStringColumn(std::string("cost"));
    EXPECT_TRUE(tc->hasMore());
    EXPECT_FALSE(id_cursor->isNull());
    EXPECT_EQ(11, id_cursor->get());
    EXPECT_TRUE(cost_cursor->isNull());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_FALSE(id_cursor->isNull());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_FALSE(cost_cursor->isNull());
    EXPECT_EQ("twenty two", cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_TRUE(id_cursor->isNull());
    EXPECT_FALSE(cost_cursor->isNull());
    EXPECT_EQ("forty one", cost_cursor->get());
    EXPECT_TRUE(tc->hasMore());
    EXPECT_TRUE(id_cursor->isNull());
    EXPECT_FALSE(cost_cursor->isNull());
    EXPECT_EQ("forty two", cost_cursor->get());
    EXPECT_FALSE(tc->hasMore());
}

TEST_F(TableTest, FilterNulls) {
    std::shared_ptr<db::DBTable> dbTable;
    EXPECT_EQ(Status::OK().code(), Tables::createChunkedDictionaryColumnsWithNulls(dbTable).code());

    std::shared_ptr<Table> table = dbTable->getTable();
    EXPECT_EQ(4, table->num_rows());
    EXPECT_EQ(2, table->num_columns());

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();

    auto leftNullFilter = std::make_shared<db::IsNullFilter>("id");
    auto rightNullFilter = std::make_shared<db::IsNullFilter>("cost");

    auto leftNotFilter = std::make_shared<db::NotFilter>(leftNullFilter);
    auto rightNotFilter = std::make_shared<db::NotFilter>(rightNullFilter);

    std::shared_ptr<db::Filter> andFilter =
            std::make_shared<db::AndFilter>(leftNotFilter, rightNotFilter);

    db::FilterProjectTableCursor fptc(*tc, andFilter);

    auto id_cursor = fptc.getLongColumn(std::string("id"));
    auto cost_cursor = fptc.getStringColumn(std::string("cost"));
    EXPECT_TRUE(fptc.hasMore());
    EXPECT_FALSE(id_cursor->isNull());
    EXPECT_EQ(12, id_cursor->get());
    EXPECT_FALSE(cost_cursor->isNull());
    EXPECT_EQ("twenty two", cost_cursor->get());
    EXPECT_FALSE(fptc.hasMore());
}


// TODO: filter on dictionary column (efficiently?)

// TODO: test mismatched column lengths

// TODO: test mismatched chunk lengths
