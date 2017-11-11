
#include <iostream>
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

arrow::Status
Tables::createSmallSimpleColumns(std::shared_ptr<arrow::Table>& table)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("id", arrow::int64()),
            arrow::field("cost", arrow::float64())
    };
    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    std::vector<int64_t> id_values;
    id_values.push_back(11);
    id_values.push_back(12);

    std::vector<double> cost_values;
    cost_values.push_back(21.9);
    cost_values.push_back(22.9);

    Int64Builder id_builder(pool);
    DoubleBuilder cost_builder(pool);

    ARROW_RETURN_NOT_OK(id_builder.Append(id_values));
    ARROW_RETURN_NOT_OK(cost_builder.Append(cost_values));

    std::shared_ptr<arrow::Array> id_array;
    ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
    std::shared_ptr<arrow::Array> cost_array;
    ARROW_RETURN_NOT_OK(cost_builder.Finish(&cost_array));

    shared_ptr<Field> id_field = arrow::field("id", arrow::int64());
    shared_ptr<Field> cost_field = arrow::field("cost", arrow::float64());

    shared_ptr<Column> id_col = std::make_shared<Column>(id_field, id_array);
    shared_ptr<Column> cost_col = std::make_shared<Column>(cost_field, cost_array);

    std::vector<std::shared_ptr<Column>> columns = {id_col, cost_col};

    table.reset(new Table(schema, columns));

    return Status::OK();
}

arrow::Status
Tables::createSmallChunkedColumns(std::shared_ptr<arrow::Table>& table)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("id", arrow::int64()),
            arrow::field("cost", arrow::float64())
    };
    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    // first chunks

    std::vector<int64_t> id_values_1;
    id_values_1.push_back(11);
    id_values_1.push_back(12);

    std::vector<double> cost_values_1;
    cost_values_1.push_back(21.9);
    cost_values_1.push_back(22.9);

    Int64Builder id_builder_1(pool);
    DoubleBuilder cost_builder_1(pool);

    ARROW_RETURN_NOT_OK(id_builder_1.Append(id_values_1));
    ARROW_RETURN_NOT_OK(cost_builder_1.Append(cost_values_1));

    std::shared_ptr<arrow::Array> id_array_1;
    ARROW_RETURN_NOT_OK(id_builder_1.Finish(&id_array_1));
    std::shared_ptr<arrow::Array> cost_array_1;
    ARROW_RETURN_NOT_OK(cost_builder_1.Finish(&cost_array_1));

    // second chunks

    std::vector<int64_t> id_values_2;
    id_values_2.push_back(31);
    id_values_2.push_back(32);

    std::vector<double> cost_values_2;
    cost_values_2.push_back(41.9);
    cost_values_2.push_back(42.9);

    Int64Builder id_builder_2(pool);
    DoubleBuilder cost_builder_2(pool);

    ARROW_RETURN_NOT_OK(id_builder_2.Append(id_values_2));
    ARROW_RETURN_NOT_OK(cost_builder_2.Append(cost_values_2));

    std::shared_ptr<arrow::Array> id_array_2;
    ARROW_RETURN_NOT_OK(id_builder_2.Finish(&id_array_2));
    std::shared_ptr<arrow::Array> cost_array_2;
    ARROW_RETURN_NOT_OK(cost_builder_2.Finish(&cost_array_2));

    // make columns

    ArrayVector id_arrays({id_array_1, id_array_2});
    shared_ptr<ChunkedArray> id_chunked = std::make_shared<ChunkedArray>(id_arrays);

    ArrayVector cost_arrays({cost_array_1, cost_array_2});
    shared_ptr<ChunkedArray> cost_chunked = std::make_shared<ChunkedArray>(cost_arrays);

    shared_ptr<Field> id_field = arrow::field("id", arrow::int64());
    shared_ptr<Field> cost_field = arrow::field("cost", arrow::float64());

    shared_ptr<Column> id_col = std::make_shared<Column>(id_field, id_arrays);
    shared_ptr<Column> cost_col = std::make_shared<Column>(cost_field, cost_arrays);

    std::vector<std::shared_ptr<Column>> columns = {id_col, cost_col};

    table.reset(new Table(schema, columns));

    return Status::OK();
}

arrow::Status
Tables::createSimple(std::shared_ptr<arrow::Table>& table)
{
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("id", arrow::int64()),
            arrow::field("cost", arrow::float64())
    };
    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    std::vector<int64_t> id_values;
    std::vector<double> cost_values;
    for (int64_t i = 0; i < 100; i++) {
        id_values.push_back(i);
        cost_values.push_back(0.5 * i);
    }

    Int64Builder id_builder(pool);
    DoubleBuilder cost_builder(pool);

    ARROW_RETURN_NOT_OK(id_builder.Append(id_values));
    ARROW_RETURN_NOT_OK(cost_builder.Append(cost_values));

    std::shared_ptr<arrow::Array> id_array;
    ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
    std::shared_ptr<arrow::Array> cost_array;
    ARROW_RETURN_NOT_OK(cost_builder.Finish(&cost_array));

    ArrayVector id_arrays({id_array, id_array});
    shared_ptr<ChunkedArray> id_chunked = std::make_shared<ChunkedArray>(id_arrays);

    ArrayVector cost_arrays({cost_array, cost_array});
    shared_ptr<ChunkedArray> cost_chunked = std::make_shared<ChunkedArray>(cost_arrays);

    shared_ptr<Field> id_field = arrow::field("id", arrow::int64());
    shared_ptr<Field> cost_field = arrow::field("cost", arrow::float64());

    shared_ptr<Column> id_col = std::make_shared<Column>(id_field, id_chunked);
    shared_ptr<Column> cost_col = std::make_shared<Column>(cost_field, cost_chunked);

    std::vector<std::shared_ptr<Column>> columns = {id_col, cost_col};

    table.reset(new Table(schema, columns));

    arrow::PrettyPrintOptions options{0};
    std::string result;
    //arrow::PrettyPrint(*(table->schema()), options, &result);
    //std::cout << "Schema: " << result << std::endl;

    return Status::OK();
}
