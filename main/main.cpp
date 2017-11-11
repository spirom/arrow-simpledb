#include <iostream>

#include "arrow/api.h"
#include "arrow/ipc/api.h"
#include "arrow/io/api.h"

#include "columns/ChunkedColumnCursor.h"
#include "tables/ScanTableCursor.h"
#include "tables/FilterProjectTableCursor.h"



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

Status saveTable(std::shared_ptr<arrow::Table> table, const std::string& arrow_path)
{
    std::cout << "Writing to " << arrow_path << std::endl;
    std::shared_ptr<arrow::io::FileOutputStream> out_file;
    arrow::io::FileOutputStream::Open(arrow_path, &out_file);
    std::shared_ptr<arrow::ipc::RecordBatchWriter> file_writer;
    arrow::ipc::RecordBatchFileWriter::Open(out_file.get(), table->schema(), &file_writer);

    file_writer->WriteTable(*table);

    file_writer->Close();
    std::cout << "Done writing! " << std::endl;
    return Status::OK();
}

Status loadTable(const std::string& arrow_path, std::shared_ptr<arrow::Table>* table) {
    std::cout << "Reading from " << arrow_path << std::endl;
    std::shared_ptr<arrow::io::ReadableFile> in_file;
    RETURN_NOT_OK(arrow::io::ReadableFile::Open(arrow_path, &in_file));

    std::shared_ptr<arrow::ipc::RecordBatchFileReader> reader;
    RETURN_NOT_OK(arrow::ipc::RecordBatchFileReader::Open(in_file.get(), &reader));

    std::cout << "Found schema: " << reader->schema()->ToString() << std::endl;

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    for (int i = 0; i < reader->num_record_batches(); ++i) {
        std::shared_ptr<RecordBatch> batch;
        RETURN_NOT_OK(reader->ReadRecordBatch(i, &batch));
        batches.push_back(batch);
    }

    arrow::Table::FromRecordBatches(batches, table);

    std::cout << "Done reading! " << std::endl;
    return Status::OK();
}


Status showTable(std::shared_ptr<arrow::Table> table) {

    std::cout << "Chunked: " << table->IsChunked()
              << " #Columns: " << table->num_columns()
              << std::endl;

    for (int i = 0; i < table->num_columns(); i++) {
        shared_ptr<Column> col = table->column(i);
        std::cout << "Column: " << col->name()
                  << " #Chunks: " << col->data()->num_chunks()
                  << " Length: " << col->data()->length()
                  << std::endl;
    }

    ScanTableCursor tc(table);

    auto id_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::Int64Array>>(
            tc.getColumn(std::string("id")));
    auto cost_cursor = std::dynamic_pointer_cast<ColumnCursorWrapper<arrow::DoubleArray>>(
            tc.getColumn(std::string("cost")));

    std::cout << "Begin Contents" << std::endl;
    while (tc.hasMore()) {
        uint64_t pos = tc.getPosition();
        std::cout << pos << "." << std::endl;
        int64_t id = id_cursor->get();
        if (id % 4 == 0) {
            double cost = cost_cursor->get();
            std::cout << pos << " : " << id << ", " << cost << std::endl;
        }
    }
    std::cout << "End Contents" << std::endl;

    std::cout << "Begin Contents" << std::endl;
    tc.reset();
    while (tc.hasMore()) {
        int64_t id = id_cursor->get();
        if (id % 7 == 0) {
            double cost = cost_cursor->get();
            std::cout << id << ", " << cost << std::endl;
        }
    }
    std::cout << "End Contents" << std::endl;



    return Status::OK();
}

Status createTable(std::shared_ptr<arrow::Table>* table) {
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

    (*table).reset(new Table(schema, columns));

    arrow::PrettyPrintOptions options{0};
    std::string result;
    arrow::PrettyPrint(*(*table)->schema(), options, &result);
    std::cout << "Schema: " << result << std::endl;

    return Status::OK();
}

int main() {
    cout << "Hello, World!" << endl;

    std::shared_ptr<arrow::Table> table;

    if (!createTable(&table).ok()) return 1;

    if (!showTable(table).ok()) return 1;

    if (!saveTable(table, "/tmp/my_table").ok()) return 1;

    std::shared_ptr<arrow::Table> table2;

    if (!loadTable("/tmp/my_table", &table2).ok()) return 1;

    if (!showTable(table2).ok()) return 1;

    std::cout << "All done!!!" << std::endl;

    return 0;
}