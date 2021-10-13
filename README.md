
# A Simple In-Memory Database Using Apache Arrow

[Apache Arrow](https://arrow.apache.org/) provides a uniform in-memory columnar representation for data, and
a convenient set of C++ APIs (among others) for working with that representation.

This project explores how an extremely simple database query engine might be implemented on top of Arrow,
and thus how the complexities of Arrow's table and column representation (such as chunked columns
and dictionary encodings) may be
abstracted away so that most of the query engine is oblivious to them. Of course, an important goal is to
preserve Arrow's performance advantages.

**_Note:_** this is not, and will not become, a functioning DBMS that you can use for your applications. It's an
exploration, and illustration, of how the core of such a DBMS could be built on top of Apache Arrow,
and a learning tool for developers of database internals.

A fairly typical implementation model is used for operators: composable iterators
(here called table cursors) are used to process queries against tables.

The core mechanism for abstracting away Arrow's data structures can be seen in the files
[libdb/columns/ChunkedColumnCursor.h](libdb/columns/ChunkedColumnCursor.h)
and
[libdb/columns/ChunkedColumnCursor.cpp](libdb/columns/ChunkedColumnCursor.cpp),
where the multiple chunks that comprise a column are hidden behind a uniform interface, including a `seek()` method.

Another abstraction mechanism for easily populating tables with data can be seen in
[libdb/tables/DBTable.h](libdb/tables/DBTable.h)
and [libdb/tables/DBTable.cpp](libdb/tables/DBTable.cpp)

# Install

Install C++ and GLib (C) Packages on official [Install Apache Arrow](https://arrow.apache.org/install/)

## Ubunbtu

```bash
 sudo apt update
 sudo apt install -y -V ca-certificates lsb-release wget
 wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
 sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
 sudo apt update
 sudo apt install -y -V libarrow-dev # For C++
 sudo apt install -y -V libarrow-glib-dev # For GLib (C)
 sudo apt install -y -V libarrow-dataset-dev # For Apache Arrow Dataset C++
 sudo apt install -y -V libarrow-flight-dev # For Apache Arrow Flight C++
 # Notes for Plasma related packages:
 #   * You need to enable "non-free" component on Debian GNU/Linux
 #   * You need to enable "multiverse" component on Ubuntu
 #   * You can use Plasma related packages only on amd64
 sudo apt install -y -V libplasma-dev # For Plasma C++
 sudo apt install -y -V libplasma-glib-dev # For Plasma GLib (C)
 sudo apt install -y -V libgandiva-dev # For Gandiva C++
 sudo apt install -y -V libgandiva-glib-dev # For Gandiva GLib (C)
 sudo apt install -y -V libparquet-dev # For Apache Parquet C++
 sudo apt install -y -V libparquet-glib-dev # For Apache Parquet GLib (C)
```

Install Google Test

```bash
 sudo apt install -y libgtest-dev
```

## Mac

Install using Homebrew.

```
 brew install apache-arrow apache-arrow-glib googletest
```

# C++ Headers

    // The following also pulls int he crucial "columns/DBSchema.h"
    #include "tables/DBTable.h"

# Creating and Populating Tables

The `DBTable` class encapsulates an Arrow table together with additional metadata, such as column encodings.
The following example creates two columns: `id` of type `long` and `cost` of type `double`.

    std::shared_ptr<db::DBTable> dbTable;

    db::Status status = db::DBTable::create(
                         {"id", "cost"},
                         {db::long_type(), db::double_type()},
                         {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN}
                         &table);

    table->addRow({db::long_val(11), db::double_val(21.9)});
    table->addRow({db::long_val(12), db::double_val(22.9)});

    table->make();

The call to `make()` prepares the table for use.
Specify `db::ColumnEncoding::DICT` to make a the corresponding column dictionary encoded (not supported for double.)

Optional calls to `endChunk()` causes the underlying columns to be broken into multiple chunks. Each such call closes
the current chunk for each column and begins a new one. In the following example, each column has two chunks of
two values each.

    std::shared_ptr<db::DBTable> dbTable;

    db::Status status = db::DBTable::create(
                         {"id", "cost"},
                         {db::long_type(), db::double_type()},
                         {db::ColumnEncoding::PLAIN, db::ColumnEncoding::PLAIN}
                         &table);

    table->addRow({db::long_val(11), db::double_val(21.9)});
    table->addRow({db::long_val(12), db::double_val(22.9)});

    table->endChunk();

    table->addRow({db::long_val(31), db::double_val(41.9)});
    table->addRow({db::long_val(32), db::double_val(42.9)});

    table->make();


# Querying Tables

The unit tests show how simple queries can be executed against tables created through Arrow APIs.
Queries are constructed by composing implementations of the `TableCursor` virtual class: currently just
`ScanTableCursor` and `FilterProjectTableCursor`. To get access to column data, call `getColumn()` on your
outermost `TableCursor` to obtain a `GenericColumnCursor`.

For example, a scan cursor can be used to simply scan a table:

    std::shared_ptr<db::TableCursor> tc = table->getScanCursor();

    // get pointers to two columns named "id" and "cost"
    auto id_cursor = tc->getLongColumn(std::string("id"));
    auto cost_cursor = tc->getDoubleColumn(std::string("cost"));

    // iterate through the table and print it
    while (tc->hasMore()) { // advances cursor -- must be called before first element
        std::print << "id = " << id_cursor->get() << ", cost = " << cost_cursor.get() <<
            std::endl;
        //
    }

Note that column cursors are automatically positioned by the table cursor's position when thay are accessed,
so any column (or part of a column) that is not needed for a query will not receive any memory accesses
when the query is executed.

Additionally, a filtering and projection cursor can be composed to fetch certain rows:

    std::shared_ptr<db::TableCursor> tc = table->getScanCursor();

    std::shared_ptr<db::Filter> leftFilter =
        std::make_shared<db::GreaterThanFilter<db::LongType>>("id", 31);
    std::shared_ptr<db::Filter> rightFilter =
        std::make_shared<db::GreaterThanFilter<db::DoubleType>>("cost", 100);
    std::shared_ptr<db::Filter> andFilter =
            std::make_shared<db::AndFilter>("id", leftFilter, rightFilter);

    db::FilterProjectTableCursor fptc(*tc, andFilter);

    // Note: the column cursors must always be obtained from the appropriate table cursor
    auto id_cursor = fptc.getLongColumn(std::string("id"));
    auto cost_cursor = fptc.getDoubleColumn(std::string("cost"));

    while (fptc.hasMore()) {
        // ...
    }

Table cursors can be composed arbitrarily:

    std::shared_ptr<db::TableCursor> tc = dbTable->getScanCursor();

    std::shared_ptr<db::Filter> first_filter =
        std::make_shared<db::GreaterThanFilter<db::LongType>>("id", 11);
    db::FilterProjectTableCursor first_cursor(*tc, first_filter);

    std::shared_ptr<db::Filter> second_filter =
        std::make_shared<db::LessThanFilter<db::DoubleType>>("cost", 42);
    db::FilterProjectTableCursor second_cursor(first_cursor, second_filter);

## Null values

Support for nulls is based ont he native support in Arrow. Create data with nulls by
calling `db::null_val()` as follows:

    table->addRow({db::long_val(11), db::null_val()});

Then call `isNull()` on a column cursor to check before attempting to obtain a non-null value.

## Memory management

To allocate an table within a specific memory pool, pass the pool as an optional extra parameter to the
`DBTable::create()` call.

## Error handling is similar to that in Arrow. See [libdb/core/Status.h](libdb/core/Status.h).

## More Examples

See the unit tests in [testdb/TableTest.cpp](testdb/TableTest.cpp) for more examples of how to use the query
framework, and the test setup code in [testdb/Tables.cpp](testdb/Tables.cpp) for more examples of creating and
populating tables.

# Things Not Yet Investigated

* Error handling is poorly integrated with Arrow's error handling
* Filtering is not pushed down into dictionaries
* Data representation
  * Non-relational data cannot yet be represented
* A full range of column types (currently just int64, double and string)
* Performance and scale
* Vectorized execution -- in fact the framework currently mnakes heavy use of virtual methods at considerable cost
* Parallelism

# Dependencies

* GCC (using 5.4.0)
* CMake (using 3.5.1)
* Apache Arrow (using 5.x.x)
* Googletest (≥ 1.8.0)

# Building

    $ mkdir <build root>
    $ cd <build root>
    $ cmake <source root> -DCMAKE_BUILD_TYPE=Debug -DGTEST_ROOT=<googletest root> -DARROW_ROOT=<arrow root>
    $ make clean
    $ make

# Running Tests

    $ <build root>/testdb/testdb


