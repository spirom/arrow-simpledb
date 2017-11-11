
# A Simple In-Memory Database Using Apache Arrow

# Queries Over Arrow Tables

# Things Not Yet Investigated

* A cleaner way to create tables
* Encodings (dictionary, ...)
* Data representation
** Nulls
** Non-relational data
* A full range of column types (currently just int64 and double)
* Vectorized execution

# Dependencies

* GCC (using 5.4.0)
* CMake (using 3.5.1)
* Apache Arrow (using 0.7.1)
* Googletest (using 1.8.0)

# Building

    $
    $ mkdir <path to build directory>
    $ cd <path to build directory>
    $ cmake <path to source directory> -DCMAKE_BUILD_TYPE=Debug
    $ make

# Running Tests

    $ <path to build directory>/testdb/testdb


