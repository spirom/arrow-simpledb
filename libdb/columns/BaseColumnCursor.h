

#ifndef BASECOLUMNCURSOR_H
#define BASECOLUMNCURSOR_H


#include <arrow/table.h>

template <typename T>
class ColumnTypeTrait {

};

template <>
class ColumnTypeTrait<arrow::StringType>
{
public:
    typedef typename arrow::StringArray ArrayType;
    typedef std::string ReturnType;
};


template <>
class ColumnTypeTrait<arrow::Int64Type>
{
public:
    typedef typename arrow::NumericArray<arrow::Int64Type> ArrayType;
    typedef typename arrow::Int64Type::c_type ReturnType;
};

template <>
class ColumnTypeTrait<arrow::DoubleType>
{
public:
    typedef typename arrow::NumericArray<arrow::DoubleType> ArrayType;
    typedef typename arrow::DoubleType::c_type ReturnType;
};

template <typename T>
class BaseColumnCursor : public ColumnTypeTrait<T> {
public:

    /**
     * Will next() produce another element?
     * @return
     */
    virtual bool hasMore() = 0;

    /**
     * Move to the next element.
     * @return True if an element is available, false otherwise (end of column.)
     */
    virtual bool next() = 0;

    /**
     * Is the element at the current position null?
     * @return
     */
    virtual bool isNull() = 0;

    /**
     * Get value at current position.
     * @return
     */
    virtual typename ColumnTypeTrait<T>::ReturnType get() = 0;

    /**
     * Reset to the first element, if any.
     */
    virtual void reset() = 0;

    /**
     * Seek to the given position.
     * @param to zero-based ordinal position of element in column
     * @return True if successful.
     */
    virtual bool seek(uint64_t to) = 0;

    virtual ~BaseColumnCursor() {}

};


#endif // BASECOLUMNCURSOR_H
