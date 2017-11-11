

#ifndef LESSTHANFILTER_H
#define LESSTHANFILTER_H

#include <string>
#include "filters/Filter.h"
#include "TableCursor.h"
#include "ColumnCursorWrapper.h"

class LessThanFilter  : public Filter {
public:

    LessThanFilter(std::string column_name, double value);

    void initialize(TableCursor &table_cursor) override;

    bool evaluate() override;

private:

    std::string _column_name;

    double _value;

    std::shared_ptr <ColumnCursorWrapper<arrow::DoubleArray>> _cursor;
};


#endif // LESSTHANFILTER_H
