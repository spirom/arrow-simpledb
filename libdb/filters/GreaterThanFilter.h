

#ifndef GREATERTHANFILTER_H
#define GREATERTHANFILTER_H

#include <string>
#include "filters/Filter.h"
#include "TableCursor.h"
#include "ColumnCursorWrapper.h"

class GreaterThanFilter : public Filter {
public:

    GreaterThanFilter(std::string column_name, double value);

    void initialize(TableCursor& table_cursor) override;

    bool evaluate() override;

private:

    std::string _column_name;

    double _value;

    std::shared_ptr<ColumnCursorWrapper<arrow::Int64Array>> _cursor;
};


#endif // GREATERTHANFILTER_H
