

#ifndef FILTER_H
#define FILTER_H

class TableCursor;

class Filter {

public:

    virtual void initialize(TableCursor& table_cursor) = 0;

    virtual bool evaluate() = 0;

    virtual ~Filter() {}
};

#endif // FILTER_H

