//
// Created by spiro on 11/7/17.
//

#ifndef GENERICCOLUMNCURSOR_H
#define GENERICCOLUMNCURSOR_H

#include <cstdint>

class TableCursor;

class GenericColumnCursor {
public:
    enum Encoding { PLAIN = 0, DICT };
    virtual ~GenericColumnCursor() = default;
    virtual void reset() = 0;
protected:
    explicit GenericColumnCursor(TableCursor &table_cursor);
    uint64_t get_table_cursor_position();

private:
    TableCursor &_table_cursor;
};


#endif // GENERICCOLUMNCURSOR_H
