//
// Created by spiro on 11/7/17.
//

#ifndef GENERICCOLUMNCURSOR_H
#define GENERICCOLUMNCURSOR_H

class TableCursor;

class GenericColumnCursor {
public:
    enum Encoding { PLAIN = 0, DICT };
    virtual ~GenericColumnCursor() {}
    virtual void reset() = 0;
protected:
    GenericColumnCursor(TableCursor &table_cursor);
    TableCursor &get_table_cursor();

private:
    TableCursor &_table_cursor;
};


#endif // GENERICCOLUMNCURSOR_H
