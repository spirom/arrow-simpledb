//
// Created by spiro on 11/7/17.
//

#ifndef GENERICCOLUMNCURSOR_H
#define GENERICCOLUMNCURSOR_H

class ScanTableCursor;

class GenericColumnCursor {
public:
    virtual ~GenericColumnCursor() {}
    virtual void reset() = 0;
protected:
    GenericColumnCursor(ScanTableCursor &table_cursor);
    ScanTableCursor &get_table_cursor();

private:
    ScanTableCursor &_table_cursor;
};


#endif // GENERICCOLUMNCURSOR_H
