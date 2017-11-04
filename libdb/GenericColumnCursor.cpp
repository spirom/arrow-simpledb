

#include "GenericColumnCursor.h"

GenericColumnCursor::GenericColumnCursor(ScanTableCursor &table_cursor)
        : _table_cursor(table_cursor)
{

}

ScanTableCursor&
GenericColumnCursor::get_table_cursor()
{
    return _table_cursor;
}