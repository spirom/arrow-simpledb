

#ifndef NOT_FILTER_H
#define NOT_FILTER_H

#include <memory>
#include <string>
#include "Filter.h"

namespace db {

    class NotFilter : public Filter {
    public:

        explicit NotFilter(std::shared_ptr<Filter> underlying);

        void initialize(TableCursor &table_cursor) override;

        bool evaluate() override;

    private:

        std::shared_ptr<Filter> _underlying;
    };

};

#endif //NOT_FILTER_H
