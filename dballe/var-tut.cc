#include "core/test-utils-core.h"
#include "var.h"

using namespace std;
using namespace wibble::tests;
using namespace dballe;

namespace {

typedef dballe::tests::test_group<> test_group;
typedef test_group::Test Test;
typedef test_group::Fixture Fixture;

std::vector<Test> tests {
    Test("resolve", [](Fixture& f) {
        wassert(actual(resolve_varcode("B12101")) == WR_VAR(0, 12, 101));
        wassert(actual(resolve_varcode("t")) == WR_VAR(0, 12, 101));
        try {
            resolve_varcode("B121");
            ensure(false);
        } catch (std::exception& e) {
            wassert(actual(e.what()).contains("cannot parse"));
        }
    }),
};

test_group newtg("dballe_var", tests);

}

