#include "dballe/core/tests.h"
#include "fortran.h"
#include <cstring>

using namespace dballe;
using namespace dballe::tests;

namespace {

class Tests : public TestCase
{
    using TestCase::TestCase;

    void register_tests() override;
} tests("core_fortran");

void Tests::register_tests()
{

add_method("empty", []{
});

}

}