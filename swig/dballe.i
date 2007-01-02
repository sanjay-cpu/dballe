// dballe.i - SWIG interface
%module Dballe

%include "std_string.i"
%include "typemaps.i"
%include "exception.i"

%exception {
	try { $action }
	catch (std::exception& e)
        {
                SWIG_exception(SWIG_RuntimeError, e.what());
        }
}


%{
#include <dballe++/db.h>
#include <dballe++/format.h>
#include <dballe/core/aliases.h>
#include <iostream>

using namespace dballe;

%}

#ifdef SWIGPYTHON

%extend dballe::Var {
        %pythoncode %{
                def __cmp__(self, other):
                        if other == None:
                                return 1
                        codea = self.code()
                        codeb = self.code()
                        if codea != codeb:
                                return cmp(codea, codeb)
                        isstra = self.info().is_string()
                        isstrb = other.info().is_string()
                        if isstra and isstrb:
                                return cmp(self.enqc(), other.enqc())
                        elif isstra and not isstrb:
                                return 1
                        elif not isstra and isstrb:
                                return -1
                        else:
                                return cmp(self.enqi(), other.enqi())
        %}
}

%extend dballe::Record {
        %pythoncode %{
                def __iter__(self):
                        i = self.begin()
                        while i.valid():
                                yield i.var()
                                i.next()
        %}
}

%extend dballe::Cursor {
        %pythoncode %{
                def __iter__(self):
                        record = Record()
                        while self.next(record):
                                yield record
        %}
}

// Rewrite Record methods to make use of the None value
%extend dballe::Record {
        %rename enqi enqi_orig;
        %rename enqd enqd_orig;
        %rename enqs enqs_orig;
        %rename enqc enqc_orig;
        %rename seti seti_orig;
        %rename setd setd_orig;
        %rename sets sets_orig;
        %rename setc setc_orig;
        %pythoncode %{
                def enqi(self, name):
                       if self.contains(name):
                                return self.enqi_orig(name)
                       else:
                                return None
                def enqd(self, name):
                       if self.contains(name):
                                return self.enqd_orig(name)
                       else:
                                return None
                def enqs(self, name):
                       if self.contains(name):
                                return self.enqs_orig(name)
                       else:
                                return None
                def enqc(self, name):
                       return self.enqs(name)

                def seti(self, name, value):
                       if value == None:
                                self.unset(name)
                       else:
                                self.seti_orig(name, value)
                def setd(self, name, value):
                       if value == None:
                                self.unset(name)
                       else:
                                self.setd_orig(name, value)
                def sets(self, name, value):
                       if value == None:
                                self.unset(name)
                       else:
                                self.sets_orig(name, value)
                def setc(self, name, value):
                       return self.sets(name, value)
        %}
}

%typemap(in) dba_keyword {
	$1 = dba_record_keyword_byname(PyString_AsString($input));
}

%typemap(in) dba_encoding {
	const char* tmp = PyString_AsString($input);
        if (strcmp(tmp, "BUFR") == 0)
                $1 = BUFR;
        else if (strcmp(tmp, "CREX") == 0)
                $1 = CREX;
        else if (strcmp(tmp, "AOF") == 0)
                $1 = AOF;
        else
                throw std::runtime_error(std::string("Unknown encoding '") + tmp + "'");
}

%typemap(in) dba_varcode {
	const char* tmp = PyString_AsString($input);
        if (($1 = dba_varcode_alias_resolve(tmp)) == 0)
                $1 = DBA_STRING_TO_VAR(tmp + 1);
}

%typemap(out) dba_varcode {
	char buf[10];
	snprintf(buf, 10, "B%02d%03d", DBA_VAR_X($1), DBA_VAR_Y($1));
	$result = PyString_FromString(buf);
}

%typemap(in, numinputs=0) dba_varcode *varcode (dba_varcode temp) {
	$1 = &temp;
}
%typemap(argout) dba_varcode *varcode {
	char buf[10];
	snprintf(buf, 10, "B%02d%03d", DBA_VAR_X(*$1), DBA_VAR_Y(*$1));
	$result = SWIG_Python_AppendOutput($result, SWIG_FromCharPtr(buf));
}

%apply int *OUTPUT { int *count };
%apply int *OUTPUT { int *contextid };
%apply int *OUTPUT { int *anaid };

#endif

%include <dballe++/var.h>
%include <dballe++/record.h>
%include <dballe++/db.h>
%include <dballe++/format.h>
