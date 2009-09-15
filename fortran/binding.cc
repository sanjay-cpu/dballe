/*
 * DB-ALLe - Archive for punctual meteorological data
 *
 * Copyright (C) 2005--2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include "dbapi.h"
#include "msgapi.h"
#include <dballe/core/verbose.h>
#include <dballe/core/aliases.h>
#include <dballe/db/db.h>
#include <dballe/db/cursor.h>
#include <dballe/db/internals.h>
#include <dballe/msg/formatter.h>
#include <dballe/init.h>

extern "C" {
#include <f77.h>
}
#include <float.h>
#include <limits.h>

#include <stdio.h>	// snprintf
#include <string.h>	// memset
//#include <math.h>

#include "handles.h"

//#define TRACEMISSING(type) fprintf(stderr, "SET TO MISSING (" type ")\n")
#define TRACEMISSING(type) do {} while(0)

/*
 * First attempt using constants
 */
//#define MISSING_STRING ""
// Largest signed one byte value
#define MISSING_BYTE SCHAR_MAX
// integer 2 byte   32767
// Largest signed int value
#define MISSING_INT INT_MAX
//#define MISSING_REAL (3.4028235E+38f)
// Largest positive float value
#define MISSING_REAL FLT_MAX
// Largest positive double value
#define MISSING_DOUBLE   DBL_MAX
//#define MISSING_DOUBLE   (1.79769E+308)
//#define MISSING_DOUBLE   (1.7976931348623167E+308)
//#define MISSING_DOUBLE   (1.797693134862316E+308)
//#define MISSING_DOUBLE   (1.79769313486E+308)
//#define MISSING_DOUBLE ((double)0x7FEFFFFFFFFFFFFF)

using namespace dballef;

static inline void tofortran(int& val)
{
	if (val == API::missing_int)
		val = MISSING_INT;
}
static inline int fromfortran(int val)
{
	return val == MISSING_INT ? API::missing_int : val;
}

/** @defgroup fortransimple Simplified interface for Dballe
 * @ingroup fortran
 *
 * This module provides a simplified fortran API to Dballe.  The interface is
 * simplified by providing functions with fewer parameters than their
 * counterparts in the full interface, and the omitted parameters are replaced
 * by useful defaults.
 *
 * The resulting interface is optimized for the common usage, making it faster
 * and less error prone.  However, when creating complicated code with more
 * parallel reads and writes, it may be useful to use the functions in
 * ::fortranfull instead, because all parameters are explicit and their precise
 * semantics is always evident.
 *
 * This is a sample code for a query session with the simplified interface:
 * \code
 *   call idba_presentati(dba, "myDSN", "mariorossi", "CippoLippo")
 *   call idba_preparati(dba, handle, "read", "read", "read")
 *   call idba_setr(handle, "latmin", 30.)
 *   call idba_setr(handle, "latmax", 50.)
 *   call idba_setr(handle, "lonmin", 10.)
 *   call idba_setr(handle, "lonmax", 20.)
 *   call idba_voglioquesto(handle, count)
 *   do i=1,count
 *      call idba_dammelo(handle, param)
 *      call idba_enqd(handle, param, ...)
 *      call idba_enqi(handle, ...)
 *      call idba_enqr(handle, ...)
 *      call idba_enqd(handle, ...)
 *      call idba_enqc(handle, ...)
 *      call idba_voglioancora(handle, countancora)
 *      do ii=1,count
 *         call idba_ancora(handle, param)
 *         call idba_enqi(handle, param)
 *      enddo
 *   enddo
 *   call idba_fatto(handle)
 *   call idba_arrivederci(dba)
 * \endcode
 *
 * This is a sample code for a data insert session with the simplified interface:
 * \code
 *   call idba_presentati(dba, "myDSN", "mariorossi", "CippoLippo")
 *   call idba_preparati(dba, handle, "reuse", "add", "add")
 *   call idba_scopa(handle, "")
 *   call idba_setr(handle, "lat", 30.)
 *   call idba_setr(handle, "lon", 10.)
 *   call idba_seti(handle, .....)
 *   call idba_seti(handle, "B12011", 5)
 *   call idba_seti(handle, "B12012", 10)
 *   call idba_prendilo(handle)
 *   call idba_setc(handle, "*var", "B12012")
 *   call idba_seti(handle, "*B33101", 50)
 *   call idba_seti(handle, "*B33102", 75)
 *   call idba_critica(handle)
 *   call idba_setc(handle, "*var", "B12011")
 *   call idba_seti(handle, "*B33101", 50)
 *   call idba_seti(handle, "*B33102", 75)
 *   call idba_critica(handle)
 *   call idba_fatto(handle)
 *   call idba_arrivederci(dba)
 * \endcode
 */

/** @file
 * @ingroup fortransimple
 * Simplified interface for Dballe.
 *
 * Every function returns an error indicator, which is 0 if no error happened,
 * or 1 if there has been an error.
 *
 * When an error happens, the functions in fdba_error.c can be used
 * to get detailed informations about it.
 *
 * \par Internals of the simplified interface
 *
 * Behind the handle returned by idba_preparati() there are a set of variables
 * that are used as implicit parameters:
 *
 * \li \c query ::dba_record, used to set the parameters of the query made by idba_voglioquesto()
 * \li \c work ::dba_record, used by idba_dammelo() to return the parameters 
 * \li \c qc ::dba_record, used to manipulate qc data.  Every time the \ref idba_enq or \ref idba_set functions are used with a variable name starting with an asterisk, they will manipulate the \c qc record instead of the others.
 * \li \c ana ::dba_cursor, used to iterate on the results of idba_quantesono()
 * \li \c query ::dba_cursor, used to iterate on the results of idba_voglioquesto()
 *
 * The simplified interface has two possible states: \c QUERY and \c RESULT.
 * Then the interface is in the \c QUERY state, the \ref idba_enq and \ref
 * idba_set functions operate in the \c query ::dba_record, to set and check the
 * parameters of a query.  idba_voglioquesto() reads the parameters from the \c
 * query ::dba_record and switches the state to \c RESULT, and further calls to
 * idba_dammelo() will put the query results in the \c work ::dba_record, to be read by
 * the \ref idba_enq functions.
 *
 * In the \c RESULT state, the \ref idba_enq and \ref idba_set functions
 * operate on the \c work ::dba_record, to inspect the results of the queries.  A
 * call to idba_ricominciamo() terminates the current query and goes back to
 * the \c QUERY state, resetting the contents of all the ::dba_record of the interface.
 *
 * idba_prendilo() inserts in the database the data coming from the \c
 * QUERY ::dba_record if invoked in the \c query state, or the data coming from the
 * \c RESULT ::dba_record if invoked in the \c result state.  This is done
 * because inserting new values in the database should be independent from the
 * state.
 *
 * \ref qc functions instead always operate on the \c qc ::dba_record, which is
 * accessed with the \ref idba_enq and \ref idba_set functions by prefixing the
 * parameter name with an asterisk.
 */

/* Handles to give to Fortran */

#define MAX_SIMPLE 50
#define MAX_SESSION 10

FDBA_HANDLE_BODY(simple, MAX_SIMPLE, "Dballe simple sessions")
FDBA_HANDLE_BODY(session, MAX_SESSION, "Dballe sessions")

#define STATE (FDBA_HANDLE(simple, *handle))
#define SESSION (FDBA_HANDLE(session, FDBA_HANDLE(simple, *handle).session).session)

static int usage_refcount = 0;

static void lib_init()
{
	if (usage_refcount > 0)
		return;

	dba_init();
	fdba_handle_init_session();
	fdba_handle_init_simple();

	++usage_refcount;
}

#if 0
static inline int double_is_missing(double d)
{
	switch (fpclassify(d))
	{
		case FP_ZERO:
			return 0;
		case FP_NORMAL:
			return d == MISSING_DOUBLE;
		default:
			return 1;
	}
}
#endif

extern "C" {

/**
 * Start working with a DBALLE database.
 *
 * This function can be called more than once once to connect to different
 * databases at the same time.
 * 
 * @param dsn
 *   The ODBC DSN of the database to use
 * @param user
 *   The username used to connect to the database
 * @param password
 *   The username used to connect to the database
 * @retval dbahandle
 *   The database handle that can be passed to idba_preparati to work with the
 *   database.
 * @return
 *   The error indication for the function.
 */
F77_INTEGER_FUNCTION(idba_presentati)(
		INTEGER(dbahandle),
		CHARACTER(dsn),
		CHARACTER(user),
		CHARACTER(password)
		TRAIL(dsn)
		TRAIL(user)
		TRAIL(password))
{
	GENPTR_INTEGER(dbahandle)
	GENPTR_CHARACTER(dsn)
	GENPTR_CHARACTER(user)
	GENPTR_CHARACTER(password)
	char s_dsn[50];
	char s_user[20];
	char s_password[20];
	dba_err err;

	/* Import input string parameters */
	cnfImpn(dsn, dsn_length, 49, s_dsn); s_dsn[49] = 0;
	cnfImpn(user, user_length, 19, s_user); s_user[19] = 0;
	cnfImpn(password, password_length, 19, s_password); s_password[19] = 0;

	/* Initialize the library if needed */
	lib_init();

	/* Allocate and initialize a new handle */
	DBA_RUN_OR_RETURN(fdba_handle_alloc_session(dbahandle));

	/* Open the DBALLE session */
	DBA_RUN_OR_GOTO(fail, dba_db_create(s_dsn, s_user, s_password,
				&(FDBA_HANDLE(session, *dbahandle).session)));

	/* Open the database session */
	return dba_error_ok();

fail:
	return err;
}

/**
 * Stop working with a DBALLE database
 *
 * @param dbahandle
 *   The database handle to close.
 */
F77_SUBROUTINE(idba_arrivederci)(INTEGER(dbahandle))
{
	GENPTR_INTEGER(dbahandle)

	dba_db_delete(FDBA_HANDLE(session, *dbahandle).session);
	FDBA_HANDLE(session, *dbahandle).session = NULL;
	fdba_handle_release_session(*dbahandle);

	/*
	dba_shutdown does not exist anymore, but I keep this code commented out
	here as a placeholder if in the future we'll need to hook actions when the
	usage refcount goes to 0

	if (--usage_refcount == 0)
		dba_shutdown();
	*/
}

/**
 * Starts a session with dballe.
 *
 * You can call idba_preparati() many times and get more handles.  This allows
 * to perform many operations on the database at the same time.
 *
 * idba_preparati() has three extra parameters that can be used to limit
 * write operations on the database, as a limited protection against
 * programming errors.
 *
 * Note that some combinations of parameters are illegal, such as anaflag=read
 * and dataflag=add (when adding a new data, it's sometimes necessary to insert
 * new pseudoana records), or dataflag=rewrite and qcflag=read (when deleting
 * data, their attributes are deleted as well).
 *
 * @param dbahandle
 *   The main DB-ALLe connection handle
 * @retval handle
 *   The session handle returned by the function
 * @param anaflag
 *   Controls access to pseudoana records and can have these values:
 *   \li \c "read" pseudoana records cannot be inserted.
 *   \li \c "write" it is possible to insert and delete pseudoana records.
 * @param dataflag
 *   Controls access to observed data and can have these values:
 *    \li \c "read" data cannot be modified in any way.
 *    \li \c "add" data can be added to the database, but existing data cannot be
 *    modified.  Deletions are disabled.  This is used to insert new data in the
 *    database while preserving the data that was already present in it.
 *    \li \c "write" data can freely be added, overwritten and deleted.
 * @param qcflag
 *    Controls access to data attributes and can have these values:
 *    \li \c "read" attributes cannot be modified in any way.
 *    \li \c "add" attributes can can be added to the database, but existing
 *    attributes cannot be modified.  Deletion of attributes is disabled.  This is
 *    used to insert new attribute in the database while preserving the attributes
 *    that were already present in it.
 *    \li \c "write" attributes can freely be added, overwritten and deleted.
 * @return
 *   The error indication for the function.
 */
F77_INTEGER_FUNCTION(idba_preparati)(
		INTEGER(dbahandle),
		INTEGER(handle),
		CHARACTER(anaflag),
		CHARACTER(dataflag),
		CHARACTER(attrflag)
		TRAIL(anaflag)
		TRAIL(dataflag)
		TRAIL(attrflag))
{
	GENPTR_INTEGER(dbahandle)
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(anaflag)
	GENPTR_CHARACTER(dataflag)
	GENPTR_CHARACTER(attrflag)
	dba_err err;
	char c_anaflag[10];
	char c_dataflag[10];
	char c_attrflag[10];

	cnfImpn(anaflag, anaflag_length,  10, c_anaflag);
	cnfImpn(dataflag, dataflag_length,  10, c_dataflag);
	cnfImpn(attrflag, attrflag_length,  10, c_attrflag);

	/* Check here to warn users of the introduction of idba_presentati */
	/*
	if (session == NULL)
		return dba_error_consistency("idba_presentati should be called before idba_preparati");
	*/

	/* Allocate and initialize a new handle */
	DBA_RUN_OR_RETURN(fdba_handle_alloc_simple(handle));

	STATE.session = *dbahandle;
	try {
		STATE.api = new DbAPI(FDBA_HANDLE(session, *dbahandle).session, c_anaflag, c_dataflag, c_attrflag);
	} catch (APIException& e) {
		err = e.err;
		goto fail;
	}
	if (!STATE.api)
	{
		err = dba_error_alloc("Allocating a new DbAPI");
		goto fail;
	}

	return dba_error_ok();

fail:
	if (STATE.api != NULL)
		delete STATE.api;

	fdba_handle_release_simple(*handle);

	return err;
}

/**
 * Access a file with wheter messages
 *
 * @retval handle
 *   The session handle returned by the function
 * @param filename
 *   Name of the file to open
 * @param mode
 *   File open mode.  It can be:
 *   \li \c r for read
 *   \li \c w for write (the old file is deleted)
 *   \li \c a for append
 * @param type
 *   Format of the data in the file.  It can be:
 *   \li \c "BUFR"
 *   \li \c "CREX"
 *   \li \c "AOF" (read only)
 *   \li \c "AUTO" (autodetect, read only)
 * @param force_report
 *   if 0, nothing happens; otherwise, choose the output message template
 *   using this report type instead of the one in the message
 * @return
 *   The error indication for the function.
 */
F77_INTEGER_FUNCTION(idba_messaggi)(
		INTEGER(handle),
		CHARACTER(filename),
		CHARACTER(mode),
		CHARACTER(type)
		TRAIL(filename)
		TRAIL(mode)
		TRAIL(type))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(filename)
	GENPTR_CHARACTER(mode)
	GENPTR_CHARACTER(type)
	dba_err err;
	char c_filename[512];
	char c_mode[10];
	char c_type[10];

	cnfImpn(filename, filename_length,  512, c_filename);
	cnfImpn(mode, mode_length,  10, c_mode);
	cnfImpn(type, type_length,  10, c_type);

	lib_init();

	/* Allocate and initialize a new handle */
	DBA_RUN_OR_RETURN(fdba_handle_alloc_simple(handle));

	STATE.session = 0;
	try {
		STATE.api = new MsgAPI(c_filename, c_mode, c_type);
	} catch (APIException& e) {
		err = e.err;
		goto fail;
	}
	if (!STATE.api)
	{
		err = dba_error_alloc("Allocating a new MsgAPI");
		goto fail;
	}

	return dba_error_ok();

fail:
	if (STATE.api != NULL)
		delete STATE.api;

	fdba_handle_release_simple(*handle);

	return err;
}


/**
 * Ends a session with DBALLE
 *
 * @param handle
 *   Handle to the session to be closed.
 */
F77_INTEGER_FUNCTION(idba_fatto)(INTEGER(handle))
{
	GENPTR_INTEGER(handle)

	delete STATE.api;
	fdba_handle_release_simple(*handle);

	return dba_error_ok();
}

/**
 * Reset the database contents, loading default report informations from a file.
 *
 * It only works in rewrite mode.
 *
 * @param handle
 *   Handle to a DBALLE session
 * @param repinfofile
 *   CSV file with the default report informations.  See dba_reset()
 *   documentation for the format of the file.
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_scopa)(INTEGER(handle), CHARACTER(repinfofile) TRAIL(repinfofile))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(repinfofile)
	char fname[PATH_MAX];

	cnfImpn(repinfofile, repinfofile_length,  PATH_MAX, fname); fname[PATH_MAX - 1] = 0;

	try {
		if (fname[0] == 0)
			STATE.api->scopa(0);
		else
			STATE.api->scopa(fname);

		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**@name idba_enq*
 * @anchor idba_enq
 * Functions used to read the output values of the DBALLE action routines
 * @{
 */

/**
 * Read one integer value from the output record
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param parameter
 *   Parameter to query.  It can be the code of a WMO variable prefixed by \c
 *   "B" (such as \c "B01023"); the code of a QC value prefixed by \c "*B"
 *   (such as \c "*B01023") or a keyword among the ones defined in \ref
 *   dba_record_keywords
 * @param value
 *   Where the value will be returned
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_enqi)(
		INTEGER(handle),
		CHARACTER(parameter),
		INTEGER(value)
		TRAIL(parameter))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)
	GENPTR_INTEGER(value)
	char parm[20];
	cnfImpn(parameter, parameter_length, 19, parm); parm[19] = 0;

	try {
		*value = STATE.api->enqi(parm);
		tofortran(*value);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Read one byte value from the output record
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param parameter
 *   Parameter to query.  It can be the code of a WMO variable prefixed by \c
 *   "B" (such as \c "B01023"); the code of a QC value prefixed by \c "*B"
 *   (such as \c "*B01023") or a keyword among the ones defined in \ref
 *   dba_record_keywords
 * @param value
 *   Where the value will be returned
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_enqb)(
		INTEGER(handle),
		CHARACTER(parameter),
		BYTE(value)
		TRAIL(parameter))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)
	GENPTR_BYTE(value)
	char parm[20];
	cnfImpn(parameter, parameter_length, 19, parm); parm[19] = 0;

	try {
		*value = STATE.api->enqb(parm);
		if (*value == API::missing_byte)
			*value = MISSING_BYTE;
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}


/**
 * Read one real value from the output record
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param parameter
 *   Parameter to query.  It can be the code of a WMO variable prefixed by \c
 *   "B" (such as \c "B01023"); the code of a QC value prefixed by \c "*B"
 *   (such as \c "*B01023") or a keyword among the ones defined in \ref
 *   dba_record_keywords
 * @param value
 *   Where the value will be returned
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_enqr)(
		INTEGER(handle),
		CHARACTER(parameter),
		REAL(value)
		TRAIL(parameter))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)
	GENPTR_REAL(value)
	char parm[20];
	cnfImpn(parameter, parameter_length, 19, parm); parm[19] = 0;

	try {
		*value = STATE.api->enqr(parm);
		if (*value == API::missing_float)
			*value = MISSING_REAL;
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Read one real*8 value from the output record
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param parameter
 *   Parameter to query.  It can be the code of a WMO variable prefixed by \c
 *   "B" (such as \c "B01023"); the code of a QC value prefixed by \c "*B"
 *   (such as \c "*B01023") or a keyword among the ones defined in \ref
 *   dba_record_keywords
 * @param value
 *   Where the value will be returned
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_enqd)(
		INTEGER(handle),
		CHARACTER(parameter),
		DOUBLE(value)
		TRAIL(parameter))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)
	GENPTR_DOUBLE(value)
	char parm[20];
	cnfImpn(parameter, parameter_length, 19, parm); parm[19] = 0;

	try {
		*value = STATE.api->enqd(parm);
		if (*value == API::missing_double)
			*value = MISSING_DOUBLE;
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Read one character value from the output record
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param parameter
 *   Parameter to query.  It can be the code of a WMO variable prefixed by \c
 *   "B" (such as \c "B01023"); the code of a QC value prefixed by \c "*B"
 *   (such as \c "*B01023") or a keyword among the ones defined in \ref
 *   dba_record_keywords
 * @param value
 *   Where the value will be returned
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_enqc)(
		INTEGER(handle),
		CHARACTER(parameter),
		CHARACTER(value)
		TRAIL(parameter)
		TRAIL(value))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)
	GENPTR_CHARACTER(value)
	char parm[20];
	cnfImpn(parameter, parameter_length, 19, parm); parm[19] = 0;

	try {
		const char* v = STATE.api->enqc(parm);
		if (!v)
		{
			if (value_length > 0)
			{
				// The missing string value has been defined as a
				// null byte plus blank padding.
				value[0] = 0;
				memset(value+1, ' ', value_length - 1);
			}
		} else
			cnfExprt(v, value, value_length);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/*@}*/

/**@name idba_set*
 * @anchor idba_set
 * Functions used to read the input values for the DBALLE action routines
 *@{*/

/**
 * Set one integer value into the input record
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param parameter
 *   Parameter to set.  It can be the code of a WMO variable prefixed by \c
 *   "B" (such as \c "B01023"); the code of a QC value prefixed by \c "*B"
 *   (such as \c "*B01023") or a keyword among the ones defined in \ref
 *   dba_record_keywords
 * @param value
 *   The value to assign to the parameter
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_seti)(
		INTEGER(handle),
		CHARACTER(parameter),
		INTEGER(value)
		TRAIL(parameter))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)
	GENPTR_INTEGER(value)
	char parm[20];
	cnfImpn(parameter, parameter_length, 19, parm); parm[19] = 0;

	try {
		if (*value == MISSING_INT)
		{
			TRACEMISSING("int");
			STATE.api->unset(parm);
		}
		else
			STATE.api->seti(parm, *value);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Set one byte value into the input record
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param parameter
 *   Parameter to set.  It can be the code of a WMO variable prefixed by \c
 *   "B" (such as \c "B01023"); the code of a QC value prefixed by \c "*B"
 *   (such as \c "*B01023") or a keyword among the ones defined in \ref
 *   dba_record_keywords
 * @param value
 *   The value to assign to the parameter
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_setb)(
		INTEGER(handle),
		CHARACTER(parameter),
		BYTE(value)
		TRAIL(parameter))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)
	GENPTR_BYTE(value)
	char parm[20];
	cnfImpn(parameter, parameter_length, 19, parm); parm[19] = 0;

	try {
		if (*value == MISSING_BYTE)
		{
			TRACEMISSING("byte");
			STATE.api->unset(parm);
		}
		else
			STATE.api->setb(parm, *value);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}


/**
 * Set one real value into the input record
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param parameter
 *   Parameter to set.  It can be the code of a WMO variable prefixed by \c
 *   "B" (such as \c "B01023"); the code of a QC value prefixed by \c "*B"
 *   (such as \c "*B01023") or a keyword among the ones defined in \ref
 *   dba_record_keywords
 * @param value
 *   The value to assign to the parameter
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_setr)(
		INTEGER(handle),
		CHARACTER(parameter),
		REAL(value)
		TRAIL(parameter))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)
	GENPTR_REAL(value)
	char parm[20];
	cnfImpn(parameter, parameter_length, 19, parm); parm[19] = 0;

	try {
		if (*value == MISSING_REAL)
		{
			TRACEMISSING("real");
			STATE.api->unset(parm);
		}
		else
			STATE.api->setr(parm, *value);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Set one real*8 value into the input record
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param parameter
 *   Parameter to set.  It can be the code of a WMO variable prefixed by \c
 *   "B" (such as \c "B01023"); the code of a QC value prefixed by \c "*B"
 *   (such as \c "*B01023") or a keyword among the ones defined in \ref
 *   dba_record_keywords
 * @param value
 *   The value to assign to the parameter
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_setd)(
		INTEGER(handle),
		CHARACTER(parameter),
		DOUBLE(value)
		TRAIL(parameter))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)
	GENPTR_DOUBLE(value)
	char parm[20];
	cnfImpn(parameter, parameter_length, 19, parm); parm[19] = 0;

	try {
		if (*value == MISSING_DOUBLE)
		{
			TRACEMISSING("double");
			STATE.api->unset(parm);
		}
		else
			STATE.api->setd(parm, *value);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Set one character value into the input record
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param parameter
 *   Parameter to set.  It can be the code of a WMO variable prefixed by \c
 *   "B" (such as \c "B01023"); the code of a QC value prefixed by \c "*B"
 *   (such as \c "*B01023") or a keyword among the ones defined in \ref
 *   dba_record_keywords
 * @param value
 *   The value to assign to the parameter
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_setc)(
		INTEGER(handle),
		CHARACTER(parameter),
		CHARACTER(value)
		TRAIL(parameter)
		TRAIL(value))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)
	GENPTR_CHARACTER(value)
	char parm[20];
	char val[255];
	cnfImpn(parameter, parameter_length, 19, parm); parm[19] = 0;
	cnfImpn(value, value_length, 254, val); val[254] = 0;

	try {
		if (val[0] == 0)
		{
			TRACEMISSING("char");
			STATE.api->unset(parm);
		}
		else
			STATE.api->setc(parm, val);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Shortcut function to set query parameters to the anagraphical context
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_setcontextana)(
		INTEGER(handle))
{
	GENPTR_INTEGER(handle)
	try {
		STATE.api->setcontextana();
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Shortcut function to read level data.
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @retval ltype
 *   Level type from the output record
 * @retval l1
 *   L1 from the output record
 * @retval l2
 *   L2 from the output record
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_enqlevel)(
		INTEGER(handle),
		INTEGER(ltype1),
		INTEGER(l1),
		INTEGER(ltype2),
		INTEGER(l2))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(ltype1)
	GENPTR_INTEGER(l1)
	GENPTR_INTEGER(ltype2)
	GENPTR_INTEGER(l2)
	try {
		STATE.api->enqlevel(*ltype1, *l1, *ltype2, *l2);
		tofortran(*ltype1); tofortran(*l1);
		tofortran(*ltype2); tofortran(*l2);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Shortcut function to set level data.
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param ltype
 *   Level type to set in the input record
 * @param l1
 *   L1 to set in the input record
 * @param l2
 *   L2 to set in the input record
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_setlevel)(
		INTEGER(handle),
		INTEGER(ltype1),
		INTEGER(l1),
		INTEGER(ltype2),
		INTEGER(l2))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(ltype1)
	GENPTR_INTEGER(l1)
	GENPTR_INTEGER(ltype2)
	GENPTR_INTEGER(l2)
	try {
		STATE.api->setlevel(
			fromfortran(*ltype1), fromfortran(*l1),
			fromfortran(*ltype2), fromfortran(*l2));
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Shortcut function to read time range data.
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @retval ptype
 *   P indicator from the output record
 * @retval p1
 *   P1 from the output record
 * @retval p2
 *   P2 from the output record
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_enqtimerange)(
		INTEGER(handle),
		INTEGER(ptype),
		INTEGER(p1),
		INTEGER(p2))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(ptype)
	GENPTR_INTEGER(p1)
	GENPTR_INTEGER(p2)
	try {
		STATE.api->enqtimerange(*ptype, *p1, *p2);
		tofortran(*ptype); tofortran(*p1); tofortran(*p2);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Shortcut function to set time range data.
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param ptype
 *   P indicator to set in the input record
 * @param p1
 *   P1 to set in the input record
 * @param p2
 *   P2 to set in the input record
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_settimerange)(
		INTEGER(handle),
		INTEGER(ptype),
		INTEGER(p1),
		INTEGER(p2))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(ptype)
	GENPTR_INTEGER(p1)
	GENPTR_INTEGER(p2)
	try {
		STATE.api->settimerange(
			fromfortran(*ptype), fromfortran(*p1), fromfortran(*p2));
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Shortcut function to read date information.
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @retval year
 *   Year from the output record
 * @retval month
 *   Month the output record
 * @retval day
 *   Day the output record
 * @retval hour
 *   Hour the output record
 * @retval min
 *   Minute the output record
 * @retval sec
 *   Second the output record
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_enqdate)(
		INTEGER(handle),
		INTEGER(year),
		INTEGER(month),
		INTEGER(day),
		INTEGER(hour),
		INTEGER(min),
		INTEGER(sec))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(year)
	GENPTR_INTEGER(month)
	GENPTR_INTEGER(day)
	GENPTR_INTEGER(hour)
	GENPTR_INTEGER(min)
	GENPTR_INTEGER(sec)
	try {
		STATE.api->enqdate(*year, *month, *day, *hour, *min, *sec);
		tofortran(*year), tofortran(*month), tofortran(*day);
		tofortran(*hour), tofortran(*min), tofortran(*sec);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Shortcut function to set date information.
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param year
 *   Year to set in the input record
 * @param month
 *   Month to set in the input
 * @param day
 *   Day to set in the input
 * @param hour
 *   Hour to set in the input
 * @param min
 *   Minute to set in the input
 * @param sec
 *   Second to set in the input
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_setdate)(
		INTEGER(handle),
		INTEGER(year),
		INTEGER(month),
		INTEGER(day),
		INTEGER(hour),
		INTEGER(min),
		INTEGER(sec))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(year)
	GENPTR_INTEGER(month)
	GENPTR_INTEGER(day)
	GENPTR_INTEGER(hour)
	GENPTR_INTEGER(min)
	GENPTR_INTEGER(sec)
	try {
		STATE.api->setdate(
			fromfortran(*year), fromfortran(*month), fromfortran(*day),
			fromfortran(*hour), fromfortran(*min), fromfortran(*sec));
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Shortcut function to set minimum date for a query.
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param year
 *   Minimum year to set in the query
 * @param month
 *   Minimum month to set in the query
 * @param day
 *   Minimum day to set in the query
 * @param hour
 *   Minimum hour to set in the query
 * @param min
 *   Minimum minute to set in the query
 * @param sec
 *   Minimum second to set in the query
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_setdatemin)(
		INTEGER(handle),
		INTEGER(year),
		INTEGER(month),
		INTEGER(day),
		INTEGER(hour),
		INTEGER(min),
		INTEGER(sec))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(year)
	GENPTR_INTEGER(month)
	GENPTR_INTEGER(day)
	GENPTR_INTEGER(hour)
	GENPTR_INTEGER(min)
	GENPTR_INTEGER(sec)
	try {
		STATE.api->setdatemin(
			fromfortran(*year), fromfortran(*month), fromfortran(*day),
			fromfortran(*hour), fromfortran(*min), fromfortran(*sec));
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Shortcut function to set maximum date for a query.
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param year
 *   Maximum year to set in the query
 * @param month
 *   Maximum month to set in the query
 * @param day
 *   Maximum day to set in the query
 * @param hour
 *   Maximum hour to set in the query
 * @param min
 *   Maximum minute to set in the query
 * @param sec
 *   Maximum second to set in the query
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_setdatemax)(
		INTEGER(handle),
		INTEGER(year),
		INTEGER(month),
		INTEGER(day),
		INTEGER(hour),
		INTEGER(min),
		INTEGER(sec))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(year)
	GENPTR_INTEGER(month)
	GENPTR_INTEGER(day)
	GENPTR_INTEGER(hour)
	GENPTR_INTEGER(min)
	GENPTR_INTEGER(sec)
	try {
		STATE.api->setdatemax(
			fromfortran(*year), fromfortran(*month), fromfortran(*day),
			fromfortran(*hour), fromfortran(*min), fromfortran(*sec));
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/*@}*/

/**
 * Remove one parameter from the input record
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param parameter
 *   Parameter to remove.  It can be the code of a WMO variable prefixed by \c
 *   "B" (such as \c "B01023"); the code of a QC value prefixed by \c "*B"
 *   (such as \c "*B01023") or a keyword among the ones defined in \ref
 *   dba_record_keywords
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_unset)(
		INTEGER(handle),
		CHARACTER(parameter)
		TRAIL(parameter))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)
	GENPTR_INTEGER(err)
	char parm[20];
	cnfImpn(parameter, parameter_length, 19, parm); parm[19] = 0;

	try {
		STATE.api->unset(parm);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Remove all parameters from the input record
 * 
 * @param handle
 *   Handle to a DBALLE session
 */
F77_SUBROUTINE(idba_unsetall)(
		INTEGER(handle))
{
	GENPTR_INTEGER(handle)

	STATE.api->unsetall();
}

/**
 * Count the number of elements in the anagraphical storage, and start a new
 * anagraphical query.
 *
 * Resulting anagraphical data can be retrieved with idba_elencamele()
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @param count
 *   The count of elements
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_quantesono)(
		INTEGER(handle),
		INTEGER(count))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(count)

	try {
		*count = STATE.api->quantesono();
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Iterate through the anagraphical data.
 *
 * Every invocation of this function will return a new anagraphical data, or
 * fill fail with code DBA_ERR_NOTFOUND when there are no more anagraphical
 * data available.
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_elencamele)(INTEGER(handle))
{
	GENPTR_INTEGER(handle)

	try {
		STATE.api->elencamele();
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Submit a query to the database.
 *
 * The query results can be accessed with calls to idba_dammelo.
 *
 * @param handle
 *   Handle to a DBALLE session
 * @retval count
 *   Number of values returned by the function
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_voglioquesto)(
		INTEGER(handle),
		INTEGER(count))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(count)

	try {
		*count = STATE.api->voglioquesto();
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Iterate through the query results data.
 *
 * Every invocation of this function will return a new result, or fill fail
 * with code DBA_ERR_NOTFOUND when there are no more results available.
 * 
 * @param handle
 *   Handle to a DBALLE session
 * @retval parameter
 *   Contains the ID of the parameter retrieved by this fetch
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_dammelo)(
		INTEGER(handle),
		CHARACTER(parameter)
		TRAIL(parameter))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)

	try {
		const char* res = STATE.api->dammelo();
		if (!res)
			cnfExprt("", parameter, parameter_length);
		else
			cnfExprt(res, parameter, parameter_length);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Insert a new item in the database.
 *
 * This function will fail if the database is open in data readonly mode, and
 * it will refuse to overwrite existing values if the database is open in data
 * add mode.
 *
 * If the database is open in pseudoana reuse mode, the pseudoana values
 * provided on input will be used to create a pseudoana record if it is
 * missing, but will be ignored if it is already present.  If it is open in
 * pseudoana rewrite mode instead, the pseudoana values on input will be used
 * to replace all the existing pseudoana values.
 *
 * @param handle
 *   Handle to a DBALLE session
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_prendilo)(
		INTEGER(handle))
{
	GENPTR_INTEGER(handle)
	try {
		STATE.api->prendilo();
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Remove all selected items from the database.
 *
 * This function will fail unless the database is open in data rewrite mode.
 *
 * @param handle
 *   Handle to a DBALLE session
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_dimenticami)(
		INTEGER(handle))
{
	GENPTR_INTEGER(handle)
	try {
		STATE.api->dimenticami();
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**@name QC functions
 * @anchor qc
 * Functions used to manipulate QC data.
 *
 * All these functions require some context data about the variable, which is
 * automatically available when the variable just came as the result of an
 * idba_dammelo() or has just been inserted with an idba_prendilo().
 *@{
 */

F77_INTEGER_FUNCTION(idba_voglioancora)(INTEGER(handle), INTEGER(count))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(count)
	try {
		*count = STATE.api->voglioancora();
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Retrieve QC informations from the last variable returned by idba_dammelo().
 *
 * @param handle
 *   Handle to a DBALLE session
 * @retval parameter
 *   Contains the ID of the parameter retrieved by this fetch
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_ancora)(
		INTEGER(handle),
		CHARACTER(parameter)
		TRAIL(parameter))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(parameter)

	try {
		const char* res = STATE.api->ancora();
		if (!res)
			cnfExprt("", parameter, parameter_length);
		else
			cnfExprt(res, parameter, parameter_length);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Insert new QC informations for a variable of the current record.
 *
 * QC informations inserted are all those set by the functions idba_seti(),
 * idba_setc(), idba_setr(), idba_setd(), using an asterisk in front of the
 * variable name.
 *
 * Contrarily to idba_prendilo(), this function resets all the QC informations
 * (but only the QC informations) previously set in input, so the values to be
 * inserted need to be explicitly set every time.
 *
 * This function will fail if the database is open in QC readonly mode, and it
 * will refuse to overwrite existing values if the database is open in QC add
 * mode.
 *
 * The variable referred by the QC informations can be specified in three ways:
 * 
 * \li by variable code, using ::idba_setc(handle, "*var", "Bxxyyy")
 * \li by variable id, using ::idba_seti(handle, "*data_id", id)
 * \li unspecified, will use the last variable returned by ::idba_dammelo
 *
 * @param handle
 *   Handle to a DBALLE session
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_critica)(
		INTEGER(handle))
{
	GENPTR_INTEGER(handle)

	try {
		STATE.api->critica();
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

/**
 * Remove QC informations for a variable of the current record.
 *
 * The QC informations to be removed are set with:
 * \code
 *   idba_setc(handle, "*varlist", "*B33021,*B33003");
 * \endcode
 *
 * The variable referred by the QC informations can be specified in three ways:
 * 
 * \li by variable code, using ::idba_setc(handle, "*var", "Bxxyyy")
 * \li by variable id, using ::idba_seti(handle, "*data_id", id)
 * \li unspecified, will use the last variable returned by ::idba_dammelo
 *
 * @param handle
 *   Handle to a DBALLE session
 * @return
 *   The error indicator for the function
 */
F77_INTEGER_FUNCTION(idba_scusa)(INTEGER(handle))
{
	GENPTR_INTEGER(handle)

	try {
		STATE.api->scusa();
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

F77_INTEGER_FUNCTION(idba_spiegal)(
		INTEGER(handle),
		INTEGER(ltype1),
		INTEGER(l1),
		INTEGER(ltype2),
		INTEGER(l2),
		CHARACTER(result)
		TRAIL(result))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(ltype1)
	GENPTR_INTEGER(l1)
	GENPTR_INTEGER(ltype2)
	GENPTR_INTEGER(l2)
	GENPTR_CHARACTER(result)

	try {
		char* res = STATE.api->spiegal(*ltype1, *l1, *ltype2, *l2);
		cnfExprt(res, result, result_length);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

F77_INTEGER_FUNCTION(idba_spiegat)(
		INTEGER(handle),
		INTEGER(ptype),
		INTEGER(p1),
		INTEGER(p2),
		CHARACTER(result)
		TRAIL(result))
{
	GENPTR_INTEGER(handle)
	GENPTR_INTEGER(ptype)
	GENPTR_INTEGER(p1)
	GENPTR_INTEGER(p2)
	GENPTR_CHARACTER(result)

	try {
		char* res = STATE.api->spiegat(*ptype, *p1, *p2);
		cnfExprt(res, result, result_length);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

F77_INTEGER_FUNCTION(idba_spiegab)(
		INTEGER(handle),
		CHARACTER(varcode),
		CHARACTER(value),
		CHARACTER(result)
		TRAIL(varcode)
		TRAIL(value)
		TRAIL(result))
{
	GENPTR_INTEGER(handle)
	GENPTR_CHARACTER(varcode)
	GENPTR_CHARACTER(value)
	GENPTR_CHARACTER(result)
	char s_varcode[10];
	char s_value[300];
	cnfImpn(varcode, varcode_length, 9, s_varcode); s_varcode[9] = 0;
	cnfImpn(value, value_length, 299, s_value); s_value[299] = 0;

	try {
		char* res = STATE.api->spiegab(s_varcode, s_value);
		cnfExprt(res, result, result_length);
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

F77_INTEGER_FUNCTION(idba_test_input_to_output)(
		INTEGER(handle))
{
	GENPTR_INTEGER(handle)

	try {
		STATE.api->test_input_to_output();
		return dba_error_ok();
	} catch (APIException& e) {
		return e.err;
	}
}

}

/*@}*/

/* vim:set ts=4 sw=4: */
