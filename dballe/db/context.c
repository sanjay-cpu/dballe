/*
 * DB-ALLe - Archive for punctual meteorological data
 *
 * Copyright (C) 2005,2006  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#define _GNU_SOURCE
#include <dballe/db/context.h>
#include <dballe/db/internals.h>
#include <dballe/core/verbose.h>

#include <sql.h>
#include <sqlext.h>

#include <config.h>

#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>

#include <assert.h>

/*
 * Define to true to enable the use of transactions during writes
 */
/*
*/

dba_err dba_db_context_create(dba_db db, dba_db_context* ins)
{
	const char* select_query =
		"SELECT id FROM context WHERE id_ana=? AND id_report=? AND datetime=?"
		" AND ltype=? AND l1=? AND l2=?"
		" AND ptype=? AND p1=? AND p2=?";
	const char* select_data_query =
		"SELECT id_ana, id_report, datetime, ltype, l1, l2, ptype, p1, p2 FROM context WHERE id=?";
	const char* insert_query =
		"INSERT INTO context VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	const char* remove_query =
		"DELETE FROM context WHERE id=?";
	dba_err err = DBA_OK;
	dba_db_context res = NULL;
	int r;

	switch (db->server_type)
	{
		case ORACLE:
			insert_query = "INSERT INTO context VALUES (seq_context.NextVal, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			break;
		case POSTGRES:
			insert_query = "INSERT INTO context VALUES (nextval('seq_context'), ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			break;
	}

	if ((res = (dba_db_context)malloc(sizeof(struct _dba_db_context))) == NULL)
		return dba_error_alloc("creating a new dba_db_context");
	res->db = db;
	res->sstm = NULL;
	res->sdstm = NULL;
	res->istm = NULL;
	res->dstm = NULL;
	res->date.fraction = 0;

	/* Create the statement for select fixed */
	DBA_RUN_OR_GOTO(cleanup, dba_db_statement_create(db, &(res->sstm)));
	SQLBindParameter(res->sstm, 1, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->id_ana), 0, 0);
	SQLBindParameter(res->sstm, 2, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->id_report), 0, 0);
	if (db->server_type == POSTGRES || db->server_type == SQLITE)
		SQLBindParameter(res->sstm, 3, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TIMESTAMP, 0, 0, &(res->date), 0, 0);
	else
		SQLBindParameter(res->sstm, 3, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_DATETIME, 0, 0, &(res->date), 0, 0);
	SQLBindParameter(res->sstm, 4, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->ltype), 0, 0);
	SQLBindParameter(res->sstm, 5, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->l1), 0, 0);
	SQLBindParameter(res->sstm, 6, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->l2), 0, 0);
	SQLBindParameter(res->sstm, 7, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->pind), 0, 0);
	SQLBindParameter(res->sstm, 8, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->p1), 0, 0);
	SQLBindParameter(res->sstm, 9, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->p2), 0, 0);

	SQLBindCol(res->sstm, 1, DBALLE_SQL_C_SINT, &(res->id), sizeof(res->id), 0);
	r = SQLPrepare(res->sstm, (unsigned char*)select_query, SQL_NTS);
	if ((r != SQL_SUCCESS) && (r != SQL_SUCCESS_WITH_INFO))
	{
		err = dba_db_error_odbc(SQL_HANDLE_STMT, res->sstm, "compiling query to look for context IDs");
		goto cleanup;
	}

	/* Create the statement for select data */
	DBA_RUN_OR_GOTO(cleanup, dba_db_statement_create(db, &(res->sdstm)));
	SQLBindParameter(res->sdstm, 1, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->id), 0, 0);
	SQLBindCol(res->sdstm, 1, DBALLE_SQL_C_SINT, &(res->id_ana),	  sizeof(res->id_ana),	0);
	SQLBindCol(res->sdstm, 2, DBALLE_SQL_C_SINT, &(res->id_report), sizeof(res->id_report),	0);
	SQLBindCol(res->sdstm, 3, SQL_C_TYPE_TIMESTAMP,  &(res->date),	  sizeof(res->date), 0);
	SQLBindCol(res->sdstm, 4, DBALLE_SQL_C_SINT, &(res->ltype),	  sizeof(res->ltype),	    0);
	SQLBindCol(res->sdstm, 5, DBALLE_SQL_C_SINT, &(res->l1),		  sizeof(res->l1),		    0);
	SQLBindCol(res->sdstm, 6, DBALLE_SQL_C_SINT, &(res->l2),		  sizeof(res->l2),		    0);
	SQLBindCol(res->sdstm, 7, DBALLE_SQL_C_SINT, &(res->pind),	  sizeof(res->pind),	    0);
	SQLBindCol(res->sdstm, 8, DBALLE_SQL_C_SINT, &(res->p1),		  sizeof(res->p1),		    0);
	SQLBindCol(res->sdstm, 9, DBALLE_SQL_C_SINT, &(res->p2),		  sizeof(res->p2),		    0);
	r = SQLPrepare(res->sdstm, (unsigned char*)select_data_query, SQL_NTS);
	if ((r != SQL_SUCCESS) && (r != SQL_SUCCESS_WITH_INFO))
	{
		err = dba_db_error_odbc(SQL_HANDLE_STMT, res->sdstm, "compiling query to retrieve context data");
		goto cleanup;
	}

	/* Create the statement for insert */
	DBA_RUN_OR_GOTO(cleanup, dba_db_statement_create(db, &(res->istm)));
	SQLBindParameter(res->istm, 1, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->id_ana), 0, 0);
	SQLBindParameter(res->istm, 2, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->id_report), 0, 0);
	if (db->server_type == POSTGRES || db->server_type == SQLITE)
		SQLBindParameter(res->istm, 3, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_TIMESTAMP, 0, 0, &(res->date), 0, 0);
	else
		SQLBindParameter(res->istm, 3, SQL_PARAM_INPUT, SQL_C_TYPE_TIMESTAMP, SQL_DATETIME, 0, 0, &(res->date), 0, 0);
	SQLBindParameter(res->istm, 4, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->ltype), 0, 0);
	SQLBindParameter(res->istm, 5, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->l1), 0, 0);
	SQLBindParameter(res->istm, 6, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->l2), 0, 0);
	SQLBindParameter(res->istm, 7, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->pind), 0, 0);
	SQLBindParameter(res->istm, 8, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->p1), 0, 0);
	SQLBindParameter(res->istm, 9, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->p2), 0, 0);
	r = SQLPrepare(res->istm, (unsigned char*)insert_query, SQL_NTS);
	if ((r != SQL_SUCCESS) && (r != SQL_SUCCESS_WITH_INFO))
	{
		err = dba_db_error_odbc(SQL_HANDLE_STMT, res->istm, "compiling query to insert into 'context'");
		goto cleanup;
	}

	/* Create the statement for remove */
	DBA_RUN_OR_GOTO(cleanup, dba_db_statement_create(db, &(res->dstm)));
	SQLBindParameter(res->dstm, 1, SQL_PARAM_INPUT, DBALLE_SQL_C_SINT, SQL_INTEGER, 0, 0, &(res->id), 0, 0);
	r = SQLPrepare(res->dstm, (unsigned char*)remove_query, SQL_NTS);
	if ((r != SQL_SUCCESS) && (r != SQL_SUCCESS_WITH_INFO))
	{
		err = dba_db_error_odbc(SQL_HANDLE_STMT, res->dstm, "compiling query to remove from 'context'");
		goto cleanup;
	}

	*ins = res;
	res = NULL;
	
cleanup:
	if (res != NULL)
		dba_db_context_delete(res);
	return err == DBA_OK ? dba_error_ok() : err;
};

void dba_db_context_delete(dba_db_context ins)
{
	if (ins->sstm != NULL)
		SQLFreeHandle(SQL_HANDLE_STMT, ins->sstm);
	if (ins->sdstm != NULL)
		SQLFreeHandle(SQL_HANDLE_STMT, ins->sdstm);
	if (ins->istm != NULL)
		SQLFreeHandle(SQL_HANDLE_STMT, ins->istm);
	if (ins->dstm != NULL)
		SQLFreeHandle(SQL_HANDLE_STMT, ins->dstm);
	free(ins);
}

dba_err dba_db_context_get_id(dba_db_context ins, int *id)
{
	SQLHSTMT stm = ins->sstm;

	int res = SQLExecute(stm);
	if ((res != SQL_SUCCESS) && (res != SQL_SUCCESS_WITH_INFO))
		return dba_db_error_odbc(SQL_HANDLE_STMT, stm, "looking for context IDs");

	/* Get the result */
	if (SQLFetch(stm) != SQL_NO_DATA)
		*id = ins->id;
	else
		*id = -1;

	res = SQLCloseCursor(stm);
	if ((res != SQL_SUCCESS) && (res != SQL_SUCCESS_WITH_INFO))
		return dba_db_error_odbc(SQL_HANDLE_STMT, stm, "closing dba_db_context_get_id cursor");

	return dba_error_ok();
}

dba_err dba_db_context_get_data(dba_db_context ins, int id)
{
	int res;
	ins->id = id;
	res = SQLExecute(ins->sdstm);
	if ((res != SQL_SUCCESS) && (res != SQL_SUCCESS_WITH_INFO))
		return dba_db_error_odbc(SQL_HANDLE_STMT, ins->sdstm, "looking for context information");
	/* Get the result */
	if (SQLFetch(ins->sdstm) == SQL_NO_DATA)
		return dba_error_notfound("looking for information for context_id %d", id);
	res = SQLCloseCursor(ins->sdstm);
	if ((res != SQL_SUCCESS) && (res != SQL_SUCCESS_WITH_INFO))
		return dba_db_error_odbc(SQL_HANDLE_STMT, ins->sdstm, "closing dba_db_context_get_data cursor");
	return dba_error_ok();
}

dba_err dba_db_context_obtain_ana(dba_db_context ins, int *id)
{
	/* Fill up the query parameters with the data for the anagraphical context */
	if (ins->id_report == -1)
		ins->id_report = 254;
	ins->date.year = 1000;
	ins->date.month = 1;
	ins->date.day = 1;
	ins->date.hour = ins->date.minute = ins->date.second = 0;
	ins->ltype = 257;
	ins->l1 = ins->l2 = 0;
	ins->pind = ins->p1 = ins->p2 = 0;

	/* See if the context entry already exists */
	DBA_RUN_OR_RETURN(dba_db_context_get_id(ins, id));

	/* If it doesn't exist yet, we create it */
	if (*id == -1)
		DBA_RUN_OR_RETURN(dba_db_context_insert(ins, id));

	return dba_error_ok();
}

dba_err dba_db_context_insert(dba_db_context ins, int *id)
{
	int res = SQLExecute(ins->istm);
	if ((res != SQL_SUCCESS) && (res != SQL_SUCCESS_WITH_INFO))
		return dba_db_error_odbc(SQL_HANDLE_STMT, ins->istm, "inserting new data into context");

	switch (ins->db->server_type)
	{
		case ORACLE:
		case POSTGRES:
			DBA_RUN_OR_RETURN(dba_db_seq_read(ins->db->seq_context));
			*id = ins->db->seq_context->out;
			return dba_error_ok();
		default:
			return dba_db_last_insert_id(ins->db, id);
	}
}

dba_err dba_db_context_remove(dba_db_context ins)
{
	int res = SQLExecute(ins->dstm);
	if ((res != SQL_SUCCESS) && (res != SQL_SUCCESS_WITH_INFO))
		return dba_db_error_odbc(SQL_HANDLE_STMT, ins->dstm, "removing a context record");

	return dba_error_ok();
}

/* vim:set ts=4 sw=4: */
