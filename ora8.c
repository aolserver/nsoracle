/* $Id$ */

/* An Oracle 8 internal driver for AOLServer
   Copyright (C) 1997 Cotton Seed

   documented 1998 by cottons@concmp.com, philg@mit.edu, shivers@lcs.mit.edu
   extended 1999 by markd@arsdigita.com
   extended 2000 by markd@arsdigita.com, curtisg@arsdigita.com, jsalz@mit.edu, 
     jsc@arsdigita.com, mayoff@arsdigita.com

   The "how-to write an AOLserver driver" docs are at
   http://www.aolserver.com/server/docs/2.3/html/dr-app.htm

   The Oracle OCI docs are in 
   Programmer's Guide to the Oracle Call Interface
   http://oradoc.photo.net/ora8doc/DOC/server803/A54656_01/toc.htm

   The documentation for this driver (including a couple of special
   Tcl API calls) is at 
         http://www.arsdigita.com/free-tools/oracle-driver.html

   Config paramters in [ns/db/driver/drivername]:

    Debug
	boolean (Defaults to off)
        Enable the "log" call so that lots of stuff gets
        sent to the server.log.

    MaxStringLogLength
	integer (defaults to 1024).  -1 implies unlimited
        how much character data to log before just 
        saying [too long]

    CharExpansion
	integer defaulting to 1.
	factor by which byte representation of character
	strings can expand when fetched from the database.
	Should only be necessary to set this if your Oracle
	is not using UTF-8, in which case a value of 2 should
	work for any ISO-8859 character set.
   
   ns_ora clob_dml SQL is logged when verbose=on in the pool's configuration
   section.

   To make a "safe" driver (say for servers running with DBA
   priviliges) that only allows SELECT statements, define
   FOR_CASSANDRACLE when compiling this

   Known Bugs:

     The cleanup after errors after stream_write_lob is very heavy-handed,
     and should be fixed to use a better cleanup after interrupting a multipart
     LOB get.

     LONGs greater than 1024 bytes aren't supported since we don't do
     the piecewise fetch stuff.  Oracle's deprecating LONGs anyway, so
     we don't want to burn the time to Do It Right.  We still want to keep
     them around since the Data Dictionary returns some stuff as longs.

     leaves behind zombie processes on HP-UX 10.xx after conn is
     closed, due to lossage with AOLServer and the HP-UX signal
     handling

     it may be the case that the Oracle libraries are able to lock the 
     whole server for moments and keep other AOLserver threads (even those
     that are just serving static files and don't even have Tcl interpreters)
     from serving; this driver never explicitly takes a lock (see
     http://db.photo.net/dating/ for an example of Oracle + JPEG service
     conflicting).

*/
/* Oracle 8 Call Interface */
#include <oci.h>

/* be sure to bump the version number if changes are made */
#include "version.h"

static char *ora_driver_version = "ArsDigita Oracle Driver version " ORA8_DRIVER_VERSION;
static char *ora_driver_name = "Oracle8";

/* other tweakable parameters */

/* how big of a buffer to use for printing error messages
  (this is no longer on the stack)
 */
#define STACK_BUFFER_SIZE 20000

/* how big a buffer to use for the returned argument
   of exec_plsql
 */
#define EXEC_PLSQL_BUFFER_SIZE 4096

/* how long the error code string can be in Ns_DbSetException
 */
#define EXCEPTION_CODE_SIZE 5

#include <stdlib.h>

/* we bring in stdio so that we can snprintf nicely formatted error messages */

#include <stdio.h>

/* we bring in string.h because the OCI uses null-terminated strings 
   and also AOLserver passes us pointers to null-terminated strings  
*/

#include <string.h>

/* we use tolower so we bring in the ctype library */

#include <ctype.h>

/* we do a stat, so we need these */

#include <sys/types.h>
#include <sys/stat.h>





/* the OCI lumps a bunch of different things into two C types, sword 
   (signed word, i.e., an int) and dvoid (pointer to void; something we
   only give back to them anyway).  We give them semantically meaningful
   names 
*/

typedef sword oci_status_t;
typedef dvoid oci_handle_t;
typedef dvoid oci_attribute_t;
typedef dvoid oci_param_t;
typedef dvoid oci_descriptor_t;



/* bring in the AOLserver interface libraries from /home/nsadmin/include/ */

#include <ns.h>

#if !defined(NS_AOLSERVER_3_PLUS)
#include <nsdb.h>
#include <nstcl.h>
#define NS_EXPORT
#endif



/* when we start a query, we allocate one fetch buffer for each 
   column that we're querying, i.e., if you say "select foo,bar from yow"
   then two fetch buffers will be allocated 
*/

struct fetch_buffer
{
  struct ora_connection *connection;
  
  /* Oracle will tell us what type this column is */
  OCITypeCode type;

  /* will be NULL unless this column happens to be one of the LOB types */
  OCILobLocator *lob;

  /* generally used for INSERT and UPDATE; seldom used */
  OCIBind *bind;

  /* generally used for SELECT; seldom used except for getting LOBs */
  OCIDefine *def;

  /* how many bytes Oracle thinks we need to allocate */
  ub2 size;

  /* how many bytes we allocated */
  unsigned buf_size;

  /* the stuff above does not change as the rows are fetched */
  /* here's where the actual value from a particular row is kept */
  char *buf;

  /* support for array DML: the array of values for this bind variable. */
  int array_count;
  char **array_values;

  /* 2-byte signed integer indicating null-ness; if null, value will be -1 */
  sb2 is_null;

  /* how many bytes are in the buffer above, 0 would mean empty string */
  ub2 fetch_length;

  /* these are only used for LONGs; the length of one piece */
  ub4 piecewise_fetch_length;

  /* in order to implement the clob_dml API call, we need 1 LOB 
     for every row/column intersection inserted.  I.e., if we do an 
     insert that results in 4 rows going into the db, with 3 CLOB
     columns then we need 12 LOBs.  This struct is for one column only
     so we just need one array of lobs. */
  OCILobLocator **lobs;

  /* this tells us how many lobs we have above (i.e., only for clob_dml) */
  ub4 n_rows;

  /* Whether we determined that this column is a LOB during processing. */
  int is_lob;
};



typedef struct fetch_buffer fetch_buffer_t;



/* this is our own data structure for keeping track 
   of an Oracle connection 
*/

struct ora_connection
{
  /* which AOLserver handle we're associated with */
  Ns_DbHandle *dbh;
  
  /* oracle handles; last the lifetime of open Oracle connection */
  OCIEnv     *env;
  OCIError   *err;
  OCIServer  *srv;
  OCISvcCtx  *svc;
  OCISession *auth;
  
  /* last the lifetime of query; this is the statement we're executing */
  OCIStmt *stmt;
  
  /* the default is autocommit; we keep track of when a connection 
     has been kicked into transaction mode.  This was to make Oracle
     look more like ANSI databases such as Illustra */
  enum
  {
    autocommit,
    transaction
  } mode;
  
  /* fetch buffers; these change per query */
  sb4 n_columns;
  fetch_buffer_t *fetch_buffers;
};


typedef struct ora_connection ora_connection_t;


/* A linked list to use when parsing SQL. */

typedef struct _string_list_elt {
  char *string;
  struct _string_list_elt *next;
} string_list_elt_t;



static char *ora_name (Ns_DbHandle *dummy);
static char *ora_db_type (Ns_DbHandle *dummy);
static int ora_open_db (Ns_DbHandle *dbh);
static int ora_close_db (Ns_DbHandle *dbh);
static int ora_dml (Ns_DbHandle *dbh, char *sql);
static Ns_Set *ora_select (Ns_DbHandle *dbh, char *sql);
static int ora_exec (Ns_DbHandle *dbh, char *sql);
static Ns_Set *ora_bindrow (Ns_DbHandle *dbh);
static int ora_get_row (Ns_DbHandle *dbh, Ns_Set *row);
static int ora_flush (Ns_DbHandle *dbh);
static int ora_server_init (char *hserver, char *hmodule, char *hdriver);
static int ora_reset_handle (Ns_DbHandle *dbh);
static sb4 ora_append_buf_to_dstring (dvoid *ctxp, CONST dvoid *bufp, ub4 len, ub1 piece);

static Ns_DbTableInfo *ora_get_table_info (Ns_DbHandle *dbh, char *table);
static char *ora_table_list (Ns_DString *pds, Ns_DbHandle *dbh, int system_tables_p);
NS_EXPORT int Ns_DbDriverInit (char *hdriver, char *config_path);

/* Tcl Commands */
static int ora_tcl_command (ClientData dummy, Tcl_Interp *interp, 
			    int argc, char *argv[]);

static int lob_dml_bind_cmd(Tcl_Interp *interp, int argc, char *argv[],
    Ns_DbHandle *dbh, ora_connection_t *connection);

static int lob_dml_cmd(Tcl_Interp *interp, int argc, char *argv[],
    Ns_DbHandle *dbh, ora_connection_t *connection);

#if defined(NS_AOLSERVER_3_PLUS)
static int ora_column_command (ClientData dummy, Tcl_Interp *interp, 
			       int argc, char *argv[]);
static int ora_table_command (ClientData dummy, Tcl_Interp *interp, 
			       int argc, char *argv[]);

static Ns_DbTableInfo *Ns_DbNewTableInfo (char *table);
static void Ns_DbFreeTableInfo (Ns_DbTableInfo *tinfo);
static void Ns_DbAddColumnInfo (Ns_DbTableInfo *tinfo, Ns_Set *column_info);
static int Ns_DbColumnIndex (Ns_DbTableInfo *tinfo, char *name);

#else
static char *ora_best_row_id (Ns_DString *pds, Ns_DbHandle *dbh, char *table);

#endif



/* lexpos is used for logging errors, macro that expands into something like 
   ora8.c:345:Ns_DbDriverInit: entry (hdriver 149f60, config_path ns/db/driver/ora8)
*/

#ifdef __GNUC__
#define lexpos() \
  __FILE__, __LINE__, __FUNCTION__
#else
#define lexpos() \
	__FILE__, __LINE__, "<unknown>"
#endif


/* result codes from stream_write_lob
 */
enum {
    STREAM_WRITE_LOB_OK = 0,
    STREAM_WRITE_LOB_ERROR,
    STREAM_WRITE_LOB_PIPE	/* user click stop, but we need to do some cleanup */
};



/* other random local prototypes */

static void log      (char *file, int line, char *fn, char *fmt, ...);
static void error    (char *file, int line, char *fn, char *fmt, ...);
static int oci_error_p (char *file, int line, char *fn,
                        Ns_DbHandle *dbh, char *ocifn, char *query,
                        oci_status_t oci_status);
static void downcase (char *s);
static char *nilp (char *s);
static int flush_handle (Ns_DbHandle *dbh);
static int stream_write_lob (Tcl_Interp *interp, Ns_DbHandle *dbh, int rowind,
			     OCILobLocator *lobl, char *path, int to_conn_p,
			     OCISvcCtx *svchp, OCIError *errhp);
static int stream_read_lob (Tcl_Interp *interp, Ns_DbHandle *dbh, int rowind, 
			    OCILobLocator *lobl, 
			    char *path, ora_connection_t *connection);



/* config parameter control */

static int debug_p = NS_FALSE;  /* should we print the verbose log messages? */
static int max_string_log_length = 0;
static int char_expansion;

static int lob_buffer_size = 16384;

/* prefetch parameters, if zero leave defaults*/
static ub4 prefetch_rows = 0;
static ub4 prefetch_memory = 0;

/* default values for the configuration parameters */

#define DEFAULT_DEBUG  			NS_FALSE
#define DEFAULT_MAX_STRING_LOG_LENGTH	1024
#define DEFAULT_CHAR_EXPANSION          1



/* we need to tell AOLserver which of our functions correspond to
   their expected db interface 
*/

/* `NULL' terminated array */
static Ns_DbProc ora_procs[] =
{
  { DbFn_Name,           (void *) ora_name },
  { DbFn_DbType,         (void *) ora_db_type },
  { DbFn_OpenDb,         (void *) ora_open_db },
  { DbFn_CloseDb,        (void *) ora_close_db },
  { DbFn_DML,            (void *) ora_dml },
  { DbFn_Select,         (void *) ora_select },
  { DbFn_Exec,           (void *) ora_exec },
  { DbFn_BindRow,        (void *) ora_bindrow },
  { DbFn_GetRow,         (void *) ora_get_row },
  { DbFn_Flush,          (void *) ora_flush },
  { DbFn_Cancel,         (void *) ora_flush },
  { DbFn_ServerInit,     (void *) ora_server_init },
  { DbFn_ResetHandle,    (void *) ora_reset_handle },

/* these aren't supported in AOLserver 3 */
#if !defined(NS_AOLSERVER_3_PLUS)
  { DbFn_GetTableInfo,   (void *) ora_get_table_info },
  { DbFn_TableList,      (void *) ora_table_list },
  { DbFn_BestRowId,      (void *) ora_best_row_id },
#endif
  
  { 0, NULL, }
};


#ifdef WIN32
#define EXTRA_OPEN_FLAGS O_BINARY
#else
#define EXTRA_OPEN_FLAGS 0
#endif


#ifdef WIN32
/*
 * This is a GNU extension that isn't present on Windows.
 */
static int
snprintf(char *buf, int len, const char *fmt, ...)
{
	va_list ap;
	int cc;

	va_start(ap, fmt);
	cc = vsprintf(buf, fmt, ap);
	va_end(ap);
	return cc;
}
#endif


/* 
 * utility functions for dealing with string lists 
 */

static string_list_elt_t *
string_list_elt_new(char *string)
{
  string_list_elt_t *elt = 
    (string_list_elt_t *) Ns_Malloc(sizeof(string_list_elt_t));
  elt->string = string;
  elt->next = 0;

  return elt;

} /* string_list_elt_new */



static int 
string_list_len (string_list_elt_t *head)
{
  int i = 0;

  while (head != NULL) {
    i++;
    head = head->next;
  }

  return i; 

} /* string_list_len */



/* Free the whole list and the strings in it. */

static void 
string_list_free_list (string_list_elt_t *head)
{
  string_list_elt_t *elt;

  while (head) {
    Ns_Free(head->string);
    elt = head->next;
    Ns_Free(head);
    head = elt;
  }

} /* string_list_free_list */



/* Parse a SQL string and return a list of all
 * the bind variables found in it.
 */

static string_list_elt_t *
parse_bind_variables(char *input)
{
  char *p, lastchar;
  enum { base, instr, bind } state;
  char bindbuf[1024], *bp=bindbuf;
  string_list_elt_t *elt, *head=0, *tail=0;
  int current_string_length = 0;

  for (p = input, state=base, lastchar='\0'; *p != '\0'; lastchar = *p, p++) {

    switch (state) {
    case base:
      if (*p == '\'') {
	state = instr;
        current_string_length = 0;
      } else if (*p == ':') {
	bp = bindbuf;
	state = bind;
      }
      break;

    case instr:
      if (*p == '\'' && (lastchar != '\'' || current_string_length == 0)) {
	state = base;
      }
      current_string_length++;
      break;

    case bind:
      if (*p == '=') {
        state = base;
        bp = bindbuf;
      } else if (!(*p == '_' || *p == '$' || *p == '#' || isalnum((int)*p))) {
	*bp = '\0';
	elt = string_list_elt_new(Ns_StrDup(bindbuf));
	if (tail == 0) {
	  head = tail = elt;
	} else {
	  tail->next = elt;
	  tail = elt;
	}
	state = base;
	p--;
      } else {
	*bp++ = *p;
      }
      break;
    }
  }

  if (state == bind) {
    *bp = '\0';
    elt = string_list_elt_new(Ns_StrDup(bindbuf));
    if (tail == 0) {
      head = tail = elt;
    } else {
      tail->next = elt;
      tail = elt;
    }
  }

  return head;

} /* parse_bind_variables */



/* we call this after every OCI call, i.e., a couple of times during
   each fetch of a row 
*/

static int
oci_error_p (char *file, int line, char *fn,
	     Ns_DbHandle *dbh, char *ocifn, char *query,
	     oci_status_t oci_status)
{
  /* for info we get from Oracle */
  char *msgbuf;
  /* what we will actually print out in the log */
  char *buf;
  /* exception code for Ns_DbSetException */
  char exceptbuf[EXCEPTION_CODE_SIZE + 1];
  ora_connection_t *connection = 0;
  ub2 offset = 0;
  sb4 errorcode = 0;
  
  if (dbh)
    connection = dbh->connection;
  
  /* success */
  if (oci_status == OCI_SUCCESS)
    return 0;

  /*
     until we get the logging situation worked out, return
     OCI_SUCCESS_WITH_INFO as a pure success.
  */
  if (oci_status == OCI_SUCCESS_WITH_INFO)
    return 0;

  /* if the query is long, nilp will return "[too long]"; 
   * if null (we're not doing 
   *    a query yet, e.g., could be opening db), then "[nil]"  
   */
  query = nilp (query);

  msgbuf = (char *)Ns_Malloc(STACK_BUFFER_SIZE * sizeof(char));
  buf = (char *)Ns_Malloc(STACK_BUFFER_SIZE * sizeof(char));
  *msgbuf = 0;
  
  switch (oci_status)
    {
    case OCI_NEED_DATA:
      snprintf (msgbuf, STACK_BUFFER_SIZE, "Error - OCI_NEED_DATA");
      break;
    case OCI_NO_DATA:
      snprintf (msgbuf, STACK_BUFFER_SIZE, "Error - OCI_NO_DATA");
      break;
    case OCI_ERROR:
      if (connection == 0)
	snprintf (msgbuf, STACK_BUFFER_SIZE, "NULL connection");
      else
	{
	  char errorbuf[1024];
	  oci_status_t oci_status1;
	  
	  oci_status1 = OCIErrorGet (connection->err,
				     1,
				     NULL,
				     &errorcode,
				     errorbuf,
				     sizeof errorbuf,
				     OCI_HTYPE_ERROR);
	  if (oci_status1)
	    snprintf (msgbuf, STACK_BUFFER_SIZE, "`OCIErrorGet ()' error");
	  else
	    snprintf (msgbuf, STACK_BUFFER_SIZE, "%s", errorbuf);

	  oci_status1 = OCIAttrGet (connection->stmt,
				     OCI_HTYPE_STMT,
				     &offset,
				     NULL,
				     OCI_ATTR_PARSE_ERROR_OFFSET,
  				     connection->err);
	  
	  if (errorcode == 1041 || errorcode == 3113 || errorcode == 12571) 
	    {
	      /* 3113 is 'end-of-file on communications channel', which
	       *      happens if the oracle process dies
               * 12571 is TNS:packet writer failure, which also happens if
	       *      the oracle process dies
	       * 1041 is the dreaded "hostdef extension doesn't exist error,
	       *      which means the db handle is screwed and can't be used
	       *      for anything else.
	       * In either case, close and re-open the handle to clear the
	       * error condition
	       */
	      flush_handle (dbh);
	      ora_close_db (dbh);
	      ora_open_db (dbh);
	    }
	  if (errorcode == 20 || errorcode == 1034) 
	    {
	      /* ora-00020 means 'maximum number of processes exceeded.
	       * ora-01034 means 'oracle not available'.
	       *           we want to make sure the oracleSID process
	       *           goes away so we don't make the problem worse
	       */
	      ora_close_db (dbh);
	    }
	}
      break;
    case OCI_INVALID_HANDLE:
      snprintf (msgbuf, STACK_BUFFER_SIZE, "Error - OCI_INVALID_HANDLE");
      break;
    case OCI_STILL_EXECUTING:
      snprintf (msgbuf, STACK_BUFFER_SIZE, "Error - OCI_STILL_EXECUTING");
      break;
    case OCI_CONTINUE:
      snprintf (msgbuf, STACK_BUFFER_SIZE, "Error - OCI_CONTINUE");
      break;
    }
  
  if (((errorcode == 900) || (offset > 0)) && (strlen(query) >= offset))
    {
      /* ora-00900 is invalid sql statment
       *           it seems to be the msg most likely to be a parse
       *           error that sets offset to 0
       */
      int len;
      len = snprintf(buf, STACK_BUFFER_SIZE, 
		     "%s:%d:%s: error in `%s ()': %s\nSQL: ",
		     file, line, fn, ocifn, msgbuf);
      if (offset > 0)
	len += snprintf(buf + len, STACK_BUFFER_SIZE - len, "%.*s", offset - 1, query);
      
      snprintf(buf + len, STACK_BUFFER_SIZE - len, " !>>>!%s",query + offset);
    }
  else
    {
      snprintf (buf, STACK_BUFFER_SIZE, 
		"%s:%d:%s: error in `%s ()': %s\nSQL: %s", 
		file, line, fn, ocifn, msgbuf, query);
    }

  Ns_Log (Error, "%s", buf);

  /* we need to call this so that AOLserver will print out the relevant
     error on pages served to browsers where ClientDebug is set */
  snprintf (exceptbuf, EXCEPTION_CODE_SIZE, "%d", (int)errorcode);
  Ns_DbSetException (dbh, exceptbuf, buf);
  
  /* error */
  Ns_Free(msgbuf);
  Ns_Free(buf);

  return 1;

} /* oci_error_p */



/* tcl_error_p is only used for ns_ora and potentialy other new Tcl commands
   does not log the error and does not Ns_DbSetException but instead just
   tells the Tcl interpreter about it
 */

static int
tcl_error_p (char *file, int line, char *fn,
	     Tcl_Interp *interp,
	     Ns_DbHandle *dbh, char *ocifn, char *query,
	     oci_status_t oci_status)
{
  char *msgbuf;
  char    *buf;
  ora_connection_t *connection = 0;
  ub2 offset = 0;
  sb4 errorcode = 0;
  
  if (dbh)
    connection = dbh->connection;
  
  /* success */
  if (oci_status == OCI_SUCCESS)
    return 0;

  query = nilp (query);
  msgbuf = (char *)Ns_Malloc(STACK_BUFFER_SIZE * sizeof(char));
  buf = (char *)Ns_Malloc(STACK_BUFFER_SIZE * sizeof(char));
  *msgbuf = 0;
  
  switch (oci_status)
    {
    case OCI_SUCCESS_WITH_INFO:
      snprintf (msgbuf, STACK_BUFFER_SIZE, "Error - OCI_SUCCESS_WITH_INFO");
      break;
    case OCI_NEED_DATA:
      snprintf (msgbuf, STACK_BUFFER_SIZE, "Error - OCI_NEED_DATA");
      break;
    case OCI_NO_DATA:
      snprintf (msgbuf, STACK_BUFFER_SIZE, "Error - OCI_NO_DATA");
      break;
    case OCI_ERROR:
      if (connection == 0)
	snprintf (msgbuf, STACK_BUFFER_SIZE, "NULL connection");
      else
	{
	  char errorbuf[512];
	  oci_status_t oci_status1;
	  
	  oci_status1 = OCIErrorGet (connection->err,
				     1,
				     NULL,
				     &errorcode,
				     errorbuf,
				     sizeof errorbuf,
				     OCI_HTYPE_ERROR);
	  if (oci_status1)
	    snprintf (msgbuf, STACK_BUFFER_SIZE, "`OCIErrorGet ()' error");
	  else
	    snprintf (msgbuf, STACK_BUFFER_SIZE, "%s", errorbuf);

	  oci_status1 = OCIAttrGet (connection->stmt,
				     OCI_HTYPE_STMT,
				     &offset,
				     NULL,
				     OCI_ATTR_PARSE_ERROR_OFFSET,
  				     connection->err);
	  
	  if (errorcode == 1041 || errorcode == 3113 || errorcode == 12571) 
	    {
	      /* 3113 is 'end-of-file on communications channel', which
	       *      happens if the oracle process dies
               * 12571 is TNS:packet writer failure, which also happens if
	       *      the oracle process dies
	       * 1041 is the dreaded "hostdef extension doesn't exist error,
	       *      which means the db handle is screwed and can't be used
	       *      for anything else.
	       * In either case, close and re-open the handle to clear the
	       * error condition
	       */
	      flush_handle (dbh);
	      ora_close_db (dbh);
	      ora_open_db (dbh);
	    }
	}
      break;
    case OCI_INVALID_HANDLE:
      snprintf (msgbuf, STACK_BUFFER_SIZE, "Error - OCI_INVALID_HANDLE");
      break;
    case OCI_STILL_EXECUTING:
      snprintf (msgbuf, STACK_BUFFER_SIZE, "Error - OCI_STILL_EXECUTING");
      break;
    case OCI_CONTINUE:
      snprintf (msgbuf, STACK_BUFFER_SIZE, "Error - OCI_CONTINUE");
      break;
    }
  
  snprintf (buf, STACK_BUFFER_SIZE, "%s:%d:%s: error in `%s ()': %s\nSQL: %s",
           file, line, fn, ocifn, msgbuf, query);
  
  Ns_Log (Error, "SQL(): %s", buf);

  Tcl_AppendResult (interp, buf, NULL);

  /* error */
  Ns_Free(msgbuf);
  Ns_Free(buf);
  
  return 1;

} /* tcl_error_p */



/* for logging errors that come from C code rather than Oracle unhappiness */

static void
error (char *file, int line, char *fn, char *fmt, ...)
{
  char *buf1;
  char  *buf;
  va_list ap;
  
  buf1 = (char *)Ns_Malloc(STACK_BUFFER_SIZE * sizeof(char));
  buf = (char *)Ns_Malloc(STACK_BUFFER_SIZE * sizeof(char));

  va_start (ap, fmt);
  vsprintf (buf1, fmt, ap);
  va_end (ap);
  
  snprintf (buf, STACK_BUFFER_SIZE, "%s:%d:%s: %s", file, line, fn, buf1);
  
  Ns_Log (Error, "%s", buf);

  Ns_Free(buf1);
  Ns_Free(buf);
} /* error */



/* for optional logging of all kinds of random stuff, turn on 
   debug in the [ns/db/driver/drivername] section of your nsd.ini
*/

static void
log (char *file, int line, char *fn, char *fmt, ...)
{
  char *buf1;
  char  *buf;
  va_list ap;
  
  if (!debug_p)
    return;

  buf1 = (char *)Ns_Malloc(STACK_BUFFER_SIZE * sizeof(char));
  buf = (char *)Ns_Malloc(STACK_BUFFER_SIZE * sizeof(char));

  va_start (ap, fmt);
  vsprintf (buf1, fmt, ap);
  va_end (ap);
  
  snprintf (buf, STACK_BUFFER_SIZE, "%s:%d:%s: %s", file, line, fn, buf1);
  
  Ns_Log (Notice, "%s", buf);

  Ns_Free(buf1);
  Ns_Free(buf);
} /* log */



/* take a whole string (rather than one character) to lowercase */

static void
downcase (char *s)
{
  for (; *s; s ++)
    *s = tolower (*s);

} /* downcase */



/* nilp is misnamed to some extent; handle empty or overly long strings 
   before printing them out to logs 
*/

static char *
nilp (char *s)
{
  if (s == 0)
    return "[nil]";

  if ((int)strlen (s) > max_string_log_length)
    return "[too long]";

  return s;

} /* nilp */



/* setting this global variable is copied from AOLserver's example drivers 
*/

NS_EXPORT int
Ns_ModuleVersion = 1;



/* Entry point (called by AOLserver when driver loaded) 

   note that this does not leave behind any structures or state outside
   of reading the configuraton parameters, as well as
   initializing OCI and registering our functions
*/

NS_EXPORT int
Ns_DbDriverInit (char *hdriver, char *config_path)
{
  int ns_status;
  oci_status_t oci_status;

  /* slurp any nsd.ini configuration parameters first */
  
  if (!Ns_ConfigGetBool (config_path, "Debug", &debug_p)) 
    debug_p = DEFAULT_DEBUG;

  if (!Ns_ConfigGetInt (config_path, "MaxStringLogLength", &max_string_log_length))
    max_string_log_length = DEFAULT_MAX_STRING_LOG_LENGTH;

  if (max_string_log_length < 0) 
    max_string_log_length = INT_MAX;

  if (!Ns_ConfigGetInt (config_path, "CharExpansion", &char_expansion))
    char_expansion = DEFAULT_CHAR_EXPANSION;

  if (!Ns_ConfigGetInt (config_path, "LobBufferSize", &lob_buffer_size))
    lob_buffer_size = 16384;
  Ns_Log (Notice, "%s driver LobBufferSize = %d", hdriver, lob_buffer_size);


  if (!Ns_ConfigGetInt (config_path, "PrefetchRows", &prefetch_rows))
      prefetch_rows = 0;
  Ns_Log (Notice, "%s driver PrefetchRows = %d", hdriver, prefetch_rows);

  if (!Ns_ConfigGetInt (config_path, "PrefetchMemory", &prefetch_memory))
      prefetch_memory = 0;
  Ns_Log (Notice, "%s driver PrefetchMemory = %d", hdriver, prefetch_memory);


  log (lexpos (), "entry (hdriver %p, config_path %s)", hdriver, nilp (config_path));
  
  oci_status = OCIInitialize (OCI_THREADED,
                              NULL, NULL, NULL, NULL);
  if (oci_error_p (lexpos (), 0, "OCIInitialize", 0, oci_status))
    return NS_ERROR;
  
  ns_status = Ns_DbRegisterDriver (hdriver, ora_procs);
  if (ns_status != NS_OK)
    {
      error (lexpos (), "Could not register driver `%s'.", nilp (ora_driver_name));
      return NS_ERROR;
    }

  Ns_Log (Notice, "Loaded %s, built on %s/%s",
	  ora_driver_version, __TIME__, __DATE__);

#if defined(FOR_CASSANDRACLE)
  Ns_Log (Notice, "    This Oracle Driver is a reduced-functionality Cassandracle driver");
#endif

  log (lexpos (), "driver `%s' loaded.", nilp (ora_driver_name));

  return NS_OK;

} /* Ns_DbDriverInit */



static char *
ora_name (Ns_DbHandle *dummy)
{
  log (lexpos (), "entry (dummy %p)", dummy);

  return ora_driver_name;

} /* ora_name */



static char *
ora_db_type (Ns_DbHandle *dummy)
{
  log (lexpos (), "entry (dummy %p)", dummy);

  return ora_driver_name;

} /* ora_db_type */



/* this is the proc AOLserver calls when it wants a new connection
   it mallocs a connection structure and then stores it in a field
   of handle structure created and maintained by AOLserver
 */

static int
ora_open_db (Ns_DbHandle *dbh)
{
  oci_status_t oci_status;
  ora_connection_t *connection;
  
  log (lexpos (), "entry (dbh %p)", dbh);
  
  if (! dbh)
    {
      error (lexpos (), "invalid args.");
      return NS_ERROR;
    }

  if (! dbh->password)
    {
      error (lexpos (), "Missing Password parameter in configuration file for pool %s.", dbh->poolname );
      return NS_ERROR;
    }

  if (! dbh->user)
    {
      error (lexpos (), "Missing User parameter in configuration file for pool %s.", dbh->poolname);
      return NS_ERROR;
    }
  
  
  connection = Ns_Malloc (sizeof *connection);

  connection->dbh  = dbh;
  connection->env  = NULL;
  connection->err  = NULL;
  connection->srv  = NULL;
  connection->svc  = NULL;
  connection->auth = NULL;
  connection->stmt = NULL;
  connection->mode = autocommit;
  connection->n_columns = 0;
  connection->fetch_buffers = NULL;
  
  /* AOLserver, in their database handle structure, gives us one field
     to store our connection structure */
  dbh->connection = connection;


  /* environment; sets connection->env */
  /* we ask for DEFAULT rather than NO_MUTEX because 
     we're in a multi-threaded environment */
  oci_status = OCIEnvInit (&connection->env,
                           OCI_DEFAULT,
                           0, NULL);
  if (oci_error_p (lexpos (), dbh, "OCIEnvInit", 0, oci_status))
    return NS_ERROR;
  
  /* sets connection->err */
  oci_status = OCIHandleAlloc (connection->env,
			       (oci_handle_t **) &connection->err,
			       OCI_HTYPE_ERROR,
			       0, NULL);
  if (oci_error_p (lexpos (), dbh, "OCIHandleAlloc", 0, oci_status))
    return NS_ERROR;
  
  /* sets connection->srv */
  oci_status = OCIHandleAlloc (connection->env,
			       (oci_handle_t **) &connection->srv,
			       OCI_HTYPE_SERVER,
			       0, NULL);
  if (oci_error_p (lexpos (), dbh, "OCIHandleAlloc", 0, oci_status))
    return NS_ERROR;

  /* sets connection->svc */
  oci_status = OCIHandleAlloc (connection->env,
			       (oci_handle_t **) &connection->svc,
			       OCI_HTYPE_SVCCTX,
			       0, NULL);
  if (oci_error_p (lexpos (), dbh, "OCIHandleAlloc", 0, oci_status))
    return NS_ERROR;
   
  /* create association between server handle and access path (datasource; 
     a string from the nsd.ini file) */
  oci_status = OCIServerAttach (connection->srv, connection->err,
				dbh->datasource,
				strlen (dbh->datasource),
				OCI_DEFAULT);
  if (oci_error_p (lexpos (), dbh, "OCIServerAttach", 0, oci_status))
    return NS_ERROR;
  
  /* tell OCI to associate the server handle with the context handle */
  oci_status = OCIAttrSet (connection->svc,
			   OCI_HTYPE_SVCCTX,
			   connection->srv,
			   0,
			   OCI_ATTR_SERVER,
			   connection->err);
  if (oci_error_p (lexpos (), dbh, "OCIAttrSet", 0, oci_status))
    return NS_ERROR;
  
  /* allocate connection->auth */
  oci_status = OCIHandleAlloc (connection->env,
			       (oci_handle_t **) &connection->auth,
			       OCI_HTYPE_SESSION,
			       0, NULL);
  if (oci_error_p (lexpos (), dbh, "OCIHandleAlloc", 0, oci_status))
    return NS_ERROR;
  
  /* give OCI the username from the nsd.ini file */
  oci_status = OCIAttrSet (connection->auth,
			   OCI_HTYPE_SESSION,
			   dbh->user,
			   strlen (dbh->user),
			   OCI_ATTR_USERNAME,
			   connection->err);
  if (oci_error_p (lexpos (), dbh, "OCIAttrSet", 0, oci_status))
    return NS_ERROR;
  
  /* give OCI the password from the nsd.ini file */
  oci_status = OCIAttrSet (connection->auth,
			   OCI_HTYPE_SESSION,
			   dbh->password,
			   strlen (dbh->password),
			   OCI_ATTR_PASSWORD,
			   connection->err);
  if (oci_error_p (lexpos (), dbh, "OCIAttrSet", 0, oci_status))
    return NS_ERROR;
  
  /* the OCI docs say this "creates a user sesion and begins a 
     user session for a given server */
  oci_status = OCISessionBegin (connection->svc,
				connection->err,
				connection->auth,
				OCI_CRED_RDBMS,
				OCI_DEFAULT);
  if (oci_error_p (lexpos (), dbh, "OCISessionBegin", 0, oci_status))
    return NS_ERROR;
  
  /* associate the particular authentications with a particular context */
  oci_status = OCIAttrSet (connection->svc,
			   OCI_HTYPE_SVCCTX,
			   connection->auth,
			   0,
			   OCI_ATTR_SESSION,
			   connection->err);
  if (oci_error_p (lexpos (), dbh, "OCIAttrSet", 0, oci_status))
    return NS_ERROR;
  
  
  log (lexpos (), "(dbh %p); return NS_OK;", dbh);

  return NS_OK;

} /* ora_open_db */



/* the objective here is to free all the handles that we created in
   ora_open_db, Ns_Free the connection structure and set the AOLserver
   db handle's connection field to null 
*/

static int
ora_close_db (Ns_DbHandle *dbh)
{
  oci_status_t oci_status;
  ora_connection_t *connection;
  
  log (lexpos (), "entry (dbh %p)", dbh);
  
  if (! dbh)
    {
      error (lexpos (), "invalid args.");
      return NS_ERROR;
    }
  
  connection = dbh->connection;
  if (! connection)
    {
      error (lexpos (), "no connection.");
      return NS_ERROR;
    }
  
  /* don't return on error; just clean up the best we can */
  oci_status = OCIServerDetach (connection->srv,
				connection->err,
				OCI_DEFAULT);
  oci_error_p (lexpos (), dbh, "OCIServerDetach", 0, oci_status);
  
  oci_status = OCIHandleFree (connection->svc,
			      OCI_HTYPE_SVCCTX);
  oci_error_p (lexpos (), dbh, "OCIHandleFree", 0, oci_status);
  connection->svc = 0;
  
  oci_status = OCIHandleFree (connection->srv,
			      OCI_HTYPE_SERVER);
  oci_error_p (lexpos (), dbh, "OCIHandleFree", 0, oci_status);
  connection->srv = 0;
  
  oci_status = OCIHandleFree (connection->err,
			      OCI_HTYPE_ERROR);
  oci_error_p (lexpos (), dbh, "OCIHandleFree", 0, oci_status);
  connection->err = 0;
  
  oci_status = OCIHandleFree (connection->env,
			      OCI_HTYPE_ENV);
  oci_error_p (lexpos (), dbh, "OCIHandleFree", 0, oci_status);
  connection->env = 0;
  
  Ns_Free (connection);
  dbh->connection = NULL;
  
  return NS_OK;

} /* ora_close_db */



#if defined(FOR_CASSANDRACLE)

/* Because Cassandracle (http://www.arsdigita.com/free-tools/cassandracle.html)
   runs with DBA priviliges, we need to prevent anything
   Bad from happening, whether through malicious intent or just plain
   human sloppiness.  Selects are pretty safe, so only those are allowed
   if FOR_CASSANDRACLE is defined, disallow any sql that does not
   begin with "select"
 */

static int
allow_sql_p (Ns_DbHandle *dbh, char *sql, int display_sql_p)
{
  char *trimmedSql = sql;

  /* trim off leading white space. (the int cast is necessary for the HP) */
  while (*trimmedSql && isspace((int)*trimmedSql)) 
    {
      trimmedSql++;
    }

  
  /* (damned if you do, damned if you don't.  doing a
   * strlen("select") each time here is wasteful of CPU which
   * would offend the sensibilities of half the world.  hard-coding
   * the "6" offends the other half)
   */
  if (strncasecmp(trimmedSql, "select", 6) != 0) 
    {
	int bufsize = strlen (sql) + 4096;

	/* don't put a 20,000 byte buffer on the stack here, since this case
	 * should be very rare.  allocate enough space for the sql
	 * plus some to handle the other text.  (4K should be enough to
	 * handle the lexpos() call)
	 */
	char *buf = Ns_Malloc (bufsize);
	
	/* means someone is trying to do something other than a select.
	 * Bad!  Very Bad!
	 */
	if (display_sql_p) 
	{
	    snprintf (buf, bufsize, "%s:%d:%s: Sql Rejected: %s", 
		      lexpos (), trimmedSql);
	} 
	else 
	{
	    snprintf (buf, bufsize, "%s:%d:%s: Sql Rejected", lexpos ());
	}
	
	Ns_Log (Error, "%s", buf);
	
	Ns_DbSetException (dbh, "ORA", buf);
	
	Ns_Free (buf);
	
	return NS_FALSE;
    }

  return NS_TRUE;

} /* allow_sql_p */
#endif

#if !defined(FOR_CASSANDRACLE)
#define allow_sql_p(a,b,c) NS_TRUE
#endif



/* this gets called on every query or dml.  Usually it will 
   return NS_OK ("I did nothing").  If the SQL is one of our special 
   cases, e.g., "begin transaction", that aren't supposed to go through
   to Oracle, we handle it and return NS_DML ("I handled it and nobody 
   else has do anything").

   return NS_ERROR on error
*/

static int
handle_builtins (Ns_DbHandle *dbh, char *sql)
{
  oci_status_t oci_status;
  ora_connection_t *connection;
  
  log (lexpos (), "entry (dbh %p, sql %s)", dbh, nilp (sql));
  
  /* args should be correct */
  connection = dbh->connection;
  
  if (! strcasecmp (sql, "begin transaction"))
    {
      log (lexpos (), "builtin `begin transaction`");
      
      connection->mode = transaction;

      return NS_DML;
    }
  else if (! strcasecmp (sql, "end transaction"))
    {
      log (lexpos (), "builtin `end transaction`");
      
      oci_status = OCITransCommit (connection->svc,
                                   connection->err,
                                   OCI_DEFAULT);
      if (oci_error_p (lexpos (), dbh, "OCITransCommit", sql, oci_status))
	{
	  flush_handle (dbh);
	  return NS_ERROR;
	}
      
      connection->mode = autocommit;
      return NS_DML;
    }
  else if (! strcasecmp (sql, "abort transaction"))
    {
      log (lexpos (), "builtin `abort transaction`");
      
      oci_status = OCITransRollback (connection->svc,
                                     connection->err,
                                     OCI_DEFAULT);
      if (oci_error_p (lexpos (), dbh, "OCITransRollback", sql, oci_status))
	{
	  flush_handle (dbh);
	  return NS_ERROR;
	}
      
      connection->mode = autocommit;
      return NS_DML;
    }

  if (!allow_sql_p(dbh, sql, NS_FALSE))
    {
      flush_handle (dbh);
      return NS_ERROR;
    }

  /* not handled */
  return NS_OK;

} /* handle_builtins */



/* this is called when Tcl script invokes [ns_db dml ...] 
   we don't really care what it is (and don't tell ora_exec)
   except that we raise an error if ora_exec tells us that it
   was not a DML statement 
*/

static int
ora_dml (Ns_DbHandle *dbh, char *sql)
{
  int ns_status;
  
  ns_status = ora_exec (dbh, sql);
  if (ns_status != NS_DML)
    {
      flush_handle (dbh);
      return NS_ERROR;
    }

  return NS_OK;

} /* ora_dml */



static int
ora_exec (Ns_DbHandle *dbh, char *sql)
{
  oci_status_t oci_status;
  ora_connection_t *connection;
  ub4 iters;  
  ub2 type;
  
  log (lexpos (), "generate simple message");
  log (lexpos (), "entry (dbh %p, sql %s)", dbh, nilp (sql));
  
  if (! dbh || !sql)
    {
      error (lexpos (), "invalid args.");
      return NS_ERROR;
    }
  
  connection = dbh->connection;
  if (connection == 0)
    {
      error (lexpos (), "no connection.");
      return NS_ERROR;
    }
  
  /* nuke any previously executing stmt */
  flush_handle (dbh);
  
  /* handle_builtins will flush the handles on a ERROR exit */

  switch (handle_builtins (dbh, sql))
    {
    case NS_DML:
      /* handled */
      return NS_DML;
      
    case NS_ERROR:
      return NS_ERROR;
      
    case NS_OK:
      break;
      
    default:
      error (lexpos (), "internal error");
      return NS_ERROR;
    }

  /* allocate a new handle and stuff in connection->stmt */
  oci_status = OCIHandleAlloc (connection->env,
                               (oci_handle_t **) &connection->stmt,
                               OCI_HTYPE_STMT,
                               0, NULL);
  if (oci_error_p (lexpos (), dbh, "OCIHandleAlloc", sql, oci_status))
    {
      flush_handle (dbh);
      return NS_ERROR;
    }

  /* purely a local call to "prepare statement for execution" */
  oci_status = OCIStmtPrepare (connection->stmt,
                               connection->err,
                               sql, strlen (sql),
                               OCI_NTV_SYNTAX,
                               OCI_DEFAULT);
  if (oci_error_p (lexpos (), dbh, "OCIStmtPrepare", sql, oci_status))
    {
      flush_handle (dbh);
      return NS_ERROR;
    }

  /* check what type of statment it is, this will affect how
     many times we expect to execute it */
  oci_status = OCIAttrGet (connection->stmt,
                           OCI_HTYPE_STMT,
			   (oci_attribute_t *) &type,
			   NULL,
                           OCI_ATTR_STMT_TYPE,
			   connection->err);
  if (oci_error_p (lexpos (), dbh, "OCIAttrGet", sql, oci_status))
    {
      /* got error asking Oracle for the statement type */
      flush_handle (dbh);
      return NS_ERROR;
    }

  if (type == OCI_STMT_SELECT)
    {
      iters = 0;
    }
  else
    {
      iters = 1;
    }

  /* actually go to server and execute statement */
  oci_status = OCIStmtExecute (connection->svc,
                               connection->stmt,
                               connection->err,
                               iters,
                               0, NULL, NULL,
                               (connection->mode == autocommit
                                ? OCI_COMMIT_ON_SUCCESS
                                : OCI_DEFAULT));
  if (oci_status == OCI_ERROR)
    {
      oci_status_t oci_status1;
      sb4 errorcode;

      oci_status1 = OCIErrorGet (connection->err,
                                 1,
                                 NULL,
                                 &errorcode,
                                 0,
                                 0,
                                 OCI_HTYPE_ERROR);

      if (oci_error_p (lexpos (), dbh, "OCIErrorGet", sql, oci_status1))
	{
	  /* the error getter got an error; let's bail */
	  flush_handle (dbh);
	  return NS_ERROR;
	}
      else if (oci_error_p (lexpos (), dbh, "OCIStmtExecute", sql, oci_status))
	{
	  /* this is where we end up for an ordinary error-producing SQL statement
	     we call oci_error_p above so that crud ends up in the log */
	  flush_handle (dbh);
	  return NS_ERROR;
	}
    }
  else if (oci_error_p (lexpos (), dbh, "OCIStmtExecute", sql, oci_status))
    {
      /* we got some weird error that wasn't OCI_ERROR; we hardly ever get here */
      flush_handle (dbh);
      return NS_ERROR;
    }

  log (lexpos (), "query type `%d'", type);
  
  if (type == OCI_STMT_SELECT)
    return NS_ROWS;
  else
    return NS_DML;

} /* ora_exec */


/*
 * malloc_fetch_buffers allocates the fetch_buffers array in the
 * specified connection.  connection->n_columns must be set to the
 * correct number before calling this function.
 */
static void
malloc_fetch_buffers (ora_connection_t *connection)
{
  int i;
  
  connection->fetch_buffers
    = Ns_Malloc (connection->n_columns * sizeof *connection->fetch_buffers);

  for (i = 0; i < connection->n_columns; i ++)
    {
      fetch_buffer_t *fetchbuf = &connection->fetch_buffers[i];
      
      fetchbuf->connection = connection;
      
      fetchbuf->type = 0;
      fetchbuf->lob = NULL;
      fetchbuf->bind = NULL;
      fetchbuf->def = NULL;
      fetchbuf->size = 0;
      fetchbuf->buf_size = 0;
      fetchbuf->buf = NULL;
      fetchbuf->array_count = 0;
      fetchbuf->array_values = NULL;
      fetchbuf->is_null = 0;
      fetchbuf->fetch_length = 0;
      fetchbuf->piecewise_fetch_length = 0;
      
      fetchbuf->lobs = NULL;
      fetchbuf->is_lob = 0;
      fetchbuf->n_rows = 0;
    }

} /* malloc_fetch_buffers */


/*
 * free_fetch_buffers frees the fetch_buffers array in the specified
 * connection.  connection->n_columns must have the same value as it
 * did when malloc_fetch_buffers was called.  The non-NULL
 * dynamically-allocated components of each fetchbuf will also be freed.
 */
static void
free_fetch_buffers(ora_connection_t *connection)
{
  if (connection != NULL && connection->fetch_buffers != NULL)
    {
      Ns_DbHandle *dbh = connection->dbh;
      int i;
      oci_status_t oci_status;

      for (i = 0; i < connection->n_columns; i++)
	{
	  fetch_buffer_t *fetchbuf = &connection->fetch_buffers[i];

	  if (fetchbuf->lob != NULL)
	    {
	      oci_status = OCIDescriptorFree(fetchbuf->lob, OCI_DTYPE_LOB);
	      oci_error_p(lexpos(), dbh, "OCIDescriptorFree", 0, oci_status);
	      fetchbuf->lob = NULL;
	    }

	  /*
	   * fetchbuf->bind is automatically deallocated when its
	   * statement is deallocated.
	   *
	   * Same for fetchbuf->def, I believe, though the manual
	   * doesn't say.
	   */

	  if (fetchbuf->buf != NULL)
	    {
	      Ns_Free(fetchbuf->buf);
	      fetchbuf->buf = NULL;
	      fetchbuf->buf_size = 0;
	    }

	  if (fetchbuf->array_values != NULL)
	    {
              /* allocated from Tcl_SplitList so Tcl_Free it */
	      Tcl_Free((char *)fetchbuf->array_values);
	      fetchbuf->array_values = NULL;
	      fetchbuf->array_count = 0;
	    }

	  if (fetchbuf->lobs != 0)
	    {
	      for (i = 0; i < fetchbuf->n_rows; i ++)
		{
		  oci_status = OCIDescriptorFree(fetchbuf->lobs[i],
		    OCI_DTYPE_LOB);
		  oci_error_p(lexpos(), dbh, "OCIDescriptorFree", 0,
		    oci_status);
		}
	      Ns_Free(fetchbuf->lobs);
	      fetchbuf->lobs = NULL;
	      fetchbuf->n_rows = 0;
	    }
	}

      Ns_Free(connection->fetch_buffers);
      connection->fetch_buffers = NULL;
    }
}

/* these are read-only and hence thread-safe */

static sb2 null_ind = -1;
static sb2 rc = 0;
static ub4 rl = 0;

/* For use by OCIBindDynamic: returns the iter'th element (0-relative)
   of the context pointer taken as an array of strings (char**). */
static sb4
list_element_put_data(dvoid   *ictxp,
		      OCIBind *bindp,
		      ub4     iter,
		      ub4     index,
		      dvoid   **bufpp,
		      ub4     *alenp,
		      ub1     *piecep,
		      dvoid   **indpp)
{
    fetch_buffer_t *fetchbuf = ictxp;
    char **elements = fetchbuf->array_values;

    *bufpp = elements[iter];
    *alenp = strlen(elements[iter]);
    *piecep = OCI_ONE_PIECE;
    *indpp = NULL;
    return OCI_CONTINUE;
}

/* this has been tested and found not to work in all cases
   Cotton worked on it for 6 hours and could not make it work;
   you have been warned.
   MarkD thinks this should be scrapped and the 'piecewise-get' loop
   should be used, but that's _another_ world of hurt.  This function
   works OK for single-piece LONGs, which is all we're going support
   for now.
*/

static sb4
long_get_data (dvoid *octxp,
	       OCIDefine *defnp,
	       ub4 iter,
	       dvoid **bufpp,
	       ub4 **alenpp,
	       ub1 *peicep,
	       dvoid **indpp,
	       ub2 **rcodep)
{
  fetch_buffer_t *fetchbuf = octxp;
  
  log (lexpos (), "entry (dbh %p; iter %d)", octxp, iter);
  log (lexpos (), "*peicep = `%d', buf_size `%d'", *peicep, fetchbuf->buf_size);
  log (lexpos (), "%p %p", defnp, fetchbuf->def);
  
  if (*peicep == OCI_ONE_PIECE || *peicep == OCI_FIRST_PIECE)
    fetchbuf->fetch_length = 0;
  else if (*peicep == OCI_NEXT_PIECE)
    fetchbuf->fetch_length += fetchbuf->piecewise_fetch_length;
  
  /* *peicep = OCI_NEXT_PIECE; */
  
  if (fetchbuf->fetch_length > fetchbuf->buf_size / 2)
    {
      fetchbuf->buf_size *= 2;
      fetchbuf->buf = Ns_Realloc (fetchbuf->buf, fetchbuf->buf_size);
    }
  
  fetchbuf->piecewise_fetch_length = fetchbuf->buf_size - fetchbuf->fetch_length;
  
  log (lexpos (), "%d, %d, %d",
       fetchbuf->buf_size,
       fetchbuf->fetch_length,
       fetchbuf->piecewise_fetch_length);
  
  /* *piecep = ???; */
  
  *bufpp = &fetchbuf->buf[fetchbuf->fetch_length];
  *alenpp = &fetchbuf->piecewise_fetch_length;
  *indpp = &fetchbuf->is_null;
  *rcodep = &rc;
  
  return OCI_CONTINUE;

} /* long_get_data */



/* AOLserver calls this one per query and gets back an ns_set of null
   strings keyed by column names; the ns_set actually comes from row
   field of the db handle struct.  The ns_set is reused every time a row 
   is fetched 
*/

static Ns_Set *
ora_bindrow (Ns_DbHandle *dbh)
{
  oci_status_t oci_status;
  ora_connection_t *connection;
  Ns_Set *row = 0;
  int i;
  
  log (lexpos (), "entry (dbh %p)", dbh);
  
  if (! dbh)
    {
      error (lexpos (), "invalid args.");
      return 0;
    }
  
  connection = dbh->connection;
  if (connection == 0)
    {
      error (lexpos (), "no connection.");
      return 0;
    }
  
  if (connection->stmt == 0)
    {
      error (lexpos (), "no active query statement executing");
      return 0;
    }
  
  if (connection->fetch_buffers != 0)
    {
      error (lexpos (), "query already bound");
      flush_handle (dbh);
      return 0;
    }
  
  row = dbh->row;
  
  /* get number of columns returned by query; sets connection->n_columns */
  oci_status = OCIAttrGet (connection->stmt,
                           OCI_HTYPE_STMT,
			   (oci_attribute_t *) &connection->n_columns,
			   NULL,
                           OCI_ATTR_PARAM_COUNT,
			   connection->err);
  if (oci_error_p (lexpos (), dbh, "OCIAttrGet", 0, oci_status))
    {
      flush_handle (dbh);
      return 0;
    }
  
  log (lexpos (), "n_columns: %d", connection->n_columns);
  
  /* allocate N fetch buffers, this proc pulls N from connection->n_columns */
  malloc_fetch_buffers (connection);
  
  for (i = 0; i < connection->n_columns; i ++)
    {
      fetch_buffer_t *fetchbuf;
      OCIParam *param;
      
      /* 512 is large enough because Oracle sends back table_name.column_name and 
	 neither right now can be larger than 30 chars */
      char name[512];
      char *name1 = 0;
      sb4 name1_size = 0;
      
      /* set current fetch buffer */
      fetchbuf = &connection->fetch_buffers[i];
      
      oci_status = OCIParamGet (connection->stmt,
				OCI_HTYPE_STMT,
				connection->err,
				(oci_param_t *) &param, i + 1);
      if (oci_error_p (lexpos (), dbh, "OCIParamGet", 0, oci_status))
	{
	  flush_handle (dbh);
	  return 0;
	}
      
      oci_status = OCIAttrGet (param,
                               OCI_DTYPE_PARAM,
                               (oci_attribute_t *) &name1,
                               &name1_size,
                               OCI_ATTR_NAME,
                               connection->err);
      if (oci_error_p (lexpos (), dbh, "OCIAttrGet", 0, oci_status))
	{
	  flush_handle (dbh);
	  return 0;
	}

      /* Oracle gives us back a pointer to a string that is not null-terminated
	 so we copy it into our local var and add a 0 at the end */
      memcpy (name, name1, name1_size);
      name[name1_size] = 0;
      /* we downcase the column name for backward-compatibility with philg's
	 AOLserver Tcl scripts written for the case-sensitive Illustra
	 RDBMS.  philg was lucky in that he always used lowercase.  You might want
         to change this to leave everything all-uppercase if you're a traditional
         Oracle shop */
      downcase (name);
      
      log (lexpos (), "name %d `%s'", name1_size, name);
      Ns_SetPut (row, name, 0);
      
      /* get the column type */
      oci_status = OCIAttrGet (param,
			       OCI_DTYPE_PARAM,
			       (oci_attribute_t *) &fetchbuf->type,
			       NULL,
			       OCI_ATTR_DATA_TYPE,
			       connection->err);
      if (oci_error_p (lexpos (), dbh, "OCIAttrGet", 0, oci_status))
	{
	  flush_handle (dbh);
	  return 0;
	}
      
      log (lexpos (), "column `%s' type `%d'", name, fetchbuf->type);
      
      switch (fetchbuf->type)
	{
	  /* we handle LOBs in the loop below */
	case OCI_TYPECODE_CLOB:
	case OCI_TYPECODE_BLOB:
	  break;

	  /* RDD is Oracle's happy fun name for ROWID (18 chars long
             but if you ask Oracle the usual way, it will give you a
             number that is too small) */
	case SQLT_RDD:
	  fetchbuf->size = 18;
          fetchbuf->buf_size = fetchbuf->size + 8;
          fetchbuf->buf = Ns_Malloc (fetchbuf->buf_size);
	  break;
	  
	case SQLT_NUM:
	  /* OCI reports that all NUMBER values has a size of 22, the size
	     of its internal storage format for numbers. We are fetching
	     all values out as strings, so we need more space. Empirically,
	     it seems to return 41 characters when it does the NUMBER to STRING
	     conversion. */
	  fetchbuf->size = 41;
          fetchbuf->buf_size = fetchbuf->size + 8;
          fetchbuf->buf = Ns_Malloc (fetchbuf->buf_size);
	  break;
	  
	  /* this might work if the rest of our LONG stuff worked */
	case SQLT_LNG:
          fetchbuf->buf_size = lob_buffer_size;
          fetchbuf->buf = Ns_Malloc (fetchbuf->buf_size);
	  break;
	  
	default:
          /* get the size */
          oci_status = OCIAttrGet (param,
                                   OCI_DTYPE_PARAM,
                                   (oci_attribute_t *) &fetchbuf->size,
                                   NULL,
                                   OCI_ATTR_DATA_SIZE,
                                   connection->err);
          if (oci_error_p (lexpos (), dbh, "OCIAttrGet", 0, oci_status))
	    {
	      flush_handle (dbh);
	      return 0;
	    }
          
	  log (lexpos (), "column `%s' size `%d'", name, fetchbuf->size);
	  
	  /* This is the important part, we allocate buf to be 8 bytes
             more than Oracle says are necessary (for null termination).
	     In the case of a RAW column we need 2x the column size
	     because the value will be returned in hex.  */
	  if (fetchbuf->type == SQLT_BIN)
            fetchbuf->buf_size = fetchbuf->size * 2 + 8;
	  else
            fetchbuf->buf_size = fetchbuf->size + 8;
	  fetchbuf->buf_size *= char_expansion;
          fetchbuf->buf = Ns_Malloc (fetchbuf->buf_size);
	  break;
        }
    }
  
  /* loop over the columns again; this could now be in the loop above
     but we originally did things this way to permit resizing of
     buffers

     Now we're telling Oracle to associate the buffers we just
     allocated with their respective columns */
  for (i = 0; i < connection->n_columns; i ++)
    {
      fetch_buffer_t *fetchbuf;
      
      fetchbuf = &connection->fetch_buffers[i];

      switch (fetchbuf->type)
	{
	case OCI_TYPECODE_CLOB:
	case OCI_TYPECODE_BLOB:
	  /* we allocate descriptors for CLOBs; these are essentially
             pointers.  We will not allocate any buffers for them
             until we're actually fetching data from individual
             rows. */
          oci_status = OCIDescriptorAlloc (connection->env,
                                           (oci_descriptor_t *) &fetchbuf->lob,
                                           OCI_DTYPE_LOB,
                                           0,
                                           0);
          if (oci_error_p (lexpos (), dbh, "OCIDescriptorAlloc", 0, oci_status))
	    {
	      flush_handle (dbh);
	      return 0;
	    }
          
          oci_status = OCIDefineByPos (connection->stmt,
                                       &fetchbuf->def,
                                       connection->err,
                                       i + 1,
                                       &fetchbuf->lob,
                                       -1,
                                       fetchbuf->type,
                                       &fetchbuf->is_null,
                                       0,
                                       0,
                                       OCI_DEFAULT);
          if (oci_error_p (lexpos (), dbh, "OCIDefineByPos", 0, oci_status))
	    {
	      flush_handle (dbh);
	      return 0;
	    }
	  break;
	  
	case SQLT_LNG:
          oci_status = OCIDefineByPos (connection->stmt,
                                       &fetchbuf->def,
                                       connection->err,
                                       i + 1,
                                       0,
                                       (sb4)SB4MAXVAL,
                                       fetchbuf->type,
                                       &fetchbuf->is_null,
                                       &fetchbuf->fetch_length,
                                       0,
                                       OCI_DYNAMIC_FETCH);
          
          if (oci_error_p (lexpos (), dbh, "OCIDefineByPos", 0, oci_status))
	    {
	      flush_handle (dbh);
	      return 0;
	    }
	  
	  log (lexpos (), "`OCIDefineDynamic ()' success");
	  break;
	  
	default:
          oci_status = OCIDefineByPos (connection->stmt,
                                       &fetchbuf->def,
                                       connection->err,
                                       i + 1,
                                       fetchbuf->buf,
                                       fetchbuf->buf_size,
                                       SQLT_STR,
                                       &fetchbuf->is_null,
                                       &fetchbuf->fetch_length,
                                       NULL,
                                       OCI_DEFAULT);
          
          if (oci_error_p (lexpos (), dbh, "OCIDefineByPos", 0, oci_status))
	    {
	      flush_handle (dbh);
	      return 0;
	    }
	  break;
        }
    }
  
  return row;

} /* ora_bindrow */



/* ora_select = do an exec and then a bindrow */

static Ns_Set *
ora_select (Ns_DbHandle *dbh, char *sql)
{
  int ns_status;
  
  log (lexpos (), "entry (dbh %p, sql %s)", dbh, nilp (sql));
  
  if (! dbh || !sql)
    {
      error (lexpos (), "invalid args.");
      return 0;
    }
  
  ns_status = ora_exec (dbh, sql);
  if (ns_status != NS_ROWS)
    {
      flush_handle (dbh);
      return 0;
    }

  return ora_bindrow (dbh);

} /* ora_select */



/* this is called every time someone calls [ns_db getrow ...] */

static int
ora_get_row (Ns_DbHandle *dbh, Ns_Set *row)
{
  oci_status_t oci_status;
  ora_connection_t *connection;
  int i;
  ub4 ret_len = 0;

  log (lexpos (), "entry (dbh %p, row %p)", dbh, row);
  
  if (! dbh || !row)
    {
      error (lexpos (), "invalid args.");
      return NS_ERROR;
    }
  
  connection = dbh->connection;
  if (connection == 0)
    {
      error (lexpos (), "no connection.");
      return NS_ERROR;
    }
  
  if (row == 0)
    {
      error (lexpos (), "invalid argument, `NULL' row");
      flush_handle (dbh);
      return NS_ERROR;
    }
  
  if (connection->stmt == 0)
    {
      error (lexpos (), "no active select");
      flush_handle (dbh);
      return NS_ERROR;
    }
  
  /* fetch */
  oci_status = OCIStmtFetch (connection->stmt,
			     connection->err,
			     1,
			     OCI_FETCH_NEXT,
			     OCI_DEFAULT);
  if (oci_status == OCI_NEED_DATA) {
      ; 
  } 
  else if (oci_status == OCI_NO_DATA)
    {
      /* we've reached beyond the last row of the select, so flush the
         statement and tell AOLserver that it isn't going to get
         anything more out of us. */
      log (lexpos (), "return NS_END_DATA;");
      
      if (flush_handle (dbh) != NS_OK)
	return NS_ERROR;
      else
	return NS_END_DATA;
    }
  else if (oci_error_p (lexpos (), dbh, "OCIStmtFetch", 0, oci_status))
    {
      /* we got some other kind of error */
      flush_handle (dbh);
      return NS_ERROR;
    }
  /* fetched succeeded; copy fetch buffers (one/column) into the ns_set */
  for (i = 0; i < connection->n_columns; i ++)
    {
      fetch_buffer_t *fetchbuf = &connection->fetch_buffers[i];

      switch (fetchbuf->type)
	{
	case OCI_TYPECODE_CLOB:
	case OCI_TYPECODE_BLOB:

	  if (fetchbuf->is_null == -1)
	    Ns_SetPutValue (row, i, "");
	  else if (fetchbuf->is_null != 0)
	    {
	      error (lexpos (), "invalid fetch buffer is_null");
	      flush_handle (dbh);
	      return NS_ERROR;
	    }
	  else
            {
	      /* CLOB is not null, let's grab it. We use an Ns_DString
		 to do this, because when dealing with variable width
		 character sets, a single character can be many bytes long
		 (in UTF8, up to six). */
              ub4 lob_length = 0;
              Ns_DString retval;
	      ub1 *bufp;

	      /* Get length of LOB, in characters for CLOBs and bytes
		 for BLOBs. */
              oci_status = OCILobGetLength (connection->svc,
                                            connection->err,
                                            fetchbuf->lob,
                                            &lob_length);
              if (oci_error_p (lexpos (), dbh, "OCILobGetLength", 
			       0, oci_status))
                {
                  flush_handle (dbh);
                  return NS_ERROR;
                }
	    
	      if (lob_length == 0)
		{
		  Ns_SetPutValue (row, i, "");
		}

	      else
		{
		  /* Initialize the buffer we're going to use for the value. */
		  bufp =(ub1 *)Ns_Malloc(lob_buffer_size);
		  Ns_DStringInit(&retval);

		  /* Do the read. */
		  oci_status = OCILobRead (
		    connection->svc,
		    connection->err,
		    fetchbuf->lob,
		    &lob_length,
		    (ub4) 1,
		    bufp,
		    lob_buffer_size,
		    &retval,
		    (OCICallbackLobRead) ora_append_buf_to_dstring,
		    (ub2) 0,
		    (ub1) SQLCS_IMPLICIT);

		  if (oci_error_p (lexpos (), dbh, "OCILobRead", 0, oci_status))
		    {
		      flush_handle (dbh);
		      Ns_DStringFree (&retval);
		      Ns_Free(bufp);
		      return NS_ERROR;
		    }
		
		  Ns_SetPutValue (row, i, Ns_DStringValue(&retval));
		  Ns_DStringFree (&retval);
		  Ns_Free(bufp);
		}
            }
	  break;
	  
	case SQLT_LNG:
	  /* this is broken for multi-part LONGs.  LONGs are being deprecated
	   * by Oracle anyway, so no big loss
           *
           * Maybe fixed by davis@arsdigita.com
	   */
	  if (fetchbuf->is_null == -1)
	    fetchbuf->buf[0] = 0;
	  else if (fetchbuf->is_null != 0)
	    {
	      error (lexpos (), "invalid fetch buffer is_null");
	      flush_handle (dbh);
	      return NS_ERROR;
	    }
	  else
          {
              fetchbuf->buf[0] = 0;
              fetchbuf->fetch_length = 0;
              ret_len = 0;
 
              log(lexpos(), "LONG start: buf_size=%d fetched=%d\n", fetchbuf->buf_size, fetchbuf->fetch_length);
 
              do {
                  dvoid *def;
                  ub1 inoutp;
                  ub1 piece;
                  ub4 type;
                  ub4 iterp;
                  ub4 idxp;
         
                  fetchbuf->fetch_length += ret_len;                     
                  if (fetchbuf->fetch_length > fetchbuf->buf_size / 2) {
                      fetchbuf->buf_size *= 2;
                      fetchbuf->buf = ns_realloc (fetchbuf->buf, fetchbuf->buf_size);
                  }
                  ret_len = fetchbuf->buf_size - fetchbuf->fetch_length;
 
                  oci_status = OCIStmtGetPieceInfo(connection->stmt, 
                                                   connection->err,
                                                   (dvoid **)&fetchbuf->def,   
                                                   &type, 
                                                   &inoutp, 
                                                   &iterp,    
                                                   &idxp, 
                                                   &piece);
    
                  if (oci_error_p (lexpos (), dbh, "OCIStmtGetPieceInfo", 0, oci_status)) {
                      flush_handle (dbh);
                      return NS_ERROR;
                  }
 
                  oci_status = OCIStmtSetPieceInfo(
                      fetchbuf->def,   
                      OCI_HTYPE_DEFINE,   
                      connection->err, 
                      (void *) (fetchbuf->buf + fetchbuf->fetch_length),
                      &ret_len,
                      piece,
                      NULL, 
                      NULL);
 
                  if (oci_error_p (lexpos (), dbh, "OCIStmtGetPieceInfo", 0, oci_status)) {
                      flush_handle (dbh);
                      return NS_ERROR;
                  }
 
                  oci_status = OCIStmtFetch(connection->stmt, 
                                            connection->err,
                                            1, 
                                            OCI_FETCH_NEXT,
                                            OCI_DEFAULT);   
 
                  log(lexpos(), "LONG: status=%d ret_len=%d buf_size=%d fetched=%d\n", oci_status, ret_len, fetchbuf->buf_size, fetchbuf->fetch_length);
 
                  if (oci_status != OCI_NEED_DATA 
                      && oci_error_p (lexpos (), dbh, "OCIStmtFetch", 0, oci_status)) {
                      flush_handle (dbh);
                      return NS_ERROR;
                  }
 
                  if (oci_status == OCI_NO_DATA)
                      break;    
    
              } while (oci_status == OCI_SUCCESS_WITH_INFO ||    
                       oci_status == OCI_NEED_DATA);   
 
          }

          fetchbuf->buf[fetchbuf->fetch_length] = 0;
          log(lexpos(), "LONG done: status=%d buf_size=%d fetched=%d\n", oci_status, fetchbuf->buf_size, fetchbuf->fetch_length);
          
          Ns_SetPutValue (row, i, fetchbuf->buf);
	  
	  break;
	  
	default:
	  /* add null termination and then do an ns_set put */
	  if (fetchbuf->is_null == -1)
	    fetchbuf->buf[0] = 0;
	  else if (fetchbuf->is_null != 0)
	    {
	      error (lexpos (), "invalid fetch buffer is_null");
	      flush_handle (dbh);
	      return NS_ERROR;
	    }
	  else
            fetchbuf->buf[fetchbuf->fetch_length] = 0;

          Ns_SetPutValue (row, i, fetchbuf->buf);

	  break;
        }
    }
  
  return NS_OK;

} /* ora_get_row */

/* Callback function for LOB case in ora_get_row. */
static sb4 ora_append_buf_to_dstring (dvoid *ctxp, CONST dvoid *bufp, ub4 len, ub1 piece)
{
  Ns_DString *retval = (Ns_DString *) ctxp;

  switch (piece)
    {
    case OCI_LAST_PIECE:
    case OCI_FIRST_PIECE:
    case OCI_NEXT_PIECE:
      Ns_DStringNAppend(retval, (char *) bufp, len);
      return OCI_CONTINUE;

    default:
      return OCI_ERROR;
    }
} /* ora_append_buf_to_dstring */



/* this is called before every sql exec (select or dml)
   after any other part of the driver notices an error
   and after you've fetch the last row 
*/

static int
flush_handle (Ns_DbHandle *dbh)
{
  oci_status_t oci_status;
  ora_connection_t *connection;
  int i;
  
  log (lexpos (), "entry (dbh %p, row %p)", dbh, 0);

  if (dbh == 0)
    {
      error (lexpos (), "invalid args, `NULL' database handle");
      return NS_ERROR;
    }

  connection = dbh->connection;

  if (connection == 0)
    {
      /* Connection is closed.  That's as good as flushed to me */
      return NS_OK;
    }

  if (connection->stmt != 0)
    {
      oci_status = OCIHandleFree (connection->stmt,
                                  OCI_HTYPE_STMT);
      if (oci_error_p (lexpos (), dbh, "OCIHandleFree", 0, oci_status))
	return NS_ERROR;
      
      connection->stmt = 0;
    }
  
  if (connection->fetch_buffers != 0)
    {
      for (i = 0; i < connection->n_columns; i ++)
	{
	  fetch_buffer_t *fetchbuf = &connection->fetch_buffers[i];
	  
	  log (lexpos (), "fetchbuf %d, %p, %d, %p, %p, %p", i, fetchbuf, fetchbuf->type, fetchbuf->lob, fetchbuf->buf, fetchbuf->lobs);
	  
	  if (fetchbuf->lob != 0)
	    {
	      oci_status = OCIDescriptorFree (fetchbuf->lob,
					      OCI_DTYPE_LOB);
	      oci_error_p (lexpos (), dbh, "OCIDescriptorFree", 0, oci_status);
	      fetchbuf->lob = 0;
	    }
	  
	  Ns_Free (fetchbuf->buf);
	  fetchbuf->buf = NULL;
	  Ns_Free(fetchbuf->array_values);
	  fetchbuf->array_values = NULL;
	  
	  if (fetchbuf->lobs != 0)
	    {
	      int k;
	      for (k = 0; k < (int)fetchbuf->n_rows; k ++)
		{
		  oci_status = OCIDescriptorFree (fetchbuf->lobs[k],
						  OCI_DTYPE_LOB);
		  oci_error_p (lexpos (), dbh, "OCIDescriptorFree", 0, oci_status);
		}
	      Ns_Free (fetchbuf->lobs);
	      fetchbuf->lobs = NULL;
	      fetchbuf->n_rows = 0;
	    }
	}
      
      Ns_Free (connection->fetch_buffers);
      connection->fetch_buffers = 0;
    }
  
  return NS_OK;

} /* flush_handle */



/* this just calls flush */

static int
ora_flush (Ns_DbHandle *dbh)
{
  ora_connection_t *connection;
  
  log (lexpos (), "entry (dbh %p)", dbh);
  
  if (dbh == 0)
    {
      error (lexpos (), "invalid args, `NULL' database handle");
      return NS_ERROR;
    }
  
  connection = dbh->connection;
  if (connection == 0)
    {
      error (lexpos (), "no connection.");
      return NS_ERROR;
    }
  
  return flush_handle (dbh);

} /* ora_flush */



/* this is for the AOLserver extended table info stuff.  Mostly it is
   useful for the /NS/Db pages 
*/

static Ns_DbTableInfo *
ora_get_table_info (Ns_DbHandle *dbh, char *table)
{
#define SQL_BUFFER_SIZE 1024
  oci_status_t oci_status;
  ora_connection_t *connection;
  char sql[SQL_BUFFER_SIZE];
  OCIStmt *stmt;
  Ns_DbTableInfo *tinfo;
  Ns_Set *cinfo;
  int i;
  sb4 n_columns;
  
  log (lexpos (), "entry (dbh %p, table %s)", dbh, nilp (table));
  
  if (! dbh || !table)
    {
      error (lexpos (), "invalid args.");
      return 0;
    }
  
  connection = dbh->connection;
  if (! connection)
    {
      error (lexpos (), "no connection.");
      return 0;
    }
  
  snprintf (sql, SQL_BUFFER_SIZE, "select * from %s", table);
  
  tinfo = Ns_DbNewTableInfo (table);
  
  oci_status = OCIHandleAlloc (connection->env,
                               (oci_handle_t **) &stmt,
                               OCI_HTYPE_STMT,
                               0, NULL);
  if (oci_error_p (lexpos (), dbh, "OCIHandleAlloc", sql, oci_status))
    return 0;
  
  oci_status = OCIStmtPrepare (stmt,
                               connection->err,
                               sql, strlen (sql),
                               OCI_NTV_SYNTAX,
                               OCI_DEFAULT);
  if (oci_error_p (lexpos (), dbh, "OCIStmtPrepare", sql, oci_status))
    return 0;
  
  oci_status = OCIStmtExecute (connection->svc,
                               stmt,
                               connection->err,
                               0,
                               0, NULL, NULL,
                               OCI_DESCRIBE_ONLY);
  
  oci_status = OCIAttrGet (stmt,
                           OCI_HTYPE_STMT,
			   (oci_attribute_t *) &n_columns,
			   NULL,
                           OCI_ATTR_PARAM_COUNT,
			   connection->err);
  if (oci_error_p (lexpos (), dbh, "OCIAttrGet", sql, oci_status))
    return 0;
  
  log (lexpos (), "Starting columns");

  for (i = 0; i < n_columns; i ++)
    {
      OCIParam *param;

      char name[512];
      char *name1;
      sb4 name1_size;
      /* for formatting the int returns big enough for 64 bits */
#define SBUF_BUFFER_SIZE 24
      char sbuf[SBUF_BUFFER_SIZE];
      ub2 size;
      ub2 precision;
      sb1 scale;
      OCITypeCode type;
      
      oci_status = OCIParamGet (stmt,
				OCI_HTYPE_STMT,
				connection->err,
				(oci_param_t *) &param, i + 1);
      if (oci_error_p (lexpos (), dbh, "OCIParamGet", sql, oci_status))
        return 0;
      
      oci_status = OCIAttrGet (param,
                               OCI_DTYPE_PARAM,
                               (oci_attribute_t *) &name1,
                               &name1_size,
                               OCI_ATTR_NAME,
                               connection->err);
      if (oci_error_p (lexpos (), dbh, "OCIAttrGet", sql, oci_status))
        return 0;
      
      log (lexpos(), "column name %s", name1);
      memcpy (name, name1, name1_size);
      name[name1_size] = 0;
      downcase (name);
      
      oci_status = OCIAttrGet (param,
			       OCI_DTYPE_PARAM,
			       (oci_attribute_t *) &type,
			       NULL,
			       OCI_ATTR_DATA_TYPE,
			       connection->err);
      if (oci_error_p (lexpos (), dbh, "OCIAttrGet", sql, oci_status))
        return 0;
      
      cinfo = Ns_SetCreate (name);
      switch (type)
	{
	case SQLT_DAT:
	  Ns_SetPut (cinfo, "type", "date");
          break;
	  
	case SQLT_NUM:
          log(lexpos(), "numeric type");
	  Ns_SetPut (cinfo, "type", "numeric");

          /* for numeric type we get precision and scale */
          /* The docs lie; they say the types for precision
             and scale are ub1 and sb1, but they seem to
             actually be ub2 and sb1, at least for Oracle 8.1.5. */
          oci_status = OCIAttrGet (param,
                                   OCI_DTYPE_PARAM,
                                   (dvoid *) &precision,
                                   NULL,
                                   OCI_ATTR_PRECISION,
                                   connection->err);
          if (oci_error_p (lexpos (), dbh, "OCIAttrGet", sql, oci_status))
            return 0;

          log(lexpos(), "precision %d", precision);
          snprintf(sbuf, SBUF_BUFFER_SIZE, "%d", (int)precision);
          Ns_SetPut (cinfo, "precision", sbuf);

          oci_status = OCIAttrGet (param,
                                   OCI_DTYPE_PARAM,
                                   (ub1 *) &scale,
                                   NULL,
                                   OCI_ATTR_SCALE,
                                   connection->err);
          if (oci_error_p (lexpos (), dbh, "OCIAttrGet", sql, oci_status))
            return 0;

          log(lexpos(), "scale %d", scale);
          snprintf(sbuf, SBUF_BUFFER_SIZE,"%d", (int)scale);
          Ns_SetPut (cinfo, "scale", sbuf);

	  break;
	  
	case SQLT_INT:
	  Ns_SetPut (cinfo, "type", "integer");
	  break;
	  
	case SQLT_FLT:
	  /* this is potentially bogus; right thing to do is add another OCI call
	     to find length and then see if it is real or double */
	  Ns_SetPut (cinfo, "type", "double");
	  break;
	  
        case SQLT_CLOB:
          Ns_SetPut (cinfo, "type", "text");
          Ns_SetPut (cinfo, "lobtype", "clob");
          break;

        case SQLT_BLOB:
          Ns_SetPut (cinfo, "type", "text");
          Ns_SetPut (cinfo, "lobtype", "blob");
          break;

	default:
	  Ns_SetPut (cinfo, "type", "text");
	  break;
        }

      log (lexpos(), "asking for size");

      /* Now lets ask for the size */
      oci_status = OCIAttrGet (param,
                               OCI_DTYPE_PARAM,
                               (oci_attribute_t *) &size,
                               NULL,
                               OCI_ATTR_DATA_SIZE,
                               connection->err);
      if (oci_error_p (lexpos (), dbh, "OCIAttrGet", sql, oci_status))
        return 0;

      snprintf(sbuf, SBUF_BUFFER_SIZE, "%d", size);
      Ns_SetPut (cinfo, "size", sbuf);

      Ns_DbAddColumnInfo (tinfo, cinfo);
    }
  
  oci_status = OCIHandleFree (stmt,
                              OCI_HTYPE_STMT);
  if (oci_error_p (lexpos (), dbh, "OCIHandleFree", sql, oci_status))
    return 0;
  
  return tinfo;

} /* ora_get_table_info */



/* poke around in Oracle and see what are all the possible tables */

static char *
ora_table_list (Ns_DString *pds, Ns_DbHandle *dbh, int system_tables_p)
{
  oci_status_t oci_status;
  ora_connection_t *connection;
  char *sql = 0;
  OCIStmt *stmt = NULL;
  
  OCIDefine *table_name_def;
  char table_name_buf[256];
  ub2 table_name_fetch_length;
  
  OCIDefine *owner_def;
  char owner_buf[256];
  ub2 owner_fetch_length;

  char *result = NULL;

  log (lexpos (), "entry (pds %p, dbh %p, system_tables_p %d)",
       pds, dbh, system_tables_p);
  
  log (lexpos (), "user: %s", nilp (dbh->user));
  
  if (! pds || !dbh)
    {
      error (lexpos (), "invalid args.");
      goto bailout;
    }
  
  connection = dbh->connection;
  if (! connection)
    {
      error (lexpos (), "no connection.");
      goto bailout;
    }
  
  sql = (system_tables_p
         ? "select table_name, owner from all_tables"
         : "select table_name from user_tables");
  
  oci_status = OCIHandleAlloc (connection->env,
                               (oci_handle_t **) &stmt,
                               OCI_HTYPE_STMT,
                               0, NULL);
  if (oci_error_p (lexpos (), dbh, "OCIHandleAlloc", sql, oci_status))
    {
      goto bailout;
    }
  
  oci_status = OCIStmtPrepare (stmt,
                               connection->err,
                               sql, strlen (sql),
                               OCI_NTV_SYNTAX,
                               OCI_DEFAULT);
  if (oci_error_p (lexpos (), dbh, "OCIStmtPrepare", sql, oci_status))
    {
      goto bailout;
    }

  
  oci_status = OCIDefineByPos (stmt,
                               &table_name_def,
                               connection->err,
                               1,
                               table_name_buf,
                               sizeof table_name_buf,
                               SQLT_STR,
                               NULL,
                               &table_name_fetch_length,
                               NULL,
                               OCI_DEFAULT);
  if (oci_error_p (lexpos (), dbh, "OCIDefineByPos", sql, oci_status))
    {
      goto bailout;
    }


  if (system_tables_p)
    {
      oci_status = OCIDefineByPos (stmt,
				   &owner_def,
				   connection->err,
				   2,
				   owner_buf,
				   sizeof owner_buf,
				   SQLT_STR,
				   NULL,
				   &owner_fetch_length,
				   NULL,
				   OCI_DEFAULT);
      if (oci_error_p (lexpos (), dbh, "OCIDefineByPos", sql, oci_status))
        {
	  goto bailout;
        }
    }
  
  oci_status = OCIStmtExecute (connection->svc,
                               stmt,
                               connection->err,
                               0,
                               0, NULL, NULL,
                               OCI_COMMIT_ON_SUCCESS);
  
  for (;;)
    {
      oci_status = OCIStmtFetch (stmt,
                                 connection->err,
                                 1,
                                 OCI_FETCH_NEXT,
                                 OCI_DEFAULT);
      if (oci_status == OCI_NO_DATA)
        break;
      else if (oci_error_p (lexpos (), dbh, "OCIStmtFetch", 0, oci_status))
        {
	  goto bailout;
        }

      
      if (system_tables_p)
	{
	  owner_buf[owner_fetch_length] = 0;
	  downcase (owner_buf);
	  
	  if (strcmp (owner_buf, dbh->user))
	    Ns_DStringNAppend (pds, owner_buf, owner_fetch_length);
	}
      
      table_name_buf[table_name_fetch_length] = 0;
      downcase (table_name_buf);
      
      Ns_DStringNAppend (pds, table_name_buf, table_name_fetch_length + 1);
      
      if (system_tables_p)
	log (lexpos (), "table: `%s.%s'", owner_buf, table_name_buf);
      else
	log (lexpos (), "table: `%s'", table_name_buf);
    }
  

  result = pds->string;

  bailout:

  if (stmt != NULL) 
    {
      oci_status = OCIHandleFree (stmt,
				  OCI_HTYPE_STMT);
      oci_error_p (lexpos (), dbh, "OCIHandleFree", sql, oci_status);
    }

  return result;

} /* ora_table_list */



/* ROWID is the always unique key for a row even when there is no
   primary key 
*/

#if !defined(NS_AOLSERVER_3_PLUS)

static char *
ora_best_row_id (Ns_DString *pds, Ns_DbHandle *dbh, char *table)
{
  log (lexpos (), "entry (pds %p, dbh %p, table %s", pds, dbh, nilp (table));
  
  Ns_DStringNAppend (pds, "rowid", 6);

  return pds->string;

} /* ora_best_row_id */

#endif



/* called by AOLserver when handle is returned by thread to pool; we
   must be careful to rollback any uncommitted work being done by a
   thread so that it doesn't get committed by another thread

   The AOLserver guys put this in for us because of the screw case
   where thread 1 deducts $1 million from the savings account then
   errs out before adding it to the checking ccount.  You don't want
   thread 2 picking up the same db conn and committing this
   half-finished transaction.

 */

static int
ora_reset_handle (Ns_DbHandle *dbh)
{
  oci_status_t oci_status;
  ora_connection_t *connection;
  
  log (lexpos (), "entry (dbh %p)", dbh);
  
  if (! dbh)
    {
      error (lexpos (), "invalid args.");
      return 0;
    }
  
  connection = dbh->connection;
  if (! connection)
    {
      error (lexpos (), "no connection.");
      return 0;
    }
  
  if (connection->mode == transaction)
    {
      oci_status = OCITransRollback (connection->svc,
                                     connection->err,
                                     OCI_DEFAULT);
      if (oci_error_p (lexpos (), dbh, "OCITransRollback", 0, oci_status))
        return NS_ERROR;
      
      connection->mode = autocommit;
    }
  
  return NS_OK;

} /* ora_reset_handle */



/* this is a function that we register as a callback with Oracle for
   DML statements that do RETURNING FOOBAR INTO ... (this was
   necessitated by the clob_dml statement which was necessitated by
   Oracle's stupid SQL parser that can't handle string literals longer
   than 4000 chars) 
*/

static sb4
no_data (dvoid *ctxp, OCIBind *bindp,
	 ub4 iter, ub4 index, dvoid **bufpp, ub4 *alenpp, ub1 *piecep, dvoid **indpp)
{
  log (lexpos (), "entry");
  
  *bufpp = (dvoid *) 0;
  *alenpp = 0;
  null_ind = -1;
  *indpp = (dvoid *) &null_ind;
  *piecep = OCI_ONE_PIECE;
  
  return OCI_CONTINUE;

} /* no_data */



/* another callback to register with Oracle */

static sb4
get_data (dvoid *ctxp, OCIBind *bindp,
	  ub4 iter, ub4 index, dvoid **bufpp, ub4 **alenp, ub1 *piecep, dvoid **indpp, ub2 **rcodepp)
{
  Ns_DbHandle *dbh;
  ora_connection_t *connection;
  fetch_buffer_t *buf;
  oci_status_t oci_status;
  int i;
  
  log (lexpos (), "entry (dbh %p; iter %d, index %d)", ctxp, iter, index);
  
  if (iter != 0)
    {
      error (lexpos (), "iter != 0");
      return NS_ERROR;
    }
  
  buf        = ctxp;
  connection = buf->connection;
  dbh        = connection->dbh;
  
  if (buf->lobs == 0)
    {
      oci_status = OCIAttrGet (bindp,
			       OCI_HTYPE_BIND,
			       (oci_attribute_t *) &buf->n_rows,
			       NULL,
			       OCI_ATTR_ROWS_RETURNED,
			       connection->err);
      if (oci_error_p (lexpos (), dbh, "OCIAttrGet", 0, oci_status))
	return NS_ERROR;
      
      log (lexpos (), "n_rows %d", buf->n_rows);
      
      buf->lobs = Ns_Malloc (buf->n_rows * sizeof *buf->lobs);

      for (i = 0; i < (int)buf->n_rows; i ++)
	buf->lobs[i] = 0;
      
      for (i = 0; i < (int)buf->n_rows; i ++)
	{
	  oci_status = OCIDescriptorAlloc (connection->env,
					   (oci_descriptor_t *) &buf->lobs[i],
					   OCI_DTYPE_LOB,
					   0,
					   0);
	  if (oci_error_p (lexpos (), dbh, "OCIDescriptorAlloc", 0, oci_status))
	    return NS_ERROR;
	}
    }
  
  *bufpp = (dvoid *) buf->lobs[index];
  
  *alenp = &rl;
  null_ind = -1;
  *indpp = (dvoid *) &null_ind;
  *piecep = OCI_ONE_PIECE;
  *rcodepp = &rc;
  
  return OCI_CONTINUE;

} /* get_data */


/* To support the [ns_ora 0or1row ...] and [ns_ora 1row ...] commands */
static Ns_Set *
ora_0or1row(Tcl_Interp *interp, Ns_DbHandle *handle, Ns_Set *row, int *nrows)
{
  log(lexpos(), "entry");
    if (row != NULL) {
        if (ora_get_row(handle, row) == NS_END_DATA) {
            *nrows = 0;
        } else {
	    switch (ora_get_row(handle, row)) {
		case NS_END_DATA:
		    *nrows = 1;
		    break;

		case NS_OK:
                    Ns_DbSetException(handle, "ORA",
			"Query returned more than one row.");
                    Tcl_SetResult(interp, handle->dsExceptionMsg.string,
                                  TCL_VOLATILE);
		    Ns_DbFlush(handle);
		    /* FALLTHROUGH */

		case NS_ERROR:
		    /* FALLTHROUGH */

		default:
		    return NULL;
		    break;
	    }
        }
        row = Ns_SetCopy(row);
    }

    return row;
}


/* this is the moby ns_ora Tcl command, it does resultrows, resultid,
   clob/blob_dml and clob/blob_dml_file, and clob/blob_get_file

   syntax:
      ns_ora dml db ?-bind set? query ?bindval1? ?bindval2? ...
      ns_ora array_dml db ?-bind set? query ?bindarray1? ?bindarray2? ...
      ns_ora clob_dml db query clob_value_1 ... clob_value_N
      ns_ora blob_dml db query clob_value_1 ... clob_value_N
      ns_ora clob_dml_file db query file_name_1 ... file_name_N
      ns_ora blob_dml_file db query file_name_1 ... file_name_N
      ns_ora clob_get_file db query file_name
      ns_ora blob_get_file db query file_name
      ns_ora write_clob db query ?nbytes?
      ns_ora write_blob db query ?nbytes?
   The query for the clob/blob_get_file should return just one value
 */

static int
ora_tcl_command (ClientData dummy, Tcl_Interp *interp, int argc, char *argv[])
{
  Ns_DbHandle *dbh;
  ora_connection_t *connection;
  oci_status_t oci_status;
  /* this is used for the [ns_ora resultrows ..] call */
#define BUFFER_SIZE 1024
  char buf[BUFFER_SIZE];
  
  if (argc < 3)
    {
      Tcl_AppendResult (interp, "wrong number of args: should be `", argv[0], 
			" command dbId ...'", NULL);
      return TCL_ERROR;
    }
  
  if (Ns_TclDbGetHandle (interp, argv[2], &dbh) != TCL_OK)
    return TCL_ERROR;
  
  if (Ns_DbDriverName (dbh) != ora_driver_name)
    {
      Tcl_AppendResult (interp, "handle `", argv[1], "' is not of type `", ora_driver_name, "'", NULL);
      return TCL_ERROR;
    }
  
  log (lexpos (), "entry (dbh %p)", dbh);
  
  connection = dbh->connection;
  if (! connection)
    {
      Tcl_AppendResult (interp,  "error: no connection", NULL);
      return TCL_ERROR;
    }
  
  if (! strcmp (argv[1], "resultid"))
    {
      OCIRowid *rowid = 0;
      sb4 rowid_size = 36;
      
      Tcl_AppendResult (interp, "resultid: not quite finished", NULL);
      return TCL_ERROR;

      if (connection->stmt == 0)
	{
	  /* no active stmt */
	  Tcl_AppendResult (interp, "resultid: no active statement", NULL);
	  return TCL_ERROR;
	}

      oci_status = OCIDescriptorAlloc (connection->env,
				       (oci_descriptor_t *) &rowid,
				       OCI_DTYPE_ROWID,
				       0,
				       0);
      if (oci_error_p (lexpos (), dbh, "OCIDescriptorAlloc", 0, oci_status))
	{
	  flush_handle (dbh);
	  return 0;
	}
      
      oci_status = OCIAttrGet (connection->stmt,
			       OCI_HTYPE_STMT,
			       (oci_attribute_t *) &rowid,
			       &rowid_size,
			       OCI_ATTR_ROWID,
			       connection->err);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIAttrGet", 0, oci_status))
	{
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
      
      log (lexpos (), "rowid: %p %s %d", rowid, nilp ((char *) rowid), rowid_size);
      
      Tcl_AppendResult (interp, "resultid: not quite finished", NULL);
      return TCL_ERROR;
    }
  else if (! strcmp (argv[1], "resultrows"))
    {
      ub4 count;
      
      if (argc != 3)
	{
	  Tcl_AppendResult (interp, "wrong number of args: should be `", argv[0], " resultrows dbId'", NULL);
	  return TCL_ERROR;
	}
      
      if (connection->stmt == 0)
	{
	  /* no active stmt */
	  Tcl_AppendResult (interp, "no active statement", NULL);
	  return TCL_ERROR;
	}
      
      oci_status = OCIAttrGet (connection->stmt,
			       OCI_HTYPE_STMT,
			       (oci_attribute_t *) &count,
			       NULL,
			       OCI_ATTR_ROW_COUNT,
			       connection->err);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIAttrGet", 0, oci_status))
	{
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
      
      snprintf (buf, BUFFER_SIZE, "%ld", (long) count);
      Tcl_AppendResult (interp, buf, NULL);
    }
  else if (   ! strcmp (argv[1], "clob_get_file")
	   || ! strcmp (argv[1], "blob_get_file")
	   || ! strcmp (argv[1], "write_clob")
	   || ! strcmp (argv[1], "write_blob"))
    {
      char *query;
      OCILobLocator *lob = NULL;
      OCIDefine *def;
      char *filename = NULL;
      int blob_p = NS_FALSE;
      int to_conn_p = NS_FALSE;
      int nbytes = INT_MAX;
      int result = TCL_ERROR;
      int write_lob_status = NS_ERROR;

      if ( ! strncmp (argv[1], "write", 5))
        to_conn_p = NS_TRUE;

      if (to_conn_p) 
        {
	    if (argc < 4 || argc > 5) 
	      {
		Tcl_AppendResult (interp, "wrong number of args: should be '",
				  argv[0], argv[1], " dbId query ?nbytes?", NULL);
		goto write_lob_cleanup;
	      }

	    if (argc == 5) 
	      {
		if (Tcl_GetInt(interp, argv[4], &nbytes) != TCL_OK) 
		  {
		    goto write_lob_cleanup;
		  }
	      }
        } 
      else 
        {
	  if (argc != 5) 
	   {
	      Tcl_AppendResult (interp, "wrong number of args: should be '",
				argv[0], argv[1], " dbId query filename", NULL);
	      goto write_lob_cleanup;
	    }
        }

      if (   ! strncmp (argv[1], "blob", 4)
	  || ! strcmp (argv[1], "write_blob"))
	  blob_p = NS_TRUE;

      query = argv[3];

      if (!allow_sql_p(dbh, query, NS_TRUE))
        {
	  Tcl_AppendResult (interp, "SQL ", query, " has been rejected "
			    "by the Oracle driver", NULL);
	  goto write_lob_cleanup;
        }
  
      if (dbh->verbose) 
	Ns_Log (Notice, "SQL():  %s", query);
      
      oci_status = OCIDescriptorAlloc (connection->env,
				       (dvoid **) &lob,
				       OCI_DTYPE_LOB,
				       0,
				       0);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIDescriptorAlloc", 
		       query, oci_status))
	{
	  goto write_lob_cleanup;
	}

      oci_status = OCIHandleAlloc (connection->env,
				   (oci_handle_t **) &connection->stmt,
				   OCI_HTYPE_STMT,
				   0, NULL);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIHandleAlloc", query, oci_status))
	{
	  goto write_lob_cleanup;
	}
      

      oci_status = OCIStmtPrepare (connection->stmt,
				   connection->err,
				   query, strlen (query),
				   OCI_NTV_SYNTAX,
				   OCI_DEFAULT);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIStmtPrepare", query, oci_status))
	{
	  goto write_lob_cleanup;
	}

      oci_status = OCIDefineByPos (connection->stmt,
				   &def,
				   connection->err,
				   1,
				   &lob,
				   -1,
				   (blob_p) ? SQLT_BLOB : SQLT_CLOB,
				   0,
				   0,
				   0,
				   OCI_DEFAULT);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIStmtPrepare",
		       query, oci_status))
	{
	  goto write_lob_cleanup;
	}


      oci_status = OCIStmtExecute (connection->svc,
				   connection->stmt,
				   connection->err,
				   1,
				   0,
				   0,
				   0,
				   OCI_DEFAULT);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIStmtExecute",
		       query, oci_status))
	{
	  goto write_lob_cleanup;
	}

      if (!to_conn_p) 
        {
	  filename = argv[4];
        }

      write_lob_status = stream_write_lob (interp, dbh, 0, lob, filename, to_conn_p,
					   connection->svc, connection->err);
      if (write_lob_status == STREAM_WRITE_LOB_ERROR) 
        {
	  tcl_error_p (lexpos (), interp, dbh, "stream_write_lob",
		       query, oci_status);
	  goto write_lob_cleanup;
        }

      /* if we survived to here, we're golden */
      result = TCL_OK;

      write_lob_cleanup:

      if (lob != NULL) 
      {
	  oci_status = OCIDescriptorFree (lob, 
					  OCI_DTYPE_LOB);
	  oci_error_p (lexpos (), dbh, "OCIDescriptorFree", 0, oci_status);
      }

      flush_handle (dbh);

      /* this is a hack.  If we don't drain a multi-part LOB, we'll get
       * an error next time we use the handle.  This works around the problem
       * for now until we find a better cleanup mechanism
       */
      if (write_lob_status != NS_OK) 
        {
	  ora_close_db (dbh);
	  ora_open_db (dbh);
        }
      
      return result;
    }
  else if (   ! strcmp (argv[1], "clob_dml")
	   || ! strcmp (argv[1], "clob_dml_file")
	   || ! strcmp (argv[1], "blob_dml")
	   || ! strcmp (argv[1], "blob_dml_file"))
    {
      return lob_dml_cmd(interp, argc, argv, dbh, connection);
    }
  else if (   ! strcmp (argv[1], "clob_dml_bind")
	   || ! strcmp (argv[1], "clob_dml_file_bind")
	   || ! strcmp (argv[1], "blob_dml_bind")
	   || ! strcmp (argv[1], "blob_dml_file_bind"))
    {
      return lob_dml_bind_cmd(interp, argc, argv, dbh, connection);
    }
  else if (! strcmp (argv[1], "exec_plsql"))
    {
      char *query;
      char *buf;
      OCIBind *bind;
      
      if (argc != 4)
	{
	  Tcl_AppendResult (interp, "wrong number of args: should be `",
			    argv[0], " exec_plsql dbId sql'", NULL);
	  return TCL_ERROR;
	}
      
      flush_handle (dbh);
      
      query = argv[3];
      
      if (!allow_sql_p(dbh, query, NS_TRUE))
        {
	  Tcl_AppendResult (interp, "SQL ", query, " has been rejected "
			    "by the Oracle driver", NULL);
	  return TCL_ERROR;
        }

      if (dbh->verbose) 
	Ns_Log (Notice, "SQL():  %s", query);
      
      oci_status = OCIHandleAlloc (connection->env,
				   (oci_handle_t **) &connection->stmt,
				   OCI_HTYPE_STMT,
				   0, NULL);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIHandleAlloc", query, oci_status))
	{
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
      
      oci_status = OCIStmtPrepare (connection->stmt,
				   connection->err,
				   query, strlen (query),
				   OCI_NTV_SYNTAX,
				   OCI_DEFAULT);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIStmtPrepare", query, oci_status))
	{
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
      
      buf = Ns_Malloc(EXEC_PLSQL_BUFFER_SIZE);

      oci_status = OCIBindByPos (connection->stmt,
				 &bind,
				 connection->err,
				 1,
				 buf,
				 EXEC_PLSQL_BUFFER_SIZE,
				 SQLT_STR,
				 0,
				 0,
				 0,
				 0,
				 0,
				 OCI_DEFAULT);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIBindByPos", query, oci_status))
	{
	  flush_handle (dbh);
          Ns_Free(buf);
	  return TCL_ERROR;
	}
      
      oci_status = OCIStmtExecute (connection->svc,
				   connection->stmt,
				   connection->err,
				   1,
				   0, NULL, NULL,
				   (connection->mode == autocommit
				    ? OCI_COMMIT_ON_SUCCESS
				    : OCI_DEFAULT));
      if (tcl_error_p (lexpos (), interp, dbh, "OCIStmtExecute", query, oci_status))
	{
	  flush_handle (dbh);
          Ns_Free(buf);
	  return TCL_ERROR;
	}
      
      Tcl_AppendResult (interp, buf, NULL);
      Ns_Free(buf);
    }
  else if (! strcmp (argv[1], "exec_plsql_bind"))
    {
      char *query;
      string_list_elt_t *bind_variables, *var_p;
      int argv_base, i;
      char *retvar, *retbuf;
      char *nbuf;
      
      if (argc < 5)
	{
	  Tcl_AppendResult (interp, "wrong number of args: should be `",
			    argv[0], " exec_plsql dbId sql retvar <args>'", NULL);
	  return TCL_ERROR;
	}
      
      flush_handle (dbh);
      
      query = argv[3];
      
      if (!allow_sql_p(dbh, query, NS_TRUE))
        {
	  Tcl_AppendResult (interp, "SQL ", query, " has been rejected "
			    "by the Oracle driver", NULL);
	  return TCL_ERROR;
        }

      if (dbh->verbose) 
	Ns_Log (Notice, "SQL():  %s", query);
      
      retvar = argv[4];

      oci_status = OCIHandleAlloc (connection->env,
				   (oci_handle_t **) &connection->stmt,
				   OCI_HTYPE_STMT,
				   0, NULL);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIHandleAlloc", query, oci_status))
	{
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
      
      oci_status = OCIStmtPrepare (connection->stmt,
				   connection->err,
				   query, strlen (query),
				   OCI_NTV_SYNTAX,
				   OCI_DEFAULT);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIStmtPrepare", query, oci_status))
	{
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
      
      argv_base = 4;

      bind_variables = parse_bind_variables(query);

      connection->n_columns = string_list_len(bind_variables);
      
      log(lexpos(), "%d bind variables", connection->n_columns);

      malloc_fetch_buffers (connection);

      retbuf = NULL;

      for (var_p = bind_variables, i=0; var_p != NULL; var_p = var_p->next, i++)
        {
          fetch_buffer_t *fetchbuf = &connection->fetch_buffers[i];
	  char *value = NULL;
          int index;

          fetchbuf->type = -1;
          index = strtol(var_p->string, &nbuf, 10);
          if (*nbuf == '\0')
            {
              /* It was a valid number.
                 Pick out one of the remaining arguments,
                 where ":1" is the first remaining arg. */
              if ((index < 1) || (index > (argc - argv_base - 1)))
                {
                  if (index < 1)
                    {
                      Tcl_AppendResult (interp, "invalid positional variable `:",
                                        var_p->string, "', valid values start with 1", NULL);
                    }
                  else
                    {
                      Tcl_AppendResult (interp, "not enough arguments for positional variable ':",
                                        var_p->string, "'", NULL);
                    }
                  flush_handle(dbh);
                  string_list_free_list(bind_variables);
                  return TCL_ERROR;
                }
              value = argv[argv_base + index];
            }
          else
            {
              value = Tcl_GetVar(interp, var_p->string, 0);
              if (value == NULL)
                {
                  if (strcmp(var_p->string, retvar) == 0)
                    {
                      /* It's OK if it's undefined,
                         since this is the return variable.
                      */
                      value = "";
                    }
                  else
                    {
                    
                      Tcl_AppendResult (interp, "undefined variable `", var_p->string,
                                        "'", NULL);
                      flush_handle(dbh);
                      string_list_free_list(bind_variables);
                      return TCL_ERROR;
                    }
                }
            }

          if (strcmp(var_p->string, retvar) == 0)
            {
              /* This is the variable we're going to return
                 as the result.
              */
              retbuf = fetchbuf->buf = Ns_Malloc(EXEC_PLSQL_BUFFER_SIZE);
              memset(retbuf, (int)'\0', (size_t)EXEC_PLSQL_BUFFER_SIZE);
              strncpy(retbuf, value, EXEC_PLSQL_BUFFER_SIZE);
              fetchbuf->fetch_length = EXEC_PLSQL_BUFFER_SIZE;
              fetchbuf->is_null = 0;

            }
          else
            {
              fetchbuf->buf = Ns_StrDup(value);
              fetchbuf->fetch_length = strlen(fetchbuf->buf) + 1;
              fetchbuf->is_null = 0;
            }


          if (dbh->verbose)
            Ns_Log(Notice, "bind variable '%s' = '%s'", var_p->string, value);

          log(lexpos(), "ns_ora exec_plsql_bind:  binding variable %s", var_p->string);

          oci_status = OCIBindByName(connection->stmt,
                                     &fetchbuf->bind,
                                     connection->err,
                                     var_p->string,
                                     strlen(var_p->string),
                                     fetchbuf->buf,
                                     fetchbuf->fetch_length,
                                     SQLT_STR,
                                     &fetchbuf->is_null,
                                     0,
                                     0,
                                     0,
                                     0,
                                     OCI_DEFAULT);

	  if (oci_error_p (lexpos (), dbh, "OCIBindByName", query, oci_status))
	    {
              Tcl_SetResult(interp, dbh->dsExceptionMsg.string, TCL_VOLATILE);
	      flush_handle (dbh);
              string_list_free_list(bind_variables);
	      return TCL_ERROR;
	    }

	}

      if (retbuf == NULL)
        {
          Tcl_AppendResult(interp, "return variable '", retvar, "' not found in statement bind variables", NULL);
          flush_handle (dbh);
          string_list_free_list(bind_variables);
          return TCL_ERROR;
        }

      oci_status = OCIStmtExecute (connection->svc,
				   connection->stmt,
				   connection->err,
				   1,
				   0, NULL, NULL,
				   (connection->mode == autocommit
				    ? OCI_COMMIT_ON_SUCCESS
				    : OCI_DEFAULT));

      string_list_free_list(bind_variables);

      if (tcl_error_p (lexpos (), interp, dbh, "OCIStmtExecute", query, oci_status))
	{
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
      
      Tcl_AppendResult (interp, retbuf, NULL);
      
      /* Check to see if return variable was a Tcl variable */
      
      (void) strtol(retvar, &nbuf, 10);
      if (*nbuf != '\0')
        {
          /* It was a variable name. */
          Tcl_SetVar(interp, retvar, retbuf, 0);
        }
    }
  else if ( ! strcmp (argv[1], "dml") ||
	    ! strcmp (argv[1], "array_dml") ||
            ! strcmp (argv[1], "select") ||
            ! strcmp (argv[1], "1row") ||
            ! strcmp (argv[1], "0or1row"))
    {
      char *query;
      int i, j;
      string_list_elt_t *bind_variables, *var_p;
      ub4 iters;
      ub2 type;
      int dml_p;
      int array_p;        /* Array DML? */
      int argv_base;      /* Index of the SQL statement argument (necessary to support -bind) */
      Ns_Set *set = NULL; /* If we're binding to an ns_set, a pointer to the struct */
      
      if ( ! strcmp(argv[1], "dml") ) {
	  dml_p = 1;
	  array_p = 0;
      } else if ( ! strcmp(argv[1], "array_dml") ) {
	  dml_p = 1;
	  array_p = 1;
      } else {
	  dml_p = 0;
	  array_p = 0;
      }

      if (argc < 4 || (!strcmp("-bind", argv[3]) && argc < 6))
	{
	  Tcl_AppendResult (interp, "wrong number of args: should be `",
			    argv[0], argv[1], "dbId ?-bind set? query ?bindvalue1? ?bindvalue2? ...", NULL);
	  return TCL_ERROR;
	}

      if (!strcmp("-bind", argv[3])) {
	  /* Binding to a set. The query is argv[5]. */
	  argv_base = 5;
	  set = Ns_TclGetSet(interp, argv[4]);
	  if (set == NULL) {
	      Tcl_AppendResult (interp, "invalid set id `", argv[4], "'", NULL);
	      return TCL_ERROR;	      
	  }
      } else {
	  /* Not binding to a set. The query is argv[3]. */
	  argv_base = 3;
      }

      flush_handle (dbh);

      query = argv[argv_base];

      if (!allow_sql_p(dbh, query, NS_TRUE))
        {
	  Tcl_AppendResult (interp, "SQL ", query, " has been rejected "
			    "by the Oracle driver", NULL);
	  return TCL_ERROR;
        }

      if (dbh->verbose) 
	Ns_Log (Notice, "SQL():  %s", query);
  
      /* handle_builtins will flush the handles on a ERROR exit */

      switch (handle_builtins (dbh, query))
        {
        case NS_DML:
          /* handled */
          return TCL_OK;
      
        case NS_ERROR:
          Tcl_SetResult(interp, dbh->dsExceptionMsg.string, TCL_VOLATILE);
          return TCL_ERROR;
      
        case NS_OK:
          break;
      
        default:
          error (lexpos (), "internal error");
          Tcl_AppendResult (interp, "internal error", NULL);
          return TCL_ERROR;
        }

      oci_status = OCIHandleAlloc (connection->env,
				   (oci_handle_t **) &connection->stmt,
				   OCI_HTYPE_STMT,
				   0, NULL);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIHandleAlloc", query, oci_status))
	{
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
      
      oci_status = OCIStmtPrepare (connection->stmt,
				   connection->err,
				   query, strlen (query),
				   OCI_NTV_SYNTAX,
				   OCI_DEFAULT);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIStmtPrepare", query, oci_status))
	{
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
      
      /* check what type of statment it is, this will affect how
         many times we expect to execute it */
      oci_status = OCIAttrGet (connection->stmt,
                               OCI_HTYPE_STMT,
                               (oci_attribute_t *) &type,
                               NULL,
                               OCI_ATTR_STMT_TYPE,
                               connection->err);
      if (oci_error_p (lexpos (), dbh, "OCIAttrGet", query, oci_status))
        {
          /* got error asking Oracle for the statement type */
          Tcl_SetResult(interp, dbh->dsExceptionMsg.string, TCL_VOLATILE);
          flush_handle (dbh);
          return NS_ERROR;
        }
      if (type == OCI_STMT_SELECT) {
        iters = 0;
        if (prefetch_rows > 0) { 
            /* Set prefetch rows attr for selects... */
            oci_status = OCIAttrSet (connection->stmt,
                                     OCI_HTYPE_STMT,
                                     (dvoid *) &prefetch_rows,
                                     0,
                                     OCI_ATTR_PREFETCH_ROWS,
                                     connection->err);
            if (oci_error_p (lexpos (), dbh, "OCIAttrSet", query, oci_status))
            {
                Tcl_SetResult(interp, dbh->dsExceptionMsg.string, TCL_VOLATILE);
                flush_handle (dbh);
                return NS_ERROR;
            }
        }
        if (prefetch_memory > 0) { 
            /* Set prefetch rows attr for selects... */
            oci_status = OCIAttrSet (connection->stmt,
                                     OCI_HTYPE_STMT,
                                     (dvoid *) &prefetch_memory,
                                     0,
                                     OCI_ATTR_PREFETCH_MEMORY,
                                     connection->err);
            if (oci_error_p (lexpos (), dbh, "OCIAttrSet", query, oci_status))
            {
                Tcl_SetResult(interp, dbh->dsExceptionMsg.string, TCL_VOLATILE);
                flush_handle (dbh);
                return NS_ERROR;
            }
        }
      } else {
        iters = 1;
      }
      
      /* Check for statement type mismatch */

      if (type != OCI_STMT_SELECT && ! dml_p) {
        Ns_DbSetException(dbh, "ORA",
                          "Query was not a statement returning rows.");
        Tcl_SetResult(interp, dbh->dsExceptionMsg.string, TCL_VOLATILE);
        flush_handle(dbh);
        return TCL_ERROR;
      } else if (type == OCI_STMT_SELECT && dml_p) {
        Ns_DbSetException(dbh, "ORA",
                          "Query was not a DML statement.");
        Tcl_SetResult(interp, dbh->dsExceptionMsg.string, TCL_VOLATILE);
        flush_handle(dbh);
        return TCL_ERROR;
      }

      bind_variables = parse_bind_variables(query);

      connection->n_columns = string_list_len(bind_variables);
      
      log(lexpos(), "%d bind variables", connection->n_columns);

      if (connection->n_columns > 0)
        malloc_fetch_buffers (connection);
      
      for (var_p = bind_variables, i=0; var_p != NULL; var_p = var_p->next, i++)
        {
          fetch_buffer_t *fetchbuf = &connection->fetch_buffers[i];
          char *nbuf;
	  char *value = NULL;
          int index;
	  int max_length = 0;

          fetchbuf->type = -1;
          index = strtol(var_p->string, &nbuf, 10);
          if (*nbuf == '\0')
            {
              /* It was a valid number.
                 Pick out one of the remaining arguments,
                 where ":1" is the first remaining arg. */
              if ((index < 1) || (index > (argc - argv_base - 1)))
                {
                  if (index < 1)
                    {
                      Tcl_AppendResult (interp, "invalid positional variable `:",
                                        var_p->string, "', valid values start with 1", NULL);
                    }
                  else
                    {
                      Tcl_AppendResult (interp, "not enough arguments for positional variable ':",
                                        var_p->string, "'", NULL);
                    }
                  flush_handle(dbh);
                  string_list_free_list(bind_variables);
                  return TCL_ERROR;
                }
              value = argv[index + argv_base];
            }
          else
            {
		if (set == NULL) {
		    value = Tcl_GetVar(interp, var_p->string, 0);
		    if (value == NULL)
		    {
			Tcl_AppendResult (interp, "undefined variable `", var_p->string,
					  "'", NULL);
			flush_handle(dbh);
			string_list_free_list(bind_variables);
			return TCL_ERROR;
		    }
		} else {
		    value = Ns_SetGet(set, var_p->string);
		    if (value == NULL)
		    {
			Tcl_AppendResult (interp, "undefined set element `", var_p->string,
					  "'", NULL);
			flush_handle(dbh);
			string_list_free_list(bind_variables);
			return TCL_ERROR;
		    }
		}
            }

	  if (array_p) {
	      /* Populate the array list. First split the list into individual values. Note
		 that Tcl_SplitList returns a block which we need to free later! */
	      if ((Tcl_SplitList(interp, value,
				 &fetchbuf->array_count,
				 &fetchbuf->array_values)) != TCL_OK) {
                  flush_handle(dbh);
                  string_list_free_list(bind_variables);
                  return TCL_ERROR;
	      }

	      if (i == 0) {
		  /* First iteration - remember the number of items in the list. */
		  iters = fetchbuf->array_count;
	      } else {
		  if ((int)iters != fetchbuf->array_count) {
		      Tcl_AppendResult(interp, "non-matching numbers of rows", NULL);
		      flush_handle(dbh);
		      string_list_free_list(bind_variables);
		      return TCL_ERROR;
		  }
	      }

	      for (j = 0; j < (int)iters; ++j)
	      {
		  /* Find the maximum length of any item in the list. */
		  int len = strlen(fetchbuf->array_values[j]);
		  if (len > max_length)
		  {
		      max_length = len;
		  }
	      }
	  } else {
	      fetchbuf->buf = Ns_StrDup(value);
	      fetchbuf->fetch_length = strlen(fetchbuf->buf) + 1;
	      fetchbuf->is_null = 0;
	  }

          if (dbh->verbose)
            Ns_Log(Notice, "bind variable '%s' = '%s'", var_p->string, value);

          log(lexpos(), "ns_ora dml:  binding variable %s", var_p->string);

	  /* If array DML, use OCI_DATA_AT_EXEC (dynamic binding). Otherwise use
	     plain ol' OCI_DEFAULT. */
          oci_status = OCIBindByName(connection->stmt,
                                     &fetchbuf->bind,
                                     connection->err,
                                     var_p->string,
                                     strlen(var_p->string),
                                     array_p ? NULL : fetchbuf->buf,
                                     array_p ? max_length + 1 : fetchbuf->fetch_length,
                                     array_p ? SQLT_CHR : SQLT_STR,
                                     array_p ? NULL : &fetchbuf->is_null,
                                     0,
                                     0,
                                     0,
                                     0,
                                     array_p ? OCI_DATA_AT_EXEC : OCI_DEFAULT);

	  if (oci_error_p (lexpos (), dbh, "OCIBindByName", query, oci_status))
	    {
              Tcl_SetResult(interp, dbh->dsExceptionMsg.string, TCL_VOLATILE);
	      flush_handle (dbh);
              string_list_free_list(bind_variables);
	      return TCL_ERROR;
	    }

	  if (array_p) {
	      /* Array DML - dynamically bind, using list_element_put_data (which will
		 return the right item for each iteration). */
	      oci_status = OCIBindDynamic (fetchbuf->bind,
					   connection->err,
					   fetchbuf, list_element_put_data,
					   fetchbuf, get_data);
	      if (tcl_error_p (lexpos (), interp, dbh, "OCIBindDynamic", query, oci_status))
	      {
		  flush_handle (dbh);
		  string_list_free_list(bind_variables);
		  return TCL_ERROR;
	      }
	  }
	}

      log(lexpos(), "ns_ora dml:  executing statement %s", nilp(query));

      oci_status = OCIStmtExecute (connection->svc,
				   connection->stmt,
				   connection->err,
				   iters,
				   0, NULL, NULL,
				   OCI_DEFAULT);

      string_list_free_list(bind_variables);
      if (connection->n_columns > 0) {
        for (i=0; i<connection->n_columns; i++) {
          Ns_Free(connection->fetch_buffers[i].buf);
	  connection->fetch_buffers[i].buf = NULL;
          Ns_Free(connection->fetch_buffers[i].array_values);
	  if (connection->fetch_buffers[i].array_values != 0) {
	      log(lexpos(), "*** Freeing buffer %p", connection->fetch_buffers[i].array_values);
	  }
	  connection->fetch_buffers[i].array_values = NULL;
        }
        Ns_Free (connection->fetch_buffers);
        connection->fetch_buffers = 0;
      }

      if (oci_error_p (lexpos (), dbh, "OCIStmtExecute", query, oci_status))
	{
          Tcl_SetResult(interp, dbh->dsExceptionMsg.string, TCL_VOLATILE);
	  flush_handle (dbh);
	  return TCL_ERROR;
	}

      
      if (dml_p) {
        if (connection->mode == autocommit)
          {
            oci_status = OCITransCommit (connection->svc,
                                         connection->err,
                                         OCI_DEFAULT);
            if (oci_error_p (lexpos (), dbh, "OCITransCommit", query, oci_status))
              {
                Tcl_SetResult(interp, dbh->dsExceptionMsg.string,
                              TCL_VOLATILE);
                flush_handle (dbh);
                return TCL_ERROR;
              }
          }
      } else /* !dml_p */ {
        Ns_Set *setPtr;
        int dynamic_p = 0;

        log(lexpos(), "ns_ora dml:  doing bind for select");
        Ns_SetTrunc(dbh->row, 0);
        setPtr = ora_bindrow(dbh);

        if (!strcmp(argv[1], "1row") ||
            !strcmp(argv[1], "0or1row")) {
          Ns_Set *row;
          int nrows;

          row = ora_0or1row(interp, dbh, setPtr, &nrows);
          dynamic_p = 1;

          if (row != NULL) {
            if (!strcmp(argv[1], "1row") && nrows != 1)
              {
                Ns_DbSetException(dbh, "ORA",
                                  "Query did not return a row.");
                /* XXX doesn't this leak a row? */
                row = NULL;
                Tcl_SetResult(interp, dbh->dsExceptionMsg.string,
                              TCL_VOLATILE);
              } 
          }
          if (row == NULL) 
            {
              error (lexpos(), "Database operation \"%s\" failed", argv[0]);
              flush_handle(dbh);
              return TCL_ERROR;
            }
          if (nrows == 0) 
            {
              Ns_SetFree(row);
              row = NULL;
            }
          setPtr = row;
        }

        if (setPtr != NULL)
          Ns_TclEnterSet(interp, setPtr, dynamic_p);
      }
    }
  else
    {
      Tcl_AppendResult (interp, "unknown command `", argv[1], "': should be "
			"dml, array_dml, "
			"resultid, resultrows, clob_dml, clob_dml_file, "
			"clob_get_file, blob_dml, blob_dml_file, "
			"blob_get_file, dml, select, 1row, "
                        "0or1row, or exec_plsql.",
			NULL);
      return TCL_ERROR;
    }

  return TCL_OK;

} /* ora_tcl_cmd */



/* called once/spawned Tcl interpreter; sets up ns_ora */

static int
ora_interp_init (Tcl_Interp *interp, void *dummy)
{
  Tcl_CreateCommand (interp, "ns_ora", ora_tcl_command, NULL, NULL);
  
#if defined(NS_AOLSERVER_3_PLUS)
  Tcl_CreateCommand (interp, "ns_column", ora_column_command, NULL, NULL);
  Tcl_CreateCommand (interp, "ns_table", ora_table_command, NULL, NULL);
#endif
  
  return NS_OK;

} /* ora_interp_init */



/* another entry point; AOLserver calls this once, presumably at
   driver loading time.  It sets up a callback to ora_interp_init
   which will be called for every Tcl interpreter that gets spawned 
*/

static int
ora_server_init (char *hserver, char *hmodule, char *hdriver)
{
  log (lexpos (), "entry (%s, %s, %s)", nilp (hserver), nilp (hmodule), nilp (hdriver));
  
  return Ns_TclInitInterps (hserver, ora_interp_init, NULL);
} /* ora_server_init */



/* read a file from the operating system and then stuff it into the lob
   This was cargo-culted from an example in the OCI programmer's
   guide.
 */
static int
stream_read_lob (Tcl_Interp *interp, Ns_DbHandle *dbh, int rowind, 
		 OCILobLocator *lobl, char *path, 
		 ora_connection_t *connection)
{
  ub4   offset = 1;
  ub4   loblen = 0;
  ub1   *bufp = NULL;
  ub4   amtp;
  ub1   piece;
  ub4   nbytes;
  ub4   remainder;
  ub4 filelen = 0;
#ifdef WIN32
  int readlen;
#else
  ssize_t readlen;
#endif
  int status = NS_ERROR;
  oci_status_t oci_status = OCI_SUCCESS;
  struct stat statbuf;

  int fd = -1;

  fd = open (path, O_RDONLY | EXTRA_OPEN_FLAGS);

  if (fd == -1) 
    {
      Ns_Log (Error, "%s:%d:%s Error opening file %s: %d(%s)",
	      lexpos(), path, errno, strerror(errno));
      Tcl_AppendResult (interp, "can't open file ", path, " for reading. ",
			"received error ", strerror(errno), NULL);
      goto bailout;
    }
  
  if (stat(path, &statbuf) == -1) 
    {
      Ns_Log (Error, "%s:%d:%s Error statting %s: %d(%s)",
	      lexpos(), path, errno, strerror(errno));
	      Tcl_AppendResult (interp, "can't stat ", path, ". ",
				"received error ", strerror(errno), NULL);
      goto bailout;
    }
  filelen = statbuf.st_size;

  remainder = amtp = filelen;

  log (lexpos(), "to do streamed write lob, amount = %d", (int)filelen);

  oci_status = OCILobGetLength(connection->svc, connection->err, lobl, &loblen);

  if (tcl_error_p (lexpos (), interp, dbh, "OCILobGetLength", 0, oci_status))
    goto bailout;
  

  log (lexpos(), "before stream write, lob length is %d", (int)loblen);

  if (filelen > lob_buffer_size)
    nbytes = lob_buffer_size;
  else
    nbytes = filelen;

  bufp = (ub1 *)Ns_Malloc(lob_buffer_size);
  readlen = read (fd, bufp, nbytes);

  if (readlen < 0)
    {
      Ns_Log (Error, "%s:%d:%s Error reading file %s: %d(%s)",
	      lexpos(), path, errno, strerror(errno));
      Tcl_AppendResult (interp, "can't read ", path,
			" received error ", strerror(errno), NULL);
      goto bailout;
    }

  remainder -= readlen; 
  
  if (remainder == 0)       /* exactly one piece in the file */ 
    {
      if (readlen > 0) 	/* if no bytes, bypass the LobWrite to insert a NULL */
        {
	  log (lexpos(), "only one piece, no need for stream write");
	  oci_status = OCILobWrite (connection->svc,
				    connection->err,
				    lobl,
				    &amtp,
				    offset,
				    bufp,
				    readlen,
				    OCI_ONE_PIECE,
				    0, 
				    0,
				    0,
				    SQLCS_IMPLICIT);
	  if (tcl_error_p (lexpos (), interp, dbh, "OCILobWrite", 0, oci_status)) 
	    {
	      goto bailout;
	    }
        }
    }
  else                    /* more than one piece */
    {
      oci_status = OCILobWrite (connection->svc,
				connection->err,
				lobl,
				&amtp,
				offset,
				bufp,
				lob_buffer_size,
				OCI_FIRST_PIECE,
				0, 
				0,
				0,
				SQLCS_IMPLICIT);

      if (   oci_status != OCI_NEED_DATA
	  && tcl_error_p (lexpos (), interp, dbh, "OCILobWrite", 0, oci_status)) 
        {
	  goto bailout;
        }

      
      piece = OCI_NEXT_PIECE;
      
      do
        {
	  if (remainder > lob_buffer_size)
	    nbytes = lob_buffer_size;
	  else
	    {
	      nbytes = remainder;
	      piece = OCI_LAST_PIECE;
	    }
	  
	  readlen = read (fd, bufp, nbytes);
	  
	  if (readlen < 0)
	    {
	      Ns_Log (Error, "%s:%d:%s Error reading file %s: %d(%s)",
		      lexpos(), path, errno, strerror(errno));
	      Tcl_AppendResult (interp, "can't read ", path,
				" received error ", strerror(errno), NULL);
	      piece = OCI_LAST_PIECE;
	    }
	  
	  oci_status = OCILobWrite (connection->svc,
				    connection->err, 
				    lobl,
				    &amtp,
				    offset,
				    bufp,
				    readlen, 
				    piece, 
				    0, 
				    0,
				    0,
				    SQLCS_IMPLICIT);
	  if (   oci_status != OCI_NEED_DATA
	      && tcl_error_p (lexpos (), interp, dbh, "OCILobWrite", 0, oci_status)) 
	    {
	      goto bailout;
	    }
	  remainder -= readlen; 
	  
      } while (oci_status == OCI_NEED_DATA && remainder > 0);
    }

  if (tcl_error_p (lexpos (), interp, dbh, "OCILobWrite", 0, oci_status))
    {
      goto bailout;
    }

  status = NS_OK;

bailout:

  if(bufp)
    Ns_Free(bufp);
  close (fd);

  if (status != NS_OK && connection->mode == transaction) 
    {
      log (lexpos(), "error writing lob.  rolling back transaction");

      oci_status = OCITransRollback (connection->svc,
                                     connection->err,
                                     OCI_DEFAULT);
      tcl_error_p (lexpos (), interp, dbh, "OCITransRollback", 0, oci_status);
    }

  return status;

} /* stream_read_lob */



static int
stream_actually_write (int fd, Ns_Conn *conn, void *bufp, int length, int to_conn_p)
{
  int bytes_written = 0;

  log (lexpos (), "entry (%d, %d, %d)", fd, length, to_conn_p);

  if (to_conn_p)
    {
      if (Ns_WriteConn (conn, bufp, length) == NS_OK) 
        {
	  bytes_written = length;
        } 
      else 
        {
	  bytes_written = 0;
        }
    }
  else
    {
      bytes_written = write (fd, bufp, length);
    }

  log (lexpos (), "exit (%d, %d, %d)", bytes_written, fd, length, to_conn_p);

  return bytes_written;
  
} /* stream_actually_write */




/* snarf lobs using stream mode from Oracle into local buffers, then
   write them to the given file (replacing the file if it exists) or
   out to the connection.
   This was cargo-culted from an example in the OCI programmer's
   guide.
*/

static int
stream_write_lob (Tcl_Interp *interp, Ns_DbHandle *dbh, int rowind, 
		  OCILobLocator *lobl, char *path, int to_conn_p, 
		  OCISvcCtx *svchp, OCIError *errhp)
{
  ub4   offset = 1;
  ub4   loblen = 0;
  ub1   *bufp = NULL;
  ub4   amtp = 0;
  ub4   piece = 0;
  ub4   remainder;            /* the number of bytes for the last piece */
  int  fd = 0;
  int bytes_to_write, bytes_written;
  int status = STREAM_WRITE_LOB_ERROR;
  oci_status_t oci_status;
  Ns_Conn *conn = NULL;

  if (path == NULL) 
    {
	path = "to connection";
    }

  log (lexpos(), "entry (path %s)", path);

  if (to_conn_p) 
    {  
      conn = Ns_TclGetConn(interp);
      
      /* this Shouldn't Happen, but spew an error just in case */
      if (conn == NULL) 
        {
	  Ns_Log (Error, "%s:%d:%s: No AOLserver conn available",
		  lexpos());
	  Tcl_AppendResult (interp, "No AOLserver conn available", NULL);
	  goto bailout;
        }
    }
  else 
    {
      fd = open (path, O_CREAT | O_TRUNC | O_WRONLY | EXTRA_OPEN_FLAGS, 0600);
      
      if (fd < 0) 
        {
	  Ns_Log (Error, "%s:%d:%s: can't open %s for writing. error %d(%s)",
		  lexpos(), path, errno, strerror(errno));
	  Tcl_AppendResult (interp, "can't open file ", path, " for writing. ",
			    "received error ", strerror(errno), NULL);
	  goto bailout;
        }
    }

  oci_status = OCILobGetLength (svchp, 
				errhp, 
				lobl, 
				&loblen);
  if (tcl_error_p (lexpos (), interp, dbh, "OCILobGetLength", path, oci_status))
    goto bailout;

  if (loblen > 0)
    {
  
      amtp = loblen;

      log (lexpos(), "loblen %d", loblen);

      bufp = (ub1 *)Ns_Malloc(lob_buffer_size);
      memset((void *)bufp, (int)'\0', (size_t)lob_buffer_size);
      
      oci_status = OCILobRead (svchp, 
			   errhp, 
			   lobl,
			   &amtp, 
			   offset, 
			   bufp,
			   (loblen < lob_buffer_size ? loblen : lob_buffer_size), 
			   0, 
			   0,
			   0,
			   SQLCS_IMPLICIT);

      switch (oci_status)
      {
	case OCI_SUCCESS:             /* only one piece */
	  log (lexpos(), "stream read %d'th piece\n", (int)(++piece));

	  bytes_written = stream_actually_write (fd, conn, bufp, loblen, to_conn_p);

	  if (bytes_written != (int)loblen) 
	    {
	      if (errno == EPIPE) 
		{ 
		  status = STREAM_WRITE_LOB_PIPE; 
		  goto bailout; 
		} 
	      if (bytes_written < 0) 
		{
		  Ns_Log (Error, "%s:%d:%s error writing %s.  error %d(%s)",
			  lexpos(), path, errno, strerror(errno));
		  Tcl_AppendResult (interp, "can't write ", path, 
				    " received error ", strerror(errno), NULL);
		  goto bailout;
		}
	      else 
		{
		  Ns_Log (Error, "%s:%d:%s error writing %s.  incomplete write of %d out of %d",
			  lexpos(), path, bytes_written, loblen);
		  Tcl_AppendResult (interp, "can't write ", path,
				    " received error ", strerror(errno), NULL);
		  goto bailout;
		}
	    }
	  break;

	case OCI_ERROR:
	  break;

	case OCI_NEED_DATA:           /* there are 2 or more pieces */

	  remainder = loblen;
					      /* a full buffer to write */
	  bytes_written = stream_actually_write (fd, conn, bufp,lob_buffer_size, to_conn_p);

	  if (bytes_written != lob_buffer_size) 
	    {
	      if (errno == EPIPE) 
		{ 
		  status = STREAM_WRITE_LOB_PIPE; 
		  goto bailout; 
		} 
	      if (bytes_written < 0) 
		{
		  Ns_Log (Error, "%s:%d:%s error writing %s.  error %d(%s)",
			  lexpos(), path, errno, strerror(errno));
		  Tcl_AppendResult (interp, "can't write ", path,
				    " received error ", strerror(errno), NULL);
		  goto bailout;
		}
	      else 
		{
		  Ns_Log (Error, "%s:%d:%s error writing %s.  incomplete write of %d out of %d",
			  lexpos(), path, bytes_written, lob_buffer_size);
		  Tcl_AppendResult (interp, "can't write ", path,
				    " received error ", strerror(errno), NULL);
		  goto bailout;
		}
	    }

	  do
	    {
	      memset(bufp, '\0', lob_buffer_size);
	      amtp = 0;
	      
	      remainder -= lob_buffer_size;
	      
	      oci_status = OCILobRead (svchp,
				       errhp,
				       lobl,
				       &amtp,
				       offset,
				       bufp,
				       lob_buffer_size, 
				       0, 
				       0,
				       0, 
				       SQLCS_IMPLICIT);
	      if (   oci_status != OCI_NEED_DATA
		  && tcl_error_p (lexpos (), interp, dbh, "OCILobRead", 0, oci_status)) 
		{
		  goto bailout;
		}
	      
	      
	      /* the amount read returned is undefined for FIRST, NEXT pieces */
	      log (lexpos(), "stream read %d'th piece, atmp = %d",
		   (int)(++piece), (int)amtp);
	      
	      if (remainder < lob_buffer_size)
		{  /* last piece not a full buffer piece */
		    bytes_to_write = remainder;
		}
	      else 
		{
		    bytes_to_write = lob_buffer_size;
		}

	      bytes_written = stream_actually_write (fd, conn, bufp, bytes_to_write, to_conn_p);
	      
	      if (bytes_written != bytes_to_write) 
		{
		  if (errno == EPIPE)
		    {
		      /* broken pipe means the user hit the stop button.
		       * if that's the case, lie and say we've completed
		       * successfully so we don't cause false-positive errors
		       * in the server.log
		       * photo.net ticket # 5901
		       */
		      status = STREAM_WRITE_LOB_PIPE;
		    }
		  else 
		    {
		      if (bytes_written < 0) 
			{
			  Ns_Log (Error, "%s:%d:%s error writing %s.  error %d(%s)",
				  lexpos(), path, errno, strerror(errno));
			  Tcl_AppendResult (interp, "can't write ", path, " for writing. ",
					    " received error ", strerror(errno), NULL);
			}
		      else 
			{
			  Ns_Log (Error, "%s:%d:%s error writing %s.  incomplete write of %d out of %d",
				  lexpos(), path, bytes_written, bytes_to_write);
			  Tcl_AppendResult (interp, "can't write ", path, " for writing. ",
					    " received error ", strerror(errno), NULL);
			}
		    } 
		  goto bailout;
		}
	     } while (oci_status == OCI_NEED_DATA);
	  break;

	default:
	  Ns_Log (Error, "%s:%d:%s: Unexpected error from OCILobRead (%d)",
		  lexpos(), oci_status);
	  goto bailout;
	  break;
	}

      status = STREAM_WRITE_LOB_OK;

bailout:
      if (bufp)
	Ns_Free(bufp);
    }

  if (!to_conn_p)
    {
      close (fd);
    }

  return status;

} /* stream_write_lob */

/*--------------------------------------------------------------------*/

/*
 * lob_dml_bind_cmd handles these commands:
 *
 * ns_ora clob_dml_bind
 * ns_ora clob_dml_file_bind
 * ns_ora blob_dml_bind
 * ns_ora blob_dml_file_bind
 */

static int
lob_dml_bind_cmd(Tcl_Interp *interp, int argc, char *argv[],
    Ns_DbHandle *dbh, ora_connection_t *connection)
{
  oci_status_t oci_status;
  char *query;
  char **data;
  int i;
  int k;
  int files_p = NS_FALSE;
  int blob_p = NS_FALSE;
  string_list_elt_t *bind_variables, *var_p;
  char **lob_argv;
  int lob_argc;
  int argv_base;
  
  if (argc < 5)
    {
      Tcl_AppendResult (interp, "wrong number of args: should be `",
			argv[0], argv[1], "dbId query clobList ",
			"[clobValues | filenames] ...'",  NULL);
      return TCL_ERROR;
    }

  flush_handle (dbh);

  if (   ! strcmp (argv[1], "clob_dml_file_bind")
      || ! strcmp (argv[1], "blob_dml_file_bind"))
    files_p = NS_TRUE;

  if ( ! strncmp (argv[1], "blob", 4))
      blob_p = NS_TRUE;

  query = argv[3];

  if (!allow_sql_p(dbh, query, NS_TRUE))
    {
      Tcl_AppendResult (interp, "SQL ", query, " has been rejected "
			"by the Oracle driver", NULL);
      return TCL_ERROR;
    }

  if (dbh->verbose) 
    Ns_Log (Notice, "SQL():  %s", query);
  
  oci_status = OCIHandleAlloc (connection->env,
			       (oci_handle_t **) &connection->stmt,
			       OCI_HTYPE_STMT,
			       0, NULL);
  if (tcl_error_p (lexpos (), interp, dbh, "OCIHandleAlloc", query, oci_status))
    {
      flush_handle (dbh);
      return TCL_ERROR;
    }
  
  oci_status = OCIStmtPrepare (connection->stmt,
			       connection->err,
			       query, strlen (query),
			       OCI_NTV_SYNTAX,
			       OCI_DEFAULT);
  if (tcl_error_p (lexpos (), interp, dbh, "OCIStmtPrepare", query, oci_status))
    {
      flush_handle (dbh);
      return TCL_ERROR;
    }
  
  data = &argv[4];
  connection->n_columns = argc - 4;

  Tcl_SplitList(interp, argv[4], &lob_argc, &lob_argv);

  bind_variables = parse_bind_variables(query);

  connection->n_columns = string_list_len(bind_variables);
  
  log(lexpos(), "%d bind variables", connection->n_columns);

  malloc_fetch_buffers (connection);
  
  argv_base = 4;

  for (var_p = bind_variables, i=0; var_p != NULL; var_p = var_p->next, i++)
    {
      fetch_buffer_t *fetchbuf = &connection->fetch_buffers[i];
      char *nbuf;
      char *value = NULL;
      int index;
      int lob_i;

      fetchbuf->type = -1;
      index = strtol(var_p->string, &nbuf, 10);
      if (*nbuf == '\0')
	{
	  /* It was a valid number.
	     Pick out one of the remaining arguments,
	     where ":1" is the first remaining arg. */
	  if ((index < 1) || (index > (argc - argv_base - 1)))
	    {
	      if (index < 1)
		{
		  Tcl_AppendResult (interp, "invalid positional variable `:",
				    var_p->string, "', valid values start with 1", NULL);
		}
	      else
		{
		  Tcl_AppendResult (interp, "not enough arguments for positional variable ':",
				    var_p->string, "'", NULL);
		}
	      flush_handle(dbh);
	      string_list_free_list(bind_variables);
	      Tcl_Free((char *)lob_argv);
	      return TCL_ERROR;
	    }
	  value = argv[argv_base + index];
	}
      else
	{
	  value = Tcl_GetVar(interp, var_p->string, 0);
	  if (value == NULL)
	    {
	      Tcl_AppendResult (interp, "undefined variable `", var_p->string,
				"'", NULL);
	      flush_handle(dbh);
	      string_list_free_list(bind_variables);
	      Tcl_Free((char *)lob_argv);
	      return TCL_ERROR;
	    }
	}


      fetchbuf->buf = Ns_StrDup(value);
      fetchbuf->fetch_length = strlen(fetchbuf->buf) + 1;
      fetchbuf->is_null = 0;

      if (dbh->verbose)
	Ns_Log(Notice, "bind variable '%s' = '%s'", var_p->string, value);

      log(lexpos(), "ns_ora clob_dml:  binding variable %s", var_p->string);

      for (lob_i=0; lob_i < lob_argc; lob_i++) {
	if (strcmp(lob_argv[lob_i], var_p->string) == 0) {
	  fetchbuf->is_lob = 1;
	  log(lexpos(), "bind variable %s is a lob", var_p->string);
	  break;
	}
      }

      oci_status = OCIBindByName(connection->stmt,
				 &fetchbuf->bind,
				 connection->err,
				 var_p->string,
				 strlen(var_p->string),
				 fetchbuf->buf,
				 fetchbuf->fetch_length,
				 fetchbuf->is_lob ?
				   (blob_p ? SQLT_BLOB : SQLT_CLOB)
				   : SQLT_STR,
				 &fetchbuf->is_null,
				 0,
				 0,
				 0,
				 0,
				 fetchbuf->is_lob ?
				   OCI_DATA_AT_EXEC : OCI_DEFAULT);

      if (oci_error_p (lexpos (), dbh, "OCIBindByName", query, oci_status))
	{
	  Tcl_SetResult(interp, dbh->dsExceptionMsg.string, TCL_VOLATILE);
	  flush_handle (dbh);
	  string_list_free_list(bind_variables);
	  Tcl_Free((char *)lob_argv);
	  return TCL_ERROR;
	}

      if (fetchbuf->is_lob)
	{
	  oci_status = OCIBindDynamic (fetchbuf->bind,
				       connection->err,
				       0, no_data,
				       fetchbuf, get_data);
	  if (tcl_error_p (lexpos (), interp, dbh, "OCIBindDynamic", query, oci_status))
	    {
	      flush_handle (dbh);
	      Tcl_Free((char *)lob_argv);
	      string_list_free_list(bind_variables);
	      return TCL_ERROR;
	    }
	}
    }

  Tcl_Free((char *)lob_argv);


  oci_status = OCIStmtExecute (connection->svc,
			       connection->stmt,
			       connection->err,
			       1,
			       0, NULL, NULL,
			       OCI_DEFAULT);

  if (tcl_error_p (lexpos (), interp, dbh, "OCIStmtExecute", query, oci_status))
    {
      flush_handle (dbh);
      string_list_free_list(bind_variables);
      return TCL_ERROR;
    }


  for (var_p = bind_variables, i=0; var_p != NULL; var_p = var_p->next, i++)
    {
      fetch_buffer_t *fetchbuf = &connection->fetch_buffers[i];
      ub4 length = -1;

      if (!fetchbuf->is_lob) {
	log(lexpos(), "column %d is not a lob", i);
	continue;
      }

      if (!files_p)
	length = strlen(fetchbuf->buf);

      if (dbh->verbose) 
	{
	  if (files_p) 
	    {
	      Ns_Log (Notice, "  CLOB # %d, file name %s", i, 
		      fetchbuf->buf);
	    }
	  else 
	    {
	      Ns_Log (Notice, "  CLOB # %d, length %d: %s", i, length, 
		      (length == 0) ? "(NULL)" : fetchbuf->buf);
	    }
	}
      
      /* if length is zero, that's an empty string.  Bypass the LobWrite
       * to have it insert a NULL value
       */
      if (length == 0) 
	  continue;

      for (k = 0; k < (int)fetchbuf->n_rows; k ++)
	{
	  if (files_p) 
	    {
	      if (stream_read_lob(interp, dbh, 1, fetchbuf->lobs[k], fetchbuf->buf,
				  connection)
		  != NS_OK) 
		{
		  tcl_error_p (lexpos (), interp, dbh, "stream_read_lob",
			       query, oci_status);
		  string_list_free_list(bind_variables);
		  return TCL_ERROR;
		}
	      continue;
	    }
	  
	  log(lexpos(), "using lob %x", fetchbuf->lobs[k]);
	  oci_status = OCILobWrite (connection->svc,
				    connection->err,
				    fetchbuf->lobs[k],
				    &length,
				    1,
				    fetchbuf->buf,
				    length,
				    OCI_ONE_PIECE,
				    0,
				    0,
				    0,
				    SQLCS_IMPLICIT);

	  if (tcl_error_p (lexpos (), interp, dbh, "OCILobWrite", query, oci_status))
	    {
	      string_list_free_list(bind_variables);
	      flush_handle (dbh);
	      return TCL_ERROR;
	    }
	}
    }
  
  if (connection->mode == autocommit)
    {
      oci_status = OCITransCommit (connection->svc,
				   connection->err,
				   OCI_DEFAULT);
      if (tcl_error_p (lexpos (), interp, dbh, "OCITransCommit", query, oci_status))
	{
	  string_list_free_list(bind_variables);
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
    }
  
  /* all done */
  free_fetch_buffers(connection);
  string_list_free_list(bind_variables);

  return TCL_OK;
}

/*--------------------------------------------------------------------*/

/*
 * lob_dml_cmd handles these commands:
 *
 * ns_ora clob_dml
 * ns_ora clob_dml_file
 * ns_ora blob_dml
 * ns_ora blob_dml_file
 */

static int
lob_dml_cmd(Tcl_Interp *interp, int argc, char *argv[],
    Ns_DbHandle *dbh, ora_connection_t *connection)
{
  char *query;
  char **data;
  int i;
  int k;
  int files_p = NS_FALSE;
  int blob_p = NS_FALSE;
  oci_status_t oci_status;
  
  if (argc < 5)
    {
      Tcl_AppendResult (interp, "wrong number of args: should be `",
			argv[0], argv[1], "dbId query ",
			"[clobValues | filenames] ...'",  NULL);
      return TCL_ERROR;
    }

  flush_handle (dbh);

  if (   ! strcmp (argv[1], "clob_dml_file")
      || ! strcmp (argv[1], "blob_dml_file"))
    files_p = NS_TRUE;

  if ( ! strncmp (argv[1], "blob", 4))
      blob_p = NS_TRUE;

  query = argv[3];

  if (!allow_sql_p(dbh, query, NS_TRUE))
    {
      Tcl_AppendResult (interp, "SQL ", query, " has been rejected "
			"by the Oracle driver", NULL);
      return TCL_ERROR;
    }

  if (dbh->verbose) 
    Ns_Log (Notice, "SQL():  %s", query);
  
  oci_status = OCIHandleAlloc (connection->env,
			       (oci_handle_t **) &connection->stmt,
			       OCI_HTYPE_STMT,
			       0, NULL);
  if (tcl_error_p (lexpos (), interp, dbh, "OCIHandleAlloc", query, oci_status))
    {
      flush_handle (dbh);
      return TCL_ERROR;
    }
  
  oci_status = OCIStmtPrepare (connection->stmt,
			       connection->err,
			       query, strlen (query),
			       OCI_NTV_SYNTAX,
			       OCI_DEFAULT);
  if (tcl_error_p (lexpos (), interp, dbh, "OCIStmtPrepare", query, oci_status))
    {
      flush_handle (dbh);
      return TCL_ERROR;
    }
  
  data = &argv[4];
  connection->n_columns = argc - 4;

  if (files_p) 
    {
      /* pre-flight the existance of the files */
      
      for (i = 0; i < connection->n_columns; i++) 
	{
	  if (access(data[i], R_OK) != 0) 
	    {
	      Tcl_AppendResult (interp, "could not access file", data[i], NULL);
	      flush_handle (dbh);
	      return TCL_ERROR;
	   }
	}
    }
  
  malloc_fetch_buffers (connection);
  
  for (i = 0; i < connection->n_columns; i ++)
    {
      fetch_buffer_t *fetchbuf = &connection->fetch_buffers[i];
      
      fetchbuf->type = -1;

      oci_status = OCIBindByPos (connection->stmt,
				 &fetchbuf->bind,
				 connection->err,
				 i + 1,
				 0,
				 -1,
				 (blob_p) ? SQLT_BLOB : SQLT_CLOB,
				 0,
				 0,
				 0,
				 0,
				 0,
				 OCI_DATA_AT_EXEC);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIBindByPos", query, oci_status))
	{
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
      
      oci_status = OCIBindDynamic (fetchbuf->bind,
				   connection->err,
				   0, no_data,
				   fetchbuf, get_data);
      if (tcl_error_p (lexpos (), interp, dbh, "OCIBindDynamic", query, oci_status))
	{
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
    }

  oci_status = OCIStmtExecute (connection->svc,
			       connection->stmt,
			       connection->err,
			       1,
			       0, NULL, NULL,
			       OCI_DEFAULT);

  if (tcl_error_p (lexpos (), interp, dbh, "OCIStmtExecute", query, oci_status))
    {
      flush_handle (dbh);
      return TCL_ERROR;
    }


  for (i = 0; i < connection->n_columns; i ++)
    {
      fetch_buffer_t *fetchbuf = &connection->fetch_buffers[i];
      
      ub4 length = -1;

      if (!files_p)
	length = strlen (data[i]);

      if (dbh->verbose) 
	{
	  if (files_p) 
	    {
	      Ns_Log (Notice, "  CLOB # %d, file name %s", i, 
		      data[i]);
	    }
	  else 
	    {
	      Ns_Log (Notice, "  CLOB # %d, length %d: %s", i, length, 
		      (length == 0) ? "(NULL)" : data[i]);
	    }
	}
      
      /* if length is zero, that's an empty string.  Bypass the LobWrite
       * to have it insert a NULL value
       */
      if (length == 0) 
	  continue;

      for (k = 0; k < (int)fetchbuf->n_rows; k ++)
	{
	  if (files_p) 
	    {
	      if (stream_read_lob(interp, dbh, 1, fetchbuf->lobs[k], data[i],
				  connection)
		  != NS_OK) 
		{
		  tcl_error_p (lexpos (), interp, dbh, "stream_read_lob",
			       query, oci_status);
		  return TCL_ERROR;
		}
	      continue;
	    }
	  
	  oci_status = OCILobWrite (connection->svc,
				    connection->err,
				    fetchbuf->lobs[k],
				    &length,
				    1,
				    data[i],
				    length,
				    OCI_ONE_PIECE,
				    0,
				    0,
				    0,
				    SQLCS_IMPLICIT);

	  if (tcl_error_p (lexpos (), interp, dbh, "OCILobWrite", query, oci_status))
	    {
	      flush_handle (dbh);
	      return TCL_ERROR;
	    }
	}
    }
  
  if (connection->mode == autocommit)
    {
      oci_status = OCITransCommit (connection->svc,
				   connection->err,
				   OCI_DEFAULT);
      if (tcl_error_p (lexpos (), interp, dbh, "OCITransCommit", query, oci_status))
	{
	  flush_handle (dbh);
	  return TCL_ERROR;
	}
    }
  
  /* all done */
  free_fetch_buffers(connection);

  return TCL_OK;
}

/*--------------------------------------------------------------------*/

#if defined(NS_AOLSERVER_3_PLUS)

/* the AOLserver3 team removed some commands that are pretty vital
 * to the normal operation of the ACS (ArsDigita Community System).
 * We include definitions for them here
 */


static int
ora_get_column_index (Tcl_Interp *interp, Ns_DbTableInfo *tinfo,
		      char *indexStr, int *index)
{
    int result = TCL_ERROR;

    if (Tcl_GetInt (interp, indexStr, index)) 
      {
	goto bailout;
      }

    if (*index >= tinfo->ncolumns) 
      {
	char buffer[80];
	snprintf (buffer, sizeof(buffer), "%d", tinfo->ncolumns);

	Tcl_AppendResult (interp, buffer, " is an invalid column "
			  "index.  ", tinfo->table->name, " only has ",
			  buffer, " columns", NULL);
	goto bailout;
      }

    result = TCL_OK;

  bailout:
    return (result);

} /* ora_get_column_index */



/* re-implement the ns_column command */
static int
ora_column_command (ClientData dummy, Tcl_Interp *interp, 
		    int argc, char *argv[])
{
    int result = TCL_ERROR;
    Ns_DbHandle	*handle;
    Ns_DbTableInfo *tinfo = NULL;
    int colindex = -1;

    if (argc < 4) 
      {
	Tcl_AppendResult (interp, "wrong # args:  should be \"",
			  argv[0], " command dbId table ?args?\"", NULL);
	goto bailout;
      }

    if (Ns_TclDbGetHandle (interp, argv[2], &handle) != TCL_OK) 
      {
	goto bailout;
      }

    /*!!! we should cache this */
    tinfo = ora_get_table_info (handle, argv[3]);
    if (tinfo == NULL) 
      {
	Tcl_AppendResult (interp, "could not get table info for "
			  "table ", argv[3], NULL);
	goto bailout;
      }

    if (!strcmp(argv[1], "count")) 
      {
	if (argc != 4) 
	  {
	    Tcl_AppendResult (interp, "wrong # of args: should be \"",
			      argv[0], " ", argv[1], " dbId table\"", NULL);
	    goto bailout;
	  }
	sprintf (interp->result, "%d", tinfo->ncolumns);

      } 
    else if (!strcmp(argv[1], "exists")) 
      {
	if (argc != 5) 
	  {
	    Tcl_AppendResult (interp, "wrong # of args: should be \"",
			      argv[0], " ", argv[1],
			      " dbId table column\"", NULL);
	    goto bailout;
	  }
	colindex = Ns_DbColumnIndex (tinfo, argv[4]);
	if (colindex < 0) 
	  { 
	    Tcl_SetResult (interp, "0", TCL_STATIC);
	  }
	else 
	  {
	    Tcl_SetResult (interp, "1", TCL_STATIC);
	  }
      } 
    else if (!strcmp(argv[1], "name")) 
      {
	if (argc != 5) 
	  {
	    Tcl_AppendResult (interp, "wrong # of args: should be \"",
			      argv[0], " ", argv[1],
			      " dbId table column\"", NULL);
	    goto bailout;
	  }
	if (ora_get_column_index (interp, tinfo, argv[4], &colindex) 
	    != TCL_OK) 
	  {
	    goto bailout;
	  }
	Tcl_SetResult (interp, tinfo->columns[colindex]->name, TCL_VOLATILE);
      } 
    else if (!strcmp(argv[1], "type")) 
      {
	if (argc != 5) 
	  {
	    Tcl_AppendResult (interp, "wrong # of args: should be \"",
			      argv[0], " ", argv[1],
			      " dbId table column\"", NULL);
	    goto bailout;
	  }
	colindex = Ns_DbColumnIndex (tinfo, argv[4]);
	if (colindex < 0) 
	  { 
	    Tcl_SetResult (interp, NULL, TCL_VOLATILE);
	  }
	else 
	  {
	    Tcl_SetResult (interp, 
			   Ns_SetGet(tinfo->columns[colindex], "type"),
			   TCL_VOLATILE);
	  }
      } 
    else if (!strcmp(argv[1], "typebyindex")) 
      {
	if (argc != 5) 
	  {
	    Tcl_AppendResult (interp, "wrong # of args: should be \"",
			      argv[0], " ", argv[1],
			      " dbId table column\"", NULL);
	    goto bailout;
	  }
	if (ora_get_column_index (interp, tinfo, argv[4], &colindex) 
	    != TCL_OK) 
	  {
	    goto bailout;
	  }
	if (colindex < 0) 
	  { 
	    Tcl_SetResult (interp, NULL, TCL_VOLATILE);
	  } 
	else 
	  {
	    Tcl_SetResult (interp, 
			   Ns_SetGet(tinfo->columns[colindex], "type"),
			   TCL_VOLATILE);
	  }

      } 
    else if (!strcmp(argv[1], "value")) 
      {
	/* not used in ACS AFAIK */
	Tcl_AppendResult (interp, argv[1], " value is not implemented.", 
			  NULL);
	goto bailout;

      } 
    else if (!strcmp(argv[1], "valuebyindex")) 
      {
	/* not used in ACS AFAIK */
	Tcl_AppendResult (interp, argv[1], " valuebyindex is not implemented.", 
			  NULL);
	goto bailout;
      } 
    else 
      {
	Tcl_AppendResult (interp, "unknown command \"", argv[1],
			  "\": should be count, exists, name, "
			  "type, typebyindex, value, or "
			  "valuebyindex", NULL);
	goto bailout;
      }

    result = TCL_OK;

  bailout:

    Ns_DbFreeTableInfo (tinfo);
    return (result);

} /* ora_column_command */



/* re-implement the ns_table command */

static int
ora_table_command (ClientData dummy, Tcl_Interp *interp, 
		   int argc, char *argv[])
{
    int result = TCL_ERROR;
    Ns_DString tables_string;
    char *tables, *scan;

    Ns_DbHandle	*handle;

    if (argc < 3) 
      {
	Tcl_AppendResult (interp, "wrong # args:  should be \"",
			  argv[0], " command dbId ?args?\"", NULL);
	goto bailout;
      }

    if (Ns_TclDbGetHandle (interp, argv[2], &handle) != TCL_OK) 
      {
	goto bailout;
      }

    if (!strcmp(argv[1], "bestrowid")) 
      {
	/* not used in ACS AFAIK */
	Tcl_AppendResult (interp, argv[1], " bestrowid is not implemented.", 
			  NULL);
	goto bailout;
      }
    else if (!strcmp(argv[1], "exists")) 
      {
	int exists_p = 0;

	if (argc != 4) 
	  {
	    Tcl_AppendResult (interp, "wrong # of args: should be \"",
			      argv[0], " ", argv[1], "dbId table\"", NULL);
	    goto bailout;
	  }

	Ns_DStringInit (&tables_string);

	scan = ora_table_list (&tables_string, handle, 1);

	if (scan == NULL) 
	  {
	    Ns_DStringFree (&tables_string);
	    goto bailout;
	  }

	while (*scan != '\000') 
	  {
	    if (!strcmp(argv[3], scan)) 
	      {
		exists_p = 1;
		break;
	      }
	    scan += strlen(scan) + 1;
	  }

	Ns_DStringFree (&tables_string);
	
	if (exists_p) 
	  {
	    Tcl_SetResult (interp, "1", TCL_STATIC);
	  } 
	else 
	  {
	    Tcl_SetResult (interp, "0", TCL_STATIC);
	  }

      } 
    else if (!strncmp(argv[1], "list", 4)) 
      {
	int system_tables_p = 0;

	if (argc != 3) 
	  {
	    Tcl_AppendResult (interp, "wrong # of args: should be \"",
			      argv[0], " ", argv[1], "dbId\"", NULL);
	    goto bailout;
	  }

	if (!strcmp(argv[1], "listall")) 
	  {
	    system_tables_p = 1;
	  }

	Ns_DStringInit (&tables_string);

	tables = ora_table_list (&tables_string, handle, system_tables_p);

	if (tables == NULL) 
	  {
	    Ns_DStringFree (&tables_string);
	    goto bailout;
	  }

	for (scan = tables; *scan != '\000'; scan += strlen(scan) + 1) 
	  {
	    Tcl_AppendElement (interp, scan);
	  }
	Ns_DStringFree (&tables_string);

      } 
    else if (!strcmp(argv[1], "value")) 
      {
	/* not used in ACS AFAIK */
	Tcl_AppendResult (interp, argv[1], " value is not implemented.", 
			  NULL);
	goto bailout;

      } 
    else 
      {
	Tcl_AppendResult (interp, "unknown command \"", argv[1],
			  "\": should be bestrowid, exists, list, "
			  "listall, or value", NULL);
	goto bailout;
      }

    result = TCL_OK;

  bailout:
    return (result);

} /* ora_table_command */



static Ns_DbTableInfo *
Ns_DbNewTableInfo (char *table)
{
    Ns_DbTableInfo *tinfo;

    tinfo = Ns_Malloc (sizeof(Ns_DbTableInfo));

    tinfo->table = Ns_SetCreate (table);
    tinfo->ncolumns = 0;
    tinfo->size = 5;
    tinfo->columns = Ns_Malloc (sizeof(Ns_Set *) * tinfo->size);

    return (tinfo);

} /* Ns_DbNewTableInfo */



static void
Ns_DbAddColumnInfo (Ns_DbTableInfo *tinfo, Ns_Set *column_info)
{
    tinfo->ncolumns++;

    if (tinfo->ncolumns > tinfo->size) 
      {
	tinfo->size *= 2;
	tinfo->columns = Ns_Realloc (tinfo->columns,
				     tinfo->size * sizeof(Ns_Set *));
      }
    tinfo->columns[tinfo->ncolumns - 1] = column_info;

} /* Ns_DbAddColumnInfo */



static void
Ns_DbFreeTableInfo (Ns_DbTableInfo *tinfo)
{
    int i;

    if (tinfo != NULL) 
      {
	for (i = 0; i < tinfo->ncolumns; i++) 
	  {
	    Ns_SetFree (tinfo->columns[i]);
	  }

	Ns_SetFree (tinfo->table);
	Ns_Free (tinfo->columns);
	Ns_Free (tinfo);
      }

} /* Ns_DbFreeTableInfo */


static int
Ns_DbColumnIndex (Ns_DbTableInfo *tinfo, char *name)
{
    int i;
    int result = -1;

    for (i = 0; i < tinfo->ncolumns; i++) 
      {
	char *cname = tinfo->columns[i]->name;
	if (   (cname == name)
	    || ((cname == NULL) && (name == NULL))
	    || (strcmp(cname, name) == 0)) 
	  {
	    result = i;
	    break;
	  }
      }

    return (result);

} /* Ns_DbColumnIndex */



#endif /* NS_AOLSERVER_3_PLUS */

