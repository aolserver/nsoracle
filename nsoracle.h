/* $Id$ */

#define NSORACLE_VERSION       "2.8a1"

#define STACK_BUFFER_SIZE      20000
#define EXEC_PLSQL_BUFFER_SIZE 4096
#define DML_BUFFER_SIZE        4000
#define MAX_DYNAMIC_BUFFER     5000000 /* FIXME: should be config param? */
#define EXCEPTION_CODE_SIZE    5

#define BIND_OUT               1
#define BIND_IN                2

/* Default values for the configuration parameters */
#define DEFAULT_DEBUG  			NS_FALSE
#define DEFAULT_MAX_STRING_LOG_LENGTH	1024
#define DEFAULT_CHAR_EXPANSION          1

#include <oci.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <ns.h>

#ifndef NS_DML
#include <nsdb.h>
#endif

#if !defined(NS_AOLSERVER_3_PLUS)
#include <nstcl.h>
#define NS_EXPORT
#endif

NS_EXPORT int Ns_ModuleVersion = 1;

static char *ora_driver_version = "Oracle Driver version " NSORACLE_VERSION;
static char *ora_driver_name = "Oracle8";

/*  
 *  The OCI lumps a bunch of different things into two C types, sword 
 *  (signed word, i.e., an int) and dvoid (pointer to void; something we
 *  only give back to them anyway).  We give them semantically meaningful
 *  names 
 *
 */

typedef sword oci_status_t;
typedef dvoid oci_handle_t;
typedef dvoid oci_attribute_t;
typedef dvoid oci_param_t;
typedef dvoid oci_descriptor_t;

typedef int (OracleCmdProc) (Tcl_Interp *interp, int objc, 
        struct Tcl_Obj * CONST * objv, Ns_DbHandle *dbh);

Tcl_ObjCmdProc OracleObjCmd;

OracleCmdProc 
    OraclePLSQL,
    OracleExecPLSQL,
    OracleExecPLSQLBind,
    OracleResultRows,
    OracleSelect,
    OracleLobSelect,
    OracleLobDML,
    OracleLobDMLBind,
    OracleDesc,
    OracleGetCols;

/* When we start a query, we allocate one fetch buffer for each 
 * column that we're querying, i.e., if you say "select foo,bar from yow"
 * then two fetch buffers will be allocated 
 */
struct fetch_buffer {
    struct ora_connection *connection;

    /* Oracle will tell us what type this column is */
    OCIStmt   *stmt;
    OCITypeCode type;

    /* will be NULL unless this column happens to be one of the LOB types */
    OCILobLocator *lob;

    /* generally used for INSERT and UPDATE; seldom used */
    OCIBind *bind;

    /* generally used for SELECT; seldom used except for getting LOBs */
    OCIDefine *def;

    /* how many bytes Oracle thinks we need to allocate */
    ub2 size;
    ub2 external_type;

    /* how many bytes we allocated */
    unsigned buf_size;

    /* the stuff above does not change as the rows are fetched */
    /* here's where the actual value from a particular row is kept */
    char *buf;
    char *name;

    /* Used for dynamic binds. */
    int   inout; 

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

struct ora_connection {
    Ns_DbHandle *dbh;
    Tcl_Interp *interp;

    OCIEnv     *env;
    OCIError   *err;
    OCIServer  *srv;
    OCISvcCtx  *svc;
    OCISession *auth;
    OCIStmt    *stmt;

    /* The default is autocommit; we keep track of when a connection 
     * has been kicked into transaction mode.  This was to make Oracle
     * look more like ANSI databases such as Illustra.
     */
    enum {
        autocommit,
        transaction
    } mode;

    /* Fetch buffers; these change per query */
    sb4 n_columns;
    fetch_buffer_t *fetch_buffers;
};
typedef struct ora_connection ora_connection_t;

/* A linked list to use when parsing SQL. */
typedef struct _string_list_elt {
    char *string;
    struct _string_list_elt *next;
} string_list_elt_t;

static char   *Ns_OracleName(Ns_DbHandle *dummy);
static char   *Ns_OracleDbType(Ns_DbHandle *dummy);
static Ns_Set *Ns_OracleSelect(Ns_DbHandle *dbh, char *sql);
static Ns_Set *Ns_OracleBindRow(Ns_DbHandle *dbh);
static int     Ns_OracleOpenDb(Ns_DbHandle *dbh);
static int     Ns_OracleCloseDb(Ns_DbHandle *dbh);
static int     Ns_OracleDML(Ns_DbHandle *dbh, char *sql);
static int     Ns_OracleExec(Ns_DbHandle *dbh, char *sql);
static int     Ns_OracleGetRow(Ns_DbHandle *dbh, Ns_Set * row);
static int     Ns_OracleFlush(Ns_DbHandle *dbh);
static int     Ns_OracleResetHandle(Ns_DbHandle * dbh);
static int     Ns_OracleServerInit(char *hserver, char *hmodule, 
                                   char *hdriver);

static Ns_Set *Oracle0or1Row(Tcl_Interp *interp, 
                             Ns_DbHandle *handle, Ns_Set *row, int *nrows);

static sb4     ora_append_buf_to_dstring(dvoid * ctxp, CONST dvoid * bufp,
                                         ub4 len, ub1 piece);

NS_EXPORT int Ns_DbDriverInit(char *hdriver, char *config_path);

#if defined(NS_AOLSERVER_3_PLUS)
Tcl_CmdProc 
    ora_column_command,
    ora_table_command;      

static Ns_DbTableInfo *ora_get_table_info(Ns_DbHandle * dbh, char *table);
static char *ora_table_list(Ns_DString * pds, Ns_DbHandle * dbh, 
                            int system_tables_p);
static      Ns_DbTableInfo *Ns_DbNewTableInfo(char *table);
static void Ns_DbFreeTableInfo(Ns_DbTableInfo * tinfo);
static void Ns_DbAddColumnInfo(Ns_DbTableInfo * tinfo, Ns_Set * column_info);
static int  Ns_DbColumnIndex(Ns_DbTableInfo * tinfo, char *name);

#else
static char *ora_best_row_id(Ns_DString * pds, Ns_DbHandle * dbh,
                             char *table);
#endif

/*  lexpos is used for logging errors, macro that expands into something like 
 *  ora8.c:345:Ns_DbDriverInit: entry (hdriver 149f60, config_path ns/db/driver/ora8)
 */

#ifdef __GNUC__
#define lexpos() __FILE__, __LINE__, __FUNCTION__
#else
#define lexpos() __FILE__, __LINE__, "<unknown>"
#endif

/* result codes from stream_write_lob
 */
enum {
    STREAM_WRITE_LOB_OK = 0,
    STREAM_WRITE_LOB_ERROR,
    STREAM_WRITE_LOB_PIPE       /* user click stop, but we need to do some cleanup */
};

enum {
    DYNAMIC_BIND_POSITIONAL = 0,
    DYNAMIC_BIND_NAMED,
    DYNAMIC_BIND_SET
};

/* Utility functions */
static void ns_ora_log(const char *file, int line, const char *fn, char *fmt, ...);
static void error(const char *file, int line, const char *fn, char *fmt, ...);
static int oci_error_p(const char *file, int line, const char *fn,
                       Ns_DbHandle * dbh, char *ocifn, char *query,
                       oci_status_t oci_status);
static int tcl_error_p(const char *file, int line, const char *fn, Tcl_Interp * interp,
        Ns_DbHandle * dbh, char *ocifn, char *query,
        oci_status_t oci_status);
static void downcase(char *s);
static char *nilp(char *s);
static int stream_write_lob(Tcl_Interp * interp, Ns_DbHandle * dbh,
                            int rowind, OCILobLocator * lobl, char *path,
                            int to_conn_p, OCISvcCtx * svchp,
                            OCIError * errhp);
static int stream_read_lob(Tcl_Interp * interp, Ns_DbHandle * dbh,
                           int rowind, OCILobLocator * lobl, char *path,
                           ora_connection_t * connection);

static string_list_elt_t * parse_bind_variables(char *input);
static void string_list_free_list(string_list_elt_t * head);
static int string_list_len(string_list_elt_t * head);
static string_list_elt_t * string_list_elt_new(char *string);

static void malloc_fetch_buffers(ora_connection_t * connection);
static void free_fetch_buffers(ora_connection_t * connection);
static int handle_builtins(Ns_DbHandle * dbh, char *sql);

/* Oracle Callbacks used in array dml and clob/blobs. */
static sb4 list_element_put_data(dvoid * ictxp,
                      OCIBind * bindp,
                      ub4 iter,
                      ub4 index,
                      dvoid ** bufpp,
                      ub4 * alenp, ub1 * piecep, dvoid ** indpp);
static sb4 no_data(dvoid * ctxp, OCIBind * bindp,
        ub4 iter, ub4 index, dvoid ** bufpp, ub4 * alenpp, ub1 * piecep,
        dvoid ** indpp);

static sb4 get_data(dvoid * ctxp, OCIBind * bindp,
         ub4 iter, ub4 index, dvoid ** bufpp, ub4 ** alenp, ub1 * piecep,
         dvoid ** indpp, ub2 ** rcodepp);

#ifdef FOR_CASSANDRACLE
static int allow_sql_p(Ns_DbHandle * dbh, char *sql, int display_sql_p);
#else
#define allow_sql_p(a,b,c) NS_TRUE
#endif

#ifdef WIN32
static int snprintf(char *buf, int len, const char *fmt, ...)
#endif


void OracleDescribeSynonym (OCIDescribe *descHandlePtr, 
        OCIParam *paramHandlePtr, ora_connection_t *connection, 
        Ns_DbHandle *dbh, Tcl_Interp *interp );

void OracleDescribePackage (OCIDescribe *descHandlePtr, 
        OCIParam *paramHandlePtr, ora_connection_t *connection, 
        Ns_DbHandle *dbh, char *package, Tcl_Interp *interp );

void OracleDescribeArguments (OCIDescribe *descHandlePtr, 
        OCIParam *paramHandlePtr, ora_connection_t *connection, 
        Ns_DbHandle *dbh, Tcl_Interp *interp, Tcl_Obj *list);

/* Config parameter control */
static int debug_p = NS_FALSE;  
static int max_string_log_length = 0;
static int lob_buffer_size = 16384;
static int char_expansion;

/* Prefetch parameters, if zero leave defaults */
static ub4 prefetch_rows = 0;
static ub4 prefetch_memory = 0;

static Ns_DbProc ora_procs[] = {
    {DbFn_Name,         (void *) Ns_OracleName},
    {DbFn_DbType,       (void *) Ns_OracleDbType},
    {DbFn_OpenDb,       (void *) Ns_OracleOpenDb},
    {DbFn_CloseDb,      (void *) Ns_OracleCloseDb},
    {DbFn_DML,          (void *) Ns_OracleDML},
    {DbFn_Select,       (void *) Ns_OracleSelect},
    {DbFn_Exec,         (void *) Ns_OracleExec},
    {DbFn_BindRow,      (void *) Ns_OracleBindRow},
    {DbFn_GetRow,       (void *) Ns_OracleGetRow},
    {DbFn_Flush,        (void *) Ns_OracleFlush},
    {DbFn_Cancel,       (void *) Ns_OracleFlush},
    {DbFn_ServerInit,   (void *) Ns_OracleServerInit},
    {DbFn_ResetHandle,  (void *) Ns_OracleResetHandle},
#if !defined(NS_AOLSERVER_3_PLUS)
    /* These aren't supported in AOLserver 3 */
    {DbFn_GetTableInfo, (void *) ora_get_table_info},
    {DbFn_TableList,    (void *) ora_table_list},
    {DbFn_BestRowId,    (void *) ora_best_row_id},
#endif
    {0, NULL,}
};

#ifdef WIN32
#define EXTRA_OPEN_FLAGS O_BINARY
#else
#define EXTRA_OPEN_FLAGS 0
#endif

