# error-test.tcl -- exercise error handling.


# $Id$

ReturnHeaders

ns_write "
<html>
<head>
    <title>Oracle Driver Error Tests</title>
</head>

<body bgcolor=white>
<h2>Oracle Driver Error Tests</h2>
<hr>

<blockquote>

This outputs what it will be doing before it actually does it.
If a server error happens, look for the prior &lt;li&gt;

<ul>
"

# random initialization

ns_write "<li> getting db handle"

set db [ns_db gethandle]


ns_write "<li> setting up test table"

catch { ns_db dml $db "drop table markd_error_test" }
ns_db dml $db "create table markd_error_test (an_int integer primary key, chunks clob, a_varchar varchar(2000) not null)"


# syntax errors

ns_write "<p><li> bad column name with ns_db select."
set sql "select an_integer from markd_error_test"

if [catch { ns_db select $db "$sql" } errorInfo] {
    if [empty_string_p $errorInfo] {
	ns_write "<b><font color=red>empty errorInfo</font></b>"
    } else {
	ns_write "got expected error: <pre>$errorInfo</pre>"
    }
} else {
    ns_write "<b><font color=red>didn't get expected exception</font></b>"
}



ns_write "<li> bad column name with ns_ora select."
set sql "select an_integer from markd_error_test"

if [catch { ns_ora select $db "$sql" } errorInfo] {
    if [empty_string_p $errorInfo] {
	ns_write "<b><font color=red>empty errorInfo</font></b>"
    } else {
	ns_write "got expected error: <pre>$errorInfo</pre>"
    }
} else {
    ns_write "<b><font color=red>didn't get expected exception</font></b>"
}



# constraint errors

ns_write "<p><li> duplicate primary keys with ns_db. "

ns_db dml $db "insert into markd_error_test (an_int, chunks, a_varchar) values (1, 'chunks 1', 'a varchar 1')"

set sql "insert into markd_error_test (an_int, chunks, a_varchar) values (1, 'chunks 1', 'a varchar 1')"

if [catch { ns_db dml $db "$sql" } errorInfo] {
    if [empty_string_p $errorInfo] {
	ns_write "<b><font color=red>empty errorInfo</font></b>"
    } else {
	ns_write "got expected error: <pre>$errorInfo</pre>"
    }
    if { [ns_dberrorcode $db] != 1 } {
	ns_write "<b><font color=red>didn't get errorcode of 1</font></b>"
    }
} else {
    ns_write "<b><font color=red>didn't get expected exception</font></b>"
}



ns_write "<p><li> duplicate primary keys with ns_ora. "


set sql "insert into markd_error_test (an_int, chunks, a_varchar) values (1, 'chunks 1', 'a varchar 1')"

if [catch { ns_ora dml $db "$sql" } errorInfo] {
    if [empty_string_p $errorInfo] {
	ns_write "<b><font color=red>empty errorInfo</font></b>"
    } else {
	ns_write "got expected error: <pre>$errorInfo</pre>"
    }
    if { [ns_dberrorcode $db] != 1 } {
	ns_write "<b><font color=red>didn't get errorcode of 1</font></b>"
    }
} else {
    ns_write "<b><font color=red>didn't get expected exception</font></b>"
}




ns_write "<p><li> inserting a null with ns_db. "


set sql "insert into markd_error_test (an_int, chunks, a_varchar) values (2, 'chunks 1', NULL)"

if [catch { ns_db dml $db "$sql" } errorInfo] {
    if [empty_string_p $errorInfo] {
	ns_write "<b><font color=red>empty errorInfo</font></b>"
    } else {
	ns_write "got expected error: <pre>$errorInfo</pre>"
    }
    if { [ns_dberrorcode $db] != 1400 } {
	ns_write "<b><font color=red>didn't get errorcode of 1400</font></b>"
    }
} else {
    ns_write "<b><font color=red>didn't get expected exception</font></b>"
}


ns_write "<p><li> inserting a null with ns_ora. "


set sql "insert into markd_error_test (an_int, chunks, a_varchar) values (1, 'chunks 1', NULL)"

if [catch { ns_ora dml $db "$sql" } errorInfo] {
    if [empty_string_p $errorInfo] {
	ns_write "<b><font color=red>empty errorInfo</font></b>"
    } else {
	ns_write "got expected error: <pre>$errorInfo</pre>"
    }
    if { [ns_dberrorcode $db] != 1400 } {
	ns_write "<b><font color=red>didn't get errorcode of 1400</font></b>"
	ns_write "<p> got ns_dberrormsg of [ns_dberrormsg $db]"
    }
} else {
    ns_write "<b><font color=red>didn't get expected exception</font></b>"
}






# wrap it up

ns_write "<p><li> cleaning up test table"
ns_db dml $db "drop table markd_error_test"

ns_write "<li> explicitly releasing handle"

ns_db releasehandle $db


ns_write "
</ul>
</blockquote>
<hr>
<address><a href=\"mailto:markd@ardigita.com\">markd@arsdigita.com</a></address>
</body>
</html>
"

