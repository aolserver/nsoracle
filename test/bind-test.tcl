# bind-test.tcl -- exercise the bind variable features

# $Id$

ReturnHeaders

ns_write "
<html>
<head>
    <title>Oracle Driver Bind Variable Tests</title>
</head>

<body bgcolor=white>
<h2>Oracle Driver Bind Variable Tests</h2>
<hr>

<blockquote>

This outputs what it will be doing before it actually does it.
If an error happens, look for the prior &lt;li&gt;

<ul>
"

# random initialization

ns_write "<li> getting db handle"

set db [ns_db gethandle]


ns_write "<li> setting up test table"

catch { ns_db dml $db "drop table markd_bind_test" }
ns_db dml $db "create table markd_bind_test (an_int integer, chunks clob, a_varchar varchar(2000))"



# test inserts

ns_write "<p><li> normal insert"

ns_db dml $db "
insert into markd_bind_test (an_int, chunks, a_varchar) 
values (1, 'clob value 1', 'varchar value 1')
"

ns_write "<li> make sure we get the values back. "
set selection [ns_db 1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 1
"]
set_variables_after_query
if { ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>they don't match</font></b>"
} else {
    ns_write "they match"
}




ns_write "<li> simple bind insert"

ns_ora dml $db "
insert into markd_bind_test (an_int, chunks, a_varchar) 
values (2, 'clob value 2', 'varchar value 2')
"

ns_write "<li> make sure we get the values back. "
set selection [ns_db 1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 2
"]
set_variables_after_query
if { ($chunks != "clob value 2") || ($a_varchar != "varchar value 2") } {
    ns_write "<b><font color=red>they don't match</font></b>"
} else {
    ns_write "they match"
}




ns_write "<li> bind insert - inline syntax."

set an_int 3
set chunks "clob value 3"
set a_varchar "varchar value 3"

ns_ora dml $db "
insert into markd_bind_test (an_int, chunks, a_varchar) 
values (:an_int, :chunks, :a_varchar)
"

ns_write "<li> make sure we get the values back. "
set selection [ns_db 1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 3
"]
set_variables_after_query
if { ($chunks != "clob value 3") || ($a_varchar != "varchar value 3") } {
    ns_write "<b><font color=red>they don't match</font></b>"
} else {
    ns_write "they match"
}





ns_write "<li> bind insert - :1 syntax."

set an_int 4
set chunks "clob value 4"
set a_varchar "varchar value 4"

ns_ora dml $db "
insert into markd_bind_test (an_int, chunks, a_varchar) 
values (:1, :2, :3)
" $an_int $chunks $a_varchar

ns_write "<li> make sure we get the values back. "
set selection [ns_db 1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 4
"]
set_variables_after_query
if { ($chunks != "clob value 4") || ($a_varchar != "varchar value 4") } {
    ns_write "<b><font color=red>they don't match</font></b>"
} else {
    ns_write "they match"
}




# CLOBs really suck. if you want to insert large clobs, use clob_dml

# ns_write "<li> bind insert with large clob value"
# 
# set large_enough_lob ""
# for { set i 0 } { $i < 17500 } { incr i } {
#     append large_enough_lob "."
#     if { [expr $i % 100] == 0 } {
# 	append large_enough_lob "\n"
#     }
# }
# set an_int 4
# set a_varchar "varchar value 4"
# 
# ns_ora dml $db "
# insert into markd_bind_test (an_int, chunks, a_varchar) 
# values (:an_int, :large_enough_lob, :a_varchar)
# "
# 
# ns_write "<li> make sure we get the values back. "
# set selection [ns_db 1row $db "
# select an_int, chunks, a_varchar
#   from markd_bind_test
#  where an_int = 4
# "]
# set_variables_after_query
# if { ($chunks != $large_enough_lob) || ($a_varchar != "varchar value 4") } {
#     ns_write "<b><font color=red>they don't match</font></b>"
# } else {
#     ns_write "they match"
# }



# select tests

ns_write "<p><li> normal select, returning 1 row. "

set selection [ns_db select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 1
"]
ns_db getrow $db $selection
set_variables_after_query

if { ($an_int != 1) || ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>didn't get expected values</font></b>"
} else {
    ns_write "got expected values"
}


ns_write "<li> simple bind select, returning 1 row. "

set selection [ns_ora select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 1
"]
ns_db getrow $db $selection
set_variables_after_query

if { ($an_int != 1) || ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>didn't get expected values</font></b>"
} else {
    ns_write "got expected values"
}



ns_write "<li> bind select, inline syntax, returning 1 row. "

set an_int_thingie 1

set selection [ns_ora select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :an_int_thingie
"]
ns_db getrow $db $selection
set_variables_after_query

if { ($an_int != 1) || ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>didn't get expected values</font></b>"
} else {
    ns_write "got expected values"
}




ns_write "<li> bind select, :1 syntax, returning 1 row. "

set an_int_thingie 1

set selection [ns_ora select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :1
" $an_int_thingie]
ns_db getrow $db $selection
set_variables_after_query

if { ($an_int != 1) || ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>didn't get expected values</font></b>"
} else {
    ns_write "got expected values"
}




ns_write "<p><li> normal select, returning no rows. "

set selection [ns_db select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 0
"]
if { [ns_db getrow $db $selection] } {
    ns_write "<b><font color=red>got something we shouldn't've</font></b>"
} else {
    ns_write "got expected result"
}


ns_write "<li> simple bind select, returning no rows. "

set selection [ns_ora select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 0
"]
if { [ns_db getrow $db $selection] } {
    ns_write "<b><font color=red>got something we shouldn't've</font></b>"
} else {
    ns_write "got expected result"
}



ns_write "<li> bind select, inline syntax, returning no rows. "

set an_int_thingie 0

set selection [ns_ora select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :an_int_thingie
"]

if { [ns_db getrow $db $selection] } {
    ns_write "<b><font color=red>got something we shouldn't've</font></b>"
} else {
    ns_write "got expected result"
}



ns_write "<li> bind select, :1 syntax, returning no rows. "

set an_int_thingie 0

set selection [ns_ora select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :1
" $an_int_thingie]

if { [ns_db getrow $db $selection] } {
    ns_write "<b><font color=red>got something we shouldn't've</font></b>"
} else {
    ns_write "got expected result"
}






ns_write "<p><li> normal select, returning several rows. "

set selection [ns_db select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 1 or an_int = 2 or an_int = 3
"]
set count 0
while { [ns_db getrow $db $selection] } {
    incr count
}
if { $count != 3 } {
    ns_write "<b><font color=red>didn't get the number of rows expected</font></b>"
} else {
    ns_write "got the expected number of rows"
}



ns_write "<li> simple bind select, returning several rows. "

set selection [ns_ora select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 1 or an_int = 2 or an_int = 3
"]
set count 0
while { [ns_db getrow $db $selection] } {
    incr count
}
if { $count != 3 } {
    ns_write "<b><font color=red>didn't get the number of rows expected</font></b>"
} else {
    ns_write "got the expected number of rows"
}



ns_write "<li> bind select, inline syntax, returning several rows. "

set int_1 1
set int_2 2
set int_3 3

set selection [ns_ora select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :int_1 or an_int = :int_2 or an_int = :int_3
"]

set count 0
while { [ns_db getrow $db $selection] } {
    incr count
}
if { $count != 3 } {
    ns_write "<b><font color=red>didn't get the number of rows expected</font></b>"
} else {
    ns_write "got the expected number of rows"
}




ns_write "<li> bind select, :1 syntax, returning several rows. "

set int_1 1
set int_2 2
set int_3 3

set selection [ns_ora select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :1 or an_int = :2 or an_int = :3
" $int_1 $int_2 $int_3]

set count 0
while { [ns_db getrow $db $selection] } {
    incr count
}
if { $count != 3 } {
    ns_write "<b><font color=red>didn't get the number of rows expected</font></b>"
} else {
    ns_write "got the expected number of rows"
}




ns_write "<li> bind select, returning several rows, with a large number of bind vars. "

set where_clause ""

set int_blah_1 1
set int_blah_2 2


# I've successfuly done 10,000

for { set i 0 } { $i < 500 } { incr i } {
    set int_$i 3
    append where_clause "or an_int = :int_$i "
}

set selection [ns_ora select $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :int_blah_1 or an_int = :int_blah_2 $where_clause
"]

set count 0
while { [ns_db getrow $db $selection] } {
    incr count
}
if { $count != 3 } {
    ns_write "<b><font color=red>didn't get the number of rows expected</font></b>"
} else {
    ns_write "got the expected number of rows"
}



# 1 row tests


ns_write "<p><li> normal 1row, returning 1 row. "

set selection [ns_db 1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 1
"]
set_variables_after_query

if { ($an_int != 1) || ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>didn't get expected values</font></b>"
} else {
    ns_write "got expected values"
}


ns_write "<li> simple bind 1row, returning 1 row. "

set selection [ns_ora 1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 1
"]
set_variables_after_query
if { ($an_int != 1) || ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>didn't get expected values</font></b>"
} else {
    ns_write "got expected values"
}



ns_write "<li> bind 1row, inline syntax, returning 1 row. "

set an_int_thingie 1

set selection [ns_ora 1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :an_int_thingie
"]
set_variables_after_query

if { ($an_int != 1) || ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>didn't get expected values</font></b>"
} else {
    ns_write "got expected values"
}




ns_write "<li> bind 1row, :1 syntax, returning 1 row. "

set an_int_thingie 1

set selection [ns_ora 1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :1
" $an_int_thingie]
set_variables_after_query

if { ($an_int != 1) || ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>didn't get expected values</font></b>"
} else {
    ns_write "got expected values"
}




ns_write "<li> bind 1row, inline syntax, returning > 1 row. "

set an_int_thingie_1 1
set an_int_thingie_2 2

if [catch {
set selection [ns_ora 1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :an_int_thingie_1 or an_int = :an_int_thingie_2
"]
} ] {
    ns_write "got expected exception"
} else {
    ns_write "<b><font color=red>didn't get expected exception</font></b>"
}




ns_write "<li> bind 1row, :1 syntax, returning > 1 row. "

set an_int_thingie_1 1
set an_int_thingie_2 2

if [catch {
set selection [ns_ora 1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :1 or an_int = :2
" an_int_thingie_1 an_int_thingie_2]
} ] {
    ns_write "got expected exception"
} else {
    ns_write "<b><font color=red>didn't get expected exception</font></b>"
}




ns_write "<li> bind 1row, inline syntax, returning 0 rows. "

set an_int_thingie_1 0

if [catch {
set selection [ns_ora 1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :an_int_thingie_1
"]
} ] {
    ns_write "got expected exception"
} else {
    ns_write "<b><font color=red>didn't get expected exception</font></b>"
}




ns_write "<li> bind 1row, :1 syntax, returning 0 rows. "

set an_int_thingie_1 0

if [catch {
set selection [ns_ora 1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :1
" $an_int_thingie_1]
} ] {
    ns_write "got expected exception"
} else {
    ns_write "<b><font color=red>didn't get expected exception</font></b>"
}





# 0 or 1 row tests


ns_write "<p><li> normal 0or1row, returning 1 row. "

set selection [ns_db 0or1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 1
"]
set_variables_after_query
if { ($an_int != 1) || ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>didn't get expected values</font></b>"
} else {
    ns_write "got expected values"
}


ns_write "<li> simple bind 0or1row, returning 1 row. "

set selection [ns_ora 0or1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 1
"]
set_variables_after_query
if { ($an_int != 1) || ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>didn't get expected values</font></b>"
} else {
    ns_write "got expected values"
}



ns_write "<li> bind 0or1row, inline syntax, returning 1 row. "

set an_int_thingie 1

set selection [ns_ora 0or1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :an_int_thingie
"]
set_variables_after_query

if { ($an_int != 1) || ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>didn't get expected values</font></b>"
} else {
    ns_write "got expected values"
}




ns_write "<li> bind 0or1row, :1 syntax, returning 1 row. "

set an_int_thingie 1

set selection [ns_ora 0or1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :1
" $an_int_thingie]
set_variables_after_query

if { ($an_int != 1) || ($chunks != "clob value 1") || ($a_varchar != "varchar value 1") } {
    ns_write "<b><font color=red>didn't get expected values</font></b>"
} else {
    ns_write "got expected values"
}


# 1 row with null tests

ns_write "<p><li> normal 0or1row, returning no rows. "

set selection [ns_db 0or1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 0
"]
if ![empty_string_p $selection] {
    ns_write "<b><font color=red>got something we shouldn't've</font></b>"
} else {
    ns_write "got expected result"
}


ns_write "<li> simple bind 0or1row, returning no rows. "

set selection [ns_ora 0or1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = 0
"]
if ![empty_string_p $selection] {
    ns_write "<b><font color=red>got something we shouldn't've</font></b>"
} else {
    ns_write "got expected result"
}




ns_write "<li> bind 0or1row, inline syntax, returning no rows. "

set an_int_thingie 0

set selection [ns_ora 0or1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :an_int_thingie
"]

if ![empty_string_p $selection] {
    ns_write "<b><font color=red>got something we shouldn't've</font></b>"
} else {
    ns_write "got expected result"
}



ns_write "<li> bind 0or1row, :1 syntax, returning no rows. "

set an_int_thingie 0

set selection [ns_ora 0or1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :1
" $an_int_thingie]
if ![empty_string_p $selection] {
    ns_write "<b><font color=red>got something we shouldn't've</font></b>"
} else {
    ns_write "got expected result"
}



ns_write "<li> bind 0or1row, inline syntax, returning > 1 rows. "

set an_int_thingie_1 1
set an_int_thingie_2 2

if [catch {
set selection [ns_ora 0or1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :an_int_thingie_1 or an_int = :an_int_thingie_2
"]
} ] {
    ns_write "got expected exception"
} else {
    ns_write "<b><font color=red>didn't get expected exception</font></b>"
}



ns_write "<li> bind 0or1row, :1 syntax, returning > 1 rows. "

set an_int_thingie_1 1
set an_int_thingie_2 2

if [catch {
set selection [ns_ora 0or1row $db "
select an_int, chunks, a_varchar
  from markd_bind_test
 where an_int = :1 or an_int = :2
" $an_int_thingie_1 $an_int_thingie_2]
} ] {
    ns_write "got expected exception"
} else {
    ns_write "<b><font color=red>didn't get expected exception</font></b>"
}




# wrap it up

ns_write "<p><li> cleaning up test table"
ns_db dml $db "drop table markd_bind_test"

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

