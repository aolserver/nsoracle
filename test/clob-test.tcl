# clob-test.tcl -- exercise the blob/clob related calls
# $Id$


# these will be created by this script
set empty_file_name "/tmp/markd-empty.txt"
set small_file_name "/tmp/markd-small.txt"
set medium_file_name "/tmp/markd-medium.txt"
set large_file_name "/tmp/markd-large.txt"
set large_binary_file_name "/tmp/markd-large.bin"


# must be bigger than 48K so that it exercises the piece-wise sections of code
set big_text_file_name "/usr/dict/words"
set big_binary_file_name "/home/nsadmin/bin/nsd"


ReturnHeaders

ns_write "
<html>
<head>
    <title>Oracle Driver BLOB/CLOB Tests</title>
</head>

<body bgcolor=white>
<h2>Oracle Driver BLOB/CLOB Tests</h2>
<hr>

<blockquote>

This outputs what it will be doing before it actually does it.
If an error happens, look for the prior &lt;li&gt;

<ul>
"



ns_write "<li> getting db handle"

set db [ns_db gethandle]




ns_write "<li> setting up test table"

catch { ns_db dml $db "drop table markd_lob_test" }
ns_db dml $db "create table markd_lob_test (lob_id integer, chunks clob, blunks blob)"



# real basic lob tests
ns_write "<p><li> <b>Starting basic LOB tests</b>"

set small_lob "This is a small lob.  There are many like it, but this one is mine."




ns_write "<p> <li> inserting small clob"

ns_ora clob_dml $db "
insert into markd_lob_test (lob_id, chunks)
values (0, empty_clob())
returning chunks into :1" $small_lob




ns_write "<li> making sure we get the same clob back. "

set back_lob [database_to_tcl_string $db "select chunks from markd_lob_test where lob_id = 0"]

# to test this conditional
# append back_lob " "

if { [string compare $back_lob $small_lob] == 0 } {
    ns_write "they match"
} else {
    ns_write "<font color=red>they don't match</font>"
}




ns_write "<p><li> inserting empty string as a clob "

ns_ora clob_dml $db "
insert into markd_lob_test (lob_id, chunks)
values (1, empty_clob())
returning chunks into :1" ""




ns_write "<li> making sure we get the same clob back. "

set back_lob [database_to_tcl_string $db "select chunks from markd_lob_test where lob_id = 1"]

# to test this conditional
# append back_lob " "

if { [string compare $back_lob ""] == 0 } {
    ns_write "they match"
} else {
    ns_write "<font color=red>they don't match</font>"
}





ns_write "<p><li> inserting small text string as a blob "

ns_ora blob_dml $db "
insert into markd_lob_test (lob_id, blunks)
values (2, empty_blob())
returning blunks into :1" $small_lob




ns_write "<li> making sure we get the same blob back. "

set back_lob [database_to_tcl_string $db "select blunks from markd_lob_test where lob_id = 2"]

# to test this conditional
# append back_lob " "

if { [string compare $back_lob $small_lob] == 0 } {
    ns_write "they match"
} else {
    ns_write "<font color=red>they don't match</font>"
}



ns_write "<p><li> inserting empty string as a blob "

ns_ora blob_dml $db "
insert into markd_lob_test (lob_id, blunks)
values (3, empty_blob())
returning blunks into :1" ""




ns_write "<li> making sure we get the same blob back. "

set back_lob [database_to_tcl_string $db "select blunks from markd_lob_test where lob_id = 3"]

# to test this conditional
# append back_lob " "

if { [string compare $back_lob ""] == 0 } {
    ns_write "they match"
} else {
    ns_write "<font color=red>they don't match</font>"
}




ns_write "<p><li> updating clob with longer value"

set longer_lob "$small_lob     $small_lob"

ns_ora clob_dml $db "
update markd_lob_test set chunks = empty_clob() 
where lob_id = 0
returning chunks into :1" $longer_lob





ns_write "<li> making sure we get the same clob back. "

set back_lob [database_to_tcl_string $db "select chunks from markd_lob_test where lob_id = 0"]

# to test this conditional
# append back_lob " "

if { [string compare $back_lob $longer_lob] == 0 } {
    ns_write "they match"
} else {
    ns_write "<font color=red>they don't match</font>"
}




ns_write "<p><li> updating clob with smaller value"

set smaller_lob "a"

ns_ora clob_dml $db "
update markd_lob_test set chunks = empty_clob() 
where lob_id = 0
returning chunks into :1" $smaller_lob





ns_write "<li> making sure we get the same clob back. "

set back_lob [database_to_tcl_string $db "select chunks from markd_lob_test where lob_id = 0"]

# to test this conditional
# append back_lob " "

if { [string compare $back_lob $smaller_lob] == 0 } {
    ns_write "they match"
} else {
    ns_write "<font color=red>they don't match</font>"
}




ns_write "<p><li> updating clob with empty value (technique 1)"

set empty_lob ""

ns_ora clob_dml $db "
update markd_lob_test set chunks = empty_clob() 
where lob_id = 0
returning chunks into :1" $empty_lob





ns_write "<li> making sure we get the same clob back. "

set back_lob [database_to_tcl_string $db "select chunks from markd_lob_test where lob_id = 0"]

# to test this conditional
# append back_lob " "

if { [string compare $back_lob $empty_lob] == 0 } {
    ns_write "they match"
} else {
    ns_write "<font color=red>they don't match</font>"
}




ns_write "<p><li> updating clob with empty value (technique 2)"

set empty_lob ""

ns_db dml $db "
update markd_lob_test set chunks = empty_clob() 
where lob_id = 0"





ns_write "<li> making sure we get the same clob back. "

set back_lob [database_to_tcl_string $db "select chunks from markd_lob_test where lob_id = 0"]

# to test this conditional
# append back_lob " "

if { [string compare $back_lob $empty_lob] == 0 } {
    ns_write "they match"
} else {
    ns_write "<font color=red>they don't match</font>"
}





ns_write "<p><li> updating blob with longer value"

set longer_lob "$small_lob     $small_lob"

ns_ora blob_dml $db "
update markd_lob_test set blunks = empty_blob() 
where lob_id = 0
returning blunks into :1" $longer_lob





ns_write "<li> making sure we get the same blob back. "

set back_lob [database_to_tcl_string $db "select blunks from markd_lob_test where lob_id = 0"]

# to test this conditional
# append back_lob " "

if { [string compare $back_lob $longer_lob] == 0 } {
    ns_write "they match"
} else {
    ns_write "<font color=red>they don't match</font>"
}




ns_write "<p><li> updating blob with smaller value"

set smaller_lob "a"

ns_ora blob_dml $db "
update markd_lob_test set blunks = empty_blob() 
where lob_id = 0
returning blunks into :1" $smaller_lob





ns_write "<li> making sure we get the same blob back. "

set back_lob [database_to_tcl_string $db "select blunks from markd_lob_test where lob_id = 0"]

# to test this conditional
# append back_lob " "

if { [string compare $back_lob $smaller_lob] == 0 } {
    ns_write "they match"
} else {
    ns_write "<font color=red>they don't match</font>"
}




ns_write "<p><li> updating blob with empty value (technique 1)"

set empty_lob ""

ns_ora blob_dml $db "
update markd_lob_test set blunks = empty_blob() 
where lob_id = 0
returning blunks into :1" $empty_lob





ns_write "<li> making sure we get the same blob back. "

set back_lob [database_to_tcl_string $db "select blunks from markd_lob_test where lob_id = 0"]

# to test this conditional
# append back_lob " "

if { [string compare $back_lob $empty_lob] == 0 } {
    ns_write "they match"
} else {
    ns_write "<font color=red>they don't match</font>"
}




ns_write "<p><li> updating blob with empty value (technique 2)"

set empty_lob ""

ns_db dml $db "
update markd_lob_test set blunks = empty_blob() 
where lob_id = 0"





ns_write "<li> making sure we get the same blob back. "

set back_lob [database_to_tcl_string $db "select blunks from markd_lob_test where lob_id = 0"]

# to test this conditional
# append back_lob " "

if { [string compare $back_lob $empty_lob] == 0 } {
    ns_write "they match"
} else {
    ns_write "<font color=red>they don't match</font>"
}



# now for the *lob_get_file calls

ns_write "<p><li> <b>Starting *lob_*_file tests</b>"




ns_write "<p><li> setting up test files"

catch { exec rm -f $empty_file_name }
catch { exec rm -f ${empty_file_name}-back }
catch { exec rm -f $small_file_name }
catch { exec rm -f ${small_file_name}-back }
catch { exec rm -f $medium_file_name }
catch { exec rm -f ${medium_file_name}-back }
catch { exec rm -f $large_file_name }
catch { exec rm -f ${large_file_name}-back }
catch { exec rm -f $large_binary_file_name }
catch { exec rm -f ${large_binary_file_name}-back }

set f [open $empty_file_name w]
close $f

# less than 16K
set f [open $small_file_name w]
puts $f "this is a small test file"
close $f

# greater than 16K, less than 32K
set f [open $medium_file_name w]
for { set i 0 } { $i < 17500 } { incr i } {
    puts -nonewline $f "."
}
close $f

# greater than 32K
exec cp $big_text_file_name $large_file_name
exec cp $big_binary_file_name $large_binary_file_name



ns_write "<p> <li> inserting empty file as clob"

ns_ora clob_dml_file $db "
insert into markd_lob_test (lob_id, chunks)
values (100, empty_clob())
returning chunks into :1" $empty_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora clob_get_file $db "select chunks from markd_lob_test where lob_id=100" ${empty_file_name}-back

if [catch { exec diff $empty_file_name ${empty_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}



ns_write "<p> <li> inserting small file as clob"

ns_ora clob_dml_file $db "
insert into markd_lob_test (lob_id, chunks)
values (101, empty_clob())
returning chunks into :1" $small_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora clob_get_file $db "select chunks from markd_lob_test where lob_id=101" ${small_file_name}-back

if [catch { exec diff $small_file_name ${small_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}




ns_write "<p> <li> inserting medium file as clob"

ns_ora clob_dml_file $db "
insert into markd_lob_test (lob_id, chunks)
values (102, empty_clob())
returning chunks into :1" $medium_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora clob_get_file $db "select chunks from markd_lob_test where lob_id=102" ${medium_file_name}-back

if [catch { exec diff $medium_file_name ${medium_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}




ns_write "<p> <li> inserting large file as clob"

ns_ora clob_dml_file $db "
insert into markd_lob_test (lob_id, chunks)
values (103, empty_clob())
returning chunks into :1" $large_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora clob_get_file $db "select chunks from markd_lob_test where lob_id=103" ${large_file_name}-back

if [catch { exec diff $large_file_name ${large_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}




ns_write "<p><li> cleaning up temp files"
catch { exec rm -f ${empty_file_name}-back }
catch { exec rm -f ${small_file_name}-back }
catch { exec rm -f ${medium_file_name}-back }
catch { exec rm -f ${large_file_name}-back }
catch { exec rm -f ${large_binary_file_name}-back }



ns_write "<p> <li> inserting empty file as blob"

ns_ora blob_dml_file $db "
insert into markd_lob_test (lob_id, blunks)
values (200, empty_blob())
returning blunks into :1" $empty_file_name




ns_write "<li> making sure we get an equivalent file back. "

ns_ora blob_get_file $db "select blunks from markd_lob_test where lob_id=200" ${empty_file_name}-back

if [catch { exec diff $empty_file_name ${empty_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}



ns_write "<p> <li> inserting small file as blob"

ns_ora blob_dml_file $db "
insert into markd_lob_test (lob_id, blunks)
values (201, empty_blob())
returning blunks into :1" $small_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora blob_get_file $db "select blunks from markd_lob_test where lob_id=201" ${small_file_name}-back

if [catch { exec diff $small_file_name ${small_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}






ns_write "<p> <li> inserting medium file as blob"

ns_ora blob_dml_file $db "
insert into markd_lob_test (lob_id, blunks)
values (202, empty_blob())
returning blunks into :1" $medium_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora blob_get_file $db "select blunks from markd_lob_test where lob_id=202" ${medium_file_name}-back

if [catch { exec diff $medium_file_name ${medium_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}




ns_write "<p> <li> inserting large file as blob"

ns_ora blob_dml_file $db "
insert into markd_lob_test (lob_id, blunks)
values (203, empty_blob())
returning blunks into :1" $large_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora blob_get_file $db "select blunks from markd_lob_test where lob_id=203" ${large_file_name}-back

if [catch { exec diff $large_file_name ${large_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}



ns_write "<p> <li> inserting large binary file as blob"

ns_ora blob_dml_file $db "
insert into markd_lob_test (lob_id, blunks)
values (204, empty_blob())
returning blunks into :1" $large_binary_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora blob_get_file $db "select blunks from markd_lob_test where lob_id=204" ${large_binary_file_name}-back

if [catch { exec diff $large_binary_file_name ${large_binary_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}



ns_write "<p><li> cleaning up temp files"
catch { exec rm -f ${empty_file_name}-back }
catch { exec rm -f ${small_file_name}-back }
catch { exec rm -f ${medium_file_name}-back }
catch { exec rm -f ${large_file_name}-back }
catch { exec rm -f ${large_binary_file_name}-back }



ns_write "<p><li> updating empty file with large file as clob"

ns_ora clob_dml_file $db "
update markd_lob_test
set chunks = empty_clob()
where lob_id = 100
returning chunks into :1" $large_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora clob_get_file $db "select chunks from markd_lob_test where lob_id=100" ${empty_file_name}-back

if [catch { exec diff $large_file_name ${empty_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}



ns_write "<p><li> updating small file with empty file as clob"

ns_ora clob_dml_file $db "
update markd_lob_test
set chunks = empty_clob()
where lob_id = 101
returning chunks into :1" $empty_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora clob_get_file $db "select chunks from markd_lob_test where lob_id=101" ${small_file_name}-back

if [catch { exec diff $empty_file_name ${small_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}




ns_write "<p><li> updating medium file with small file as clob"

ns_ora clob_dml_file $db "
update markd_lob_test
set chunks = empty_clob()
where lob_id = 102
returning chunks into :1" $small_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora clob_get_file $db "select chunks from markd_lob_test where lob_id=102" ${medium_file_name}-back

if [catch { exec diff $small_file_name ${medium_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}




ns_write "<p><li> updating large file with medium file as clob"

ns_ora clob_dml_file $db "
update markd_lob_test
set chunks = empty_clob()
where lob_id = 103
returning chunks into :1" $medium_file_name


ns_write "<li> making sure we get an equivalent file back. "

ns_ora clob_get_file $db "select chunks from markd_lob_test where lob_id=103" ${large_file_name}-back

if [catch { exec diff $medium_file_name ${large_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}





ns_write "<p><li> cleaning up temp files"
catch { exec rm -f ${empty_file_name}-back }
catch { exec rm -f ${small_file_name}-back }
catch { exec rm -f ${medium_file_name}-back }
catch { exec rm -f ${large_file_name}-back }
catch { exec rm -f ${large_binary_file_name}-back }



ns_write "<p><li> updating empty file with medium file as blob"

ns_ora blob_dml_file $db "
update markd_lob_test
set blunks = empty_blob()
where lob_id = 200
returning blunks into :1" $medium_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora blob_get_file $db "select blunks from markd_lob_test where lob_id=200" ${empty_file_name}-back

if [catch { exec diff $medium_file_name ${empty_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}



ns_write "<p><li> updating small file with large binary file as blob"

ns_ora blob_dml_file $db "
update markd_lob_test
set blunks = empty_blob()
where lob_id = 201
returning blunks into :1" $large_binary_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora blob_get_file $db "select blunks from markd_lob_test where lob_id=201" ${small_file_name}-back

if [catch { exec diff $large_binary_file_name ${small_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}




ns_write "<p><li> updating medium file with empty file as blob"

ns_ora blob_dml_file $db "
update markd_lob_test
set blunks = empty_blob()
where lob_id = 202
returning blunks into :1" $empty_file_name



ns_write "<li> making sure we get an equivalent file back. "

ns_ora blob_get_file $db "select blunks from markd_lob_test where lob_id=202" ${medium_file_name}-back

if [catch { exec diff $empty_file_name ${medium_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}




ns_write "<p><li> updating large file with small file as blob"

ns_ora blob_dml_file $db "
update markd_lob_test
set blunks = empty_blob()
where lob_id = 203
returning blunks into :1" $small_file_name


ns_write "<li> making sure we get an equivalent file back. "

ns_ora blob_get_file $db "select blunks from markd_lob_test where lob_id=203" ${large_file_name}-back

if [catch { exec diff $small_file_name ${large_file_name}-back } ] {
    ns_write "<font color=red>they don't match</font>"
} else {
    ns_write "they match"
}



ns_write "<p><li> cleaning up files"
catch { exec rm -f $empty_file_name }
catch { exec rm -f ${empty_file_name}-back }
catch { exec rm -f $small_file_name }
catch { exec rm -f ${small_file_name}-back }
catch { exec rm -f $medium_file_name }
catch { exec rm -f ${medium_file_name}-back }
catch { exec rm -f $large_file_name }
catch { exec rm -f ${large_file_name}-back }
catch { exec rm -f $large_binary_file_name }
catch { exec rm -f ${large_binary_file_name}-back }




# now for write* tests

ns_write "<p><li> <b>Starting write_clob tests</b>"


ns_write "<p><li> Empty clob:"

ns_ora clob_dml $db "
insert into markd_lob_test (lob_id, chunks)
values (500, empty_clob())
returning chunks into :1" ""

ns_write "<pre>\"[ns_ora write_clob $db "select chunks from markd_lob_test where lob_id = 500"]\"</pre>"




ns_write "<li> small clob:"

ns_ora clob_dml $db "
insert into markd_lob_test (lob_id, chunks)
values (501, empty_clob())
returning chunks into :1" $small_lob


ns_write "<pre>\""

ns_ora write_clob $db "select chunks from markd_lob_test where lob_id = 501"

ns_write "\"</pre>"



ns_write "<li> large enough clob (17000 dots):"

set large_enouge_lob ""

for { set i 0 } { $i < 17500 } { incr i } {
    append large_enough_lob "."
    if { [expr $i % 100] == 0 } {
	append large_enough_lob "\n"
    }
}

ns_ora clob_dml $db "
insert into markd_lob_test (lob_id, chunks)
values (502, empty_clob())
returning chunks into :1" $large_enough_lob


ns_write "<pre>\""

ns_ora write_clob $db "select chunks from markd_lob_test where lob_id = 502"

ns_write "\"</pre>"





ns_write "<p><li> <b>Starting write_blob tests</b>"


ns_write "<p><li> Empty blob:"

ns_ora blob_dml $db "
insert into markd_lob_test (lob_id, blunks)
values (600, empty_blob())
returning blunks into :1" ""

ns_write "<pre>\"[ns_ora write_blob $db "select blunks from markd_lob_test where lob_id = 600"]\"</pre>"




ns_write "<li> small blob:"

ns_ora blob_dml $db "
insert into markd_lob_test (lob_id, blunks)
values (601, empty_blob())
returning blunks into :1" $small_lob


ns_write "<pre>\""

ns_ora write_blob $db "select blunks from markd_lob_test where lob_id = 601"

ns_write "\"</pre>"



ns_write "<li> large enough blob:"

set large_enouge_lob ""

for { set i 0 } { $i < 17600 } { incr i } {
    append large_enough_lob "."
    if { [expr $i % 100] == 0 } {
	append large_enough_lob "\n"
    }
}

ns_ora blob_dml $db "
insert into markd_lob_test (lob_id, blunks)
values (602, empty_blob())
returning blunks into :1" $large_enough_lob


ns_write "<pre>\""

ns_ora write_blob $db "select blunks from markd_lob_test where lob_id = 602"

ns_write "\"</pre>"




# wrap it up

ns_write "<p><li> cleaning up test table"

ns_db dml $db "drop table markd_lob_test"


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