# table-test.tcl -- exercise ns_table
# $Id$


ReturnHeaders

ns_write "
<html>
<head>
    <title>Oracle Driver Table Tests</title>
</head>

<body bgcolor=white>
<h2>Oracle Driver Table Tests</h2>
<hr>

<blockquote>

This exercises the ora_table_list function in the oracle driver, outputting
what it will be doing before it actually does it.  If an error happens,
look for the prior &lt;li&gt;

<ul>
"



ns_write "<li> getting db handle"

set db [ns_db gethandle]


ns_write "
<p><li> doing <code>ns_table list</code>: 
<ul>
"

set table_list [ns_table list $db]

foreach table $table_list {
    ns_write "<li> $table\n"
}

ns_write "</ul>"


ns_write "
<p><li> doing <code>ns_table listall</code>: 
<ul>
"

set table_list [ns_table listall $db]

foreach table $table_list {
    ns_write "<li> $table\n"
}

ns_write "</ul>"




ns_write "<p> <li> explicitly releasing handle"

ns_db releasehandle $db


ns_write "
</ul>
</blockquote>
<hr>
<address><a href=\"mailto:markd@ardigita.com\">markd@arsdigita.com</a></address>
</body>
</html>
"