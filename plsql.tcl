# $Id$
#
# plsql.tcl --
#
#       Implements PL/SQL integration layer that will automatically create
#       Tcl wrapper procedures to invoke PL/SQL procedures and functions.
#
# Copyright (C) 2002-2004 Lee Teague <lteague@dashf.com>
#

namespace eval ::plsql {}

#{{{ parse_args
#
# Usage: parse_args name_of_args_var -myoption ?my? ?next? ?few? ?varnames? ?-default ?my? ?next? ?few? ?values??
#
# This will ONLY fail if you have the wrong number of args.
# ${-myoption} (your option) will be set to 1 or 0 if it was or was not passed in.
#
proc parse_args { args_var args } {
    if { ! [llength $args] } {
      return 1
    }
   
    if { [lsearch -exact [lrange $args 2 end] "-default"] > -1 } {
      set defaultargs [lrange $args [expr [lsearch -exact [lrange $args 2 end] "-default"] + 3] end]
      set args [lrange $args 0 [expr [lsearch -exact [lrange $args 2 end] "-default"] + 1]]
    } else {
      set defaultargs {}
    }

    for { set __arg_count__ 0 } { $__arg_count__ < [llength $args] } { incr __arg_count__ } {
      set arg [lindex $args $__arg_count__]
      set default [lindex $defaultargs [expr $__arg_count__ - 1]]
      upvar $arg thisarg
      if { ! [info exists thisarg] } {
        set thisarg $default
      }
    }

    upvar $args_var originalargs
    
    if { ! [info exists originalargs] || ! [llength $originalargs] } {
      return 1
    }

    set option [lindex $args 0]

    upvar $option theoption
    set theoption 0
    set pos [lsearch $originalargs $option]
    if { $pos == -1 } {
      return 1
    }
    set numofargs [expr [llength $args] -1]
    set args [lrange $args 1 end]

    set myargs [lrange $originalargs [expr $pos+1] end]

    if { [llength $myargs] < $numofargs } {
      set err "Option $option requires args $args"
      ns_log error $err
      return -code error $err
    }
    set originalargs [concat [lrange $originalargs 0 [expr $pos -1]] [lrange $originalargs [expr $pos + $numofargs + 1] end]]
    
    set i 0
    foreach arg $args {
      upvar $arg thisarg
      set thisarg [lindex $myargs $i]
      incr i
    }

    upvar theoptions theoptions
    
    lappend theoptions $option
    set theoption 1

    return 1
}
#}}}

#{{{ is_arg_set
proc is_arg_set { option } {
  upvar $option theoption

  if { [isfalse theoption] } { 
    return 0
  } else {
    return 1
  }

}
#}}}

#{{{ isfalse
proc isfalse { varname } {
  upvar $varname thevar

  if { ! [info exists thevar] } {
    return 1
  }

  if { [string is false $thevar] } {
    return 1
  }

  return 0
}
#}}}

#{{{ plsql::init
#
proc plsql::init { args } {

  # handle regexp or glob matching
  parse_args args -regexp
  parse_args args -pool pool
  set package [lindex $args 0]
  set matches [lrange $args 1 end]
  if { ![llength $package] } { error "Wrong # of args.  Should be [lindex [info level 1] 0] package ?-pool pool? ?-regexp? ?match? ?match? ?match?" }
  if { ![llength $matches] } {
    if { [is_arg_set -regexp] } {
      set matches [list .*]
    } else {
      set matches [list *]
    }
  }

  # get the description of the package.
  if { [llength $pool] } {
    set dbh [ns_db gethandle $pool]
  } else {
    set dbh [ns_db gethandle]
  }

  ns_log notice Loading PL/SQL package $package . . .

  set objects [ns_ora desc $dbh $package]
  ns_db releasehandle $dbh

  # for each object (procedure or function)
  foreach object $objects {
    foreach { object_name arguments } $object break;

    set args [list]
    set object_type PROCEDURE

    foreach argument $arguments {
      foreach { arg mode type default } $argument break;
      set arg [string tolower $arg]

      if { $arg == "" } {
        set object_type [list "FUNCTION $type"]
      } else {
        lappend args [list $arg $mode $type $default]
      }
    }
    lappend signatures($object_name) [concat $object_type $args]
  }

  foreach {object_name sigs} [array get signatures] {
    foreach match $matches {
      if { ([is_arg_set -regexp] && [regexp $match [string tolower $object_name]]) || (![is_arg_set -regexp] && [string match $match [string tolower $object_name]]) } {
        # set up the procedure in the package namespace
        make_procedure $pool $package [string tolower $object_name] $sigs
        break
      }
    }
  }


  if { [llength $objects] } {
    return [info procs ::${package}::*]
  } else {
    error "Invalid Package"
  }

}
#}}}

#{{{ plsql::make_procedure
#
# Usage: make_procedure mypkg myproc { { {a IN NUMBER 0} } { { a IN NUMBER 0 } { b IN NUMBER 1 } } }
#
# signatures is a list of lists:
# {
#   {
#     { OBJECT_TYPE ?return type? }
#     { ARG_NAME MODE TYPE DEFAULT }
#     { ARG_NAME MODE TYPE DEFAULT }
#   }
#   {
#     OBJECT_TYPE
#     { ARG_NAME MODE TYPE DEFAULT }
#   }
# }
# ...
proc plsql::make_procedure { pool package procedure signatures } {

  # initialize the namespace
  namespace eval ::${package} {}

  if { [llength $pool] } {
    proc ::${package}::${procedure} args [subst -nocommands {
      eval ::plsql::execute \"$package\" \"$procedure\" \"\{$signatures\}\" \"\{\$args\}\" -pool \"\{$pool\}\"
    }]
  } else {
    proc ::${package}::${procedure} args [subst -nocommands {
      eval ::plsql::execute \"$package\" \"$procedure\" \"\{$signatures\}\" \"\{\$args\}\"
    }]
  }
}
#}}}

#{{{ plsql::execute
#
proc plsql::execute { args } {
  
  foreach { _package _procedure _signatures _args } $args break;
  parse_args _args -debug
  parse_args _args -select
  parse_args _args -handle _dbh
  parse_args _args -pool _pool
  # we do this because you can't bind "_args", but you can bind "args"
  unset args

  # which signature are they trying to use?
  set _signature [::plsql::find_signature "$_args" "$_signatures"]
  # the object_type ({FUNCTION return_type} or PROCEDURE) is the first element of the signature
  set _object_type [lindex $_signature 0]
  set _signature [lrange $_signature 1 end]
  set _thenames ""
  set _theargs ""
  set _debug ""
  # for each parg (varname, mode, type, default)
  for { set _i 0 } { $_i < [llength $_signature] } { incr _i } {
    set _parg [lindex $_signature $_i]
    set _arg [lindex $_args $_i]
    foreach { _varname _mode _type _default } $_parg break;
    if { $_mode == "IN" } {
      # arg is a value
      set $_varname $_arg
    } else {
      # arg is the name of a variable
      upvar 2 $_arg $_varname
    }
    # if this arg was specified and not defaulted
    if { $_i < [llength $_args] } {
      # prepend the : to bind the varname
      set _bind :$_varname
      # -- means default, '--' means literal --
      if { $_default && [set $_varname] == "--" } { continue }
      # if it's a value, use the appropriate TO_type business
      if { $_mode == "IN" } {
        switch -glob -- [string map {CLOB VARCHAR2} $_type] {
          NUMBER      {
            set _bind TO_NUMBER($_bind)
          }
          DATE        {
            # (today) is explicitly DATE- regsub it out
            regsub {^\((.*?)\)$} "[set $_varname]" {\1} $_varname
            # SYSDATE = SYSDATE, leave it alone.
            if { [string match *SYSDATE* [string toupper [set $_varname]]] } {
              set _bind [set $_varname]
            } else {
              # do a clock scan and clock format to get a proper date string.
              set $_varname [clock format [clock scan [set $_varname]] -format "%m/%d/%Y %H:%M:%S"]
              set _bind "TO_DATE($_bind, 'MM/DD/YYYY HH24:MI:SS')"
            }
          }
          VARCHAR*    {
            # 'string' is explicitly VARCHAR2- regsub it out
            regsub {^'(.*?)'$} "[set $_varname]" {\1} $_varname
            set _bind "TO_CHAR($_bind)"
          }
        }
      }
      lappend _thenames $_varname
      lappend _theargs $_bind
      lappend _debug $_varname $_mode $_type $_default
    }
  }
  set _procedure_arguments {}
  # set up the named args list
  foreach _name $_thenames _arg $_theargs {
    lappend _procedure_arguments "${_name}=>$_arg"
  }

  # get a db handle
  if { [llength $_pool] } {
    set _dbh [ns_db gethandle $_pool]
  } elseif { ![llength $_dbh] } {
    set _dbh [ns_db gethandle]
  }

  if { [is_arg_set -handle] } {
    set _release 0
  } else {
    set _release 1
  }

  # catch the whole thing so we can drop the handle upon failure
  if {[catch {
    # is this a function or procedure?
    switch -exact -- [lindex $_object_type 0] {
      FUNCTION {
        # it's a function
        # if the return value is going to be really big, they may have specified -select.  
        # Note that when using -select, the function can't have any side effects, and you 
        # can't bind multiple out variables.  Also, you can't have a default argument in 
        # the middle using --.  This is mainly here for selecting out huge XML documents 
        # and things.
        set \_return_type [lreplace $_object_type 0 0]
        if { [is_arg_set -select] } {
          # -select.  use select and getrow
          set _call "${_package}.${_procedure}([join $_theargs {, }])"
          set _call "SELECT $_call AS result FROM DUAL"
          # the ns_ora select call
          set _setid [ns_ora select $_dbh $_call]
          ns_db getrow $_dbh $_setid
          set _result [ns_set get $_setid result]
        } else {
          # not -select.  use plsql and bind multiple out variables
          # this result_bind_variable___ stuff is just to make sure we're not stomping out another bind variable.  
          set result_bind_variable___ {}
          set _call "BEGIN :result_bind_variable___ := ${_package}.${_procedure}([join $_procedure_arguments {, }]); END;"
          # the ns_oracle plsql call
          if { $_return_type == "REF CURSOR" } {
            # this function returns a ref cursor
            ns_oracle_plsql _dbh $_call result_bind_variable___
            if { [catch {
              set _setid [ns_db bindrow $_dbh]
            }] } {
              continue
            }
            # create and return a DBO.
            set _release [[::plsql::get_ref_cursor_hook] $_dbh $_setid _result 0]
          } else {
            # find the ref cursor argument, if any.
            foreach _parg $_signature {
              if { [lindex $_parg 2] == "REF CURSOR" } {
                # found one
                set _ref [lindex $_parg 0]
                break
              }
            }
            if { [info exists _ref] } {
              # $_ref is the ref cursor argument.
              ns_oracle_plsql _dbh $_call $_ref
              set _setid [ns_db bindrow $_dbh]
              set _release [[::plsql::get_ref_cursor_hook] $_dbh $_setid $_ref 1]
            } else {
              # no ref cursor argument
              ns_oracle_plsql _dbh $_call
            }
            set _result $result_bind_variable___
          }
        }
      }
      PROCEDURE {
        # a simple procedure call.  We return {} with these.  
        set _call "BEGIN ${_package}.${_procedure}([join $_procedure_arguments {, }]); END;"
        # find the ref cursor argument, if any.
        foreach _parg $_signature {
          if { [lindex $_parg 2] == "REF CURSOR" } {
            # found one
            set _ref [lindex $_parg 0]
            break
          }
        }
        if { [info exists _ref] } {
          # $_ref is the ref cursor argument.
          ns_oracle_plsql _dbh $_call $_ref
          set _setid [ns_db bindrow $_dbh]
          set _release [[::plsql::get_ref_cursor_hook] $_dbh $_setid $_ref 1]
        } else {
          # no ref cursor argument
          ns_oracle_plsql _dbh $_call
        }
        set _result ""
      }
    }
  } _err]} {
    # if there was an error, drop the db handle anyway.
    if { $_release } {
      ns_db releasehandle $_dbh
    }
    error $_err
  } else {
    # no error, just drop the db handle if necessary.
    if { $_release } {
      ns_db releasehandle $_dbh
    }
  }

  # debugging
  if { [is_arg_set -debug] } {
    ns_log notice ${_package}::${_procedure} $_call
    foreach {_varname _mode _type _default} $_debug {
      ns_log notice ${_package}::${_procedure} $_varname $_mode $_type $_default => [set $_varname]
    }
  }

  return $_result
}
#}}}

#{{{ plsql::ns_oracle_plsql
#
proc plsql::ns_oracle_plsql { dbh_var call {bind_variable {}} {loopsafe 1} } {

  # upvar the bind_variable
  if { [llength $bind_variable] } {
    upvar $bind_variable $bind_variable
  }
  upvar $dbh_var handle

  set caught [catch { uplevel "ns_ora plsql $handle \"$call\" $bind_variable" } oerr]

  # The following errors are non-fatal and the query *should* work if
  # we just give it another try after bouncing the pool.

  if { $caught && $loopsafe && [lsearch -exact [list 04061 04068 1012 00028 04065 ""] [ns_dberrorcode $handle]] != -1  } {
    # Don't recurse, try once, then give up.
    if { [lindex [info level -1] 0] != "ns_oracle_plsql"} {
      ns_log warning "Retrying last query . . ."
      ns_db bouncepool [ns_db poolname $handle]
      ns_db releasehandle $handle
      set handle [ns_db gethandle]
      uplevel "ns_oracle_plsql $dbh_var \"$call\" \"$bind_variable\" 0"
    }
  } elseif { $caught } {
    error $oerr
  }

}
#}}}

#{{{ plsql::ref_cursor_hook
# the default ref_cursor_hook (see set_ref_cursor_hook).
proc plsql::ref_cursor_hook { dbh setid variable_name array_allowed } {
  
  upvar $variable_name mydbo

  set dbo(columns) {}
  set dbo(list) {}

  # set up columns element
  for { set i 0 } { $i <= [expr [ns_set size $setid] - 1] } { incr i } {
    lappend dbo(columns) [string trim [ns_set key $setid $i]]
    set columns($i) [string trim [ns_set key $setid $i]]
  }

  set i 0
  while { [ns_db getrow $dbh $setid] } {
    incr i
    lappend dbo(list) $i
    foreach column [array names columns] {
      set dbo(${i}:$columns(${column})) [ns_set value $setid $column]
    }
  }
  if { $array_allowed } {
    array set mydbo [array get dbo]
  } else {
    set mydbo [array get dbo]
  }

  return 1
}
#}}}

#{{{ plsql::type
# if the parg is in/out or out, we do an upvar 4 on the var.
# RETURNS > 1 if the type matches, based on priority, 0 if not.
proc plsql::type { var parg } {
  foreach { varname mode type default } $parg break;

  switch -exact -- $mode {
    IN         {
      # myvar is a value - check the type
      set myvar $var
    }
    OUT        {
      if { [string is double $var] } {
        # out variables can't be doubles
        return 0
      } else {
        upvar 4 $var myvar
        if { ! [info exists myvar] && $type != "REF CURSOR" } {
          set myvar ""
        }
        # as long as var isn't a number, the type matches.
        return 1
      }
    }
    INOUT      {
      # myvar is a value - check the type
      upvar 4 $var myvar
      if { ! [info exists myvar] } {
        return 0
      }
    }
  }

  if { $default && $myvar == "--" } {
    # explicitly DEFAULT
    return 10
  }

  switch -glob -- [string map {CLOB VARCHAR2} $type] {
    NUMBER     {
      if { [string is double $myvar] } {
        # it's a number
        return 2
      }
    }
    DATE       {
      if { [regexp {^\((.*?)\)$} $myvar] } {
        # explicitly DATE
        return 10
      }
      if { ! [string is double $myvar] && ! [catch { clock scan $myvar }] } {
        # implicitly DATE
        return 2
      }
    }
    VARCHAR*   {
      if { [regexp {^'(.*?)'$} $myvar] } {
        # explicitly VARCHAR
        return 10
      } else {
        # varchar matches almost everything..
        return 1
      }
    }
    default    {
      # if it's anything else...
      ns_log warning "Unknown Type: $type"
      # but let it match anyway.
      return 1
    }
  }

  return 0

}
#}}}

#{{{ plsql::find_signature
proc plsql::find_signature { args_list signatures } {
  # figure out which one they're trying to call

  # first, narrow the choices down to the right number of arguments
  set possible {}
  set length [llength $signatures]
  for {set i 0} {$i <= $length } {incr i} {
    set val [lrange [lindex $signatures $i] 1 end]
    set required_args 0
    set num_args [llength $val]
    foreach arg $val {
      if { ! [lindex $arg 3] } {
        incr required_args
      }
    }
    if { [set length [llength $args_list]] >= $required_args && $length <= $num_args } {
      lappend possible $i
    }
  }

  # find the best matching signature
  set biggest 0
  foreach key $possible {
    set signature [lindex $signatures $key]
    set err 0
    set total 1
    foreach parg [lrange $signature 1 end] arg $args_list {
      # multiply all of the results together, if any of them are 0, it will fail.
      # otherwise, the biggest total wins.
      set total [expr $total * [::plsql::type $arg $parg]]
    }
    if { $total > [lindex $biggest 0] } {
      set biggest "$total $key"
    }
  }
  # biggest represents the best matching signature
  if { [llength $biggest] == 2 } {
    return [lindex $signatures [lindex $biggest 1]]
  }


  # no matching signatures - return the USAGE
  set errors ""
  foreach sig $signatures {
    set signature ""
    foreach arg [lrange $sig 1 end] {
      if { [lindex $arg end] } {
        lappend signature "[concat [lrange $arg 0 end-1] DEFAULT]"
      } else {
        lappend signature "[concat [lrange $arg 0 end-1]]"
      }
    }
    lappend errors "[lindex [info level -2] 0] $signature"
  }
  error [subst {USAGE: [join $errors "\n       "]}]
}
#}}}

#{{{ plsql::set_ref_cursor_hook
#
# Give this procedure the name of a procedure that will be responsible for
# handling ref cursors.  It must accept the following arguments:
# dbh setid variable_name return
# WHERE
# dbh is the ns_db handle
# setid is the ns_set to be fetched
# variable_name is the name of the variable to be upvar'd
# array_allowed is 0 or 1: 0 if we're intending to return the value of variable_name, 1 otherwise
# the procedure should return 0 or 1:
# 0 if the procedure is going to release the dbh, 1 if the plsql engine should handle it.
#
proc plsql::set_ref_cursor_hook { proc_name } {
  nsv_set plsql ref_cursor_hook $proc_name
}
#}}}

#{{{ plsql::get_ref_cursor_hook
#
proc plsql::get_ref_cursor_hook {  } {
  if { [nsv_exists plsql ref_cursor_hook] } {
    return [nsv_get plsql ref_cursor_hook]
  } else {
    return ::plsql::ref_cursor_hook
  }
}
#}}}
