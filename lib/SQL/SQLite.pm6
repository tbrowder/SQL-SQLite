unit module SQL::SQLite;

use DBIish;

# This module provodes specific functions for the SQLite RDBMS.

# See module 'DBD::SQL-92' for general functions which should work
# with SQLite.


sub open-db($dbfile) is export(:open-db) {
    # returns a handle to it
    my $dbh = DBIish.connect('SQLite', :database($dbfile));
    return $dbh;
} # open-db
