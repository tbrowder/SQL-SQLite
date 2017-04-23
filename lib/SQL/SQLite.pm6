unit module SQL::SQLite;

use DBIish;

# This module provodes specific functions for the SQLite RDBMS.

# See module 'SQL::SQL-92' for general functions which should work
# with SQLite.


sub open-db($dbfile) is export(:open-db) {
    # returns a handle to it
    my $dbh = DBIish.connect('SQLite', :database($dbfile));
    return $dbh;
} # open-db


sub table-exists($dbh, $table) is export(:table-exists) {
    my $sth = $dbh.prepare(qq:to/STATEMENT/);
    SELECT name
    FROM sqlite_master
    WHERE type='table' and name='$table';
    STATEMENT

    $sth.execute;
    my @vals = $sth.row;
    my $table-exists = @vals ?? True !! False;

    # always clean up after an execute
    $sth.finish;

    return $table-exists;
}
