package VH1_sqlite3_funcs;

my @export_ok_all
  = (
     'dump_all_tables',
     'dump_table',
     'get_column_count',
     'get_column_sum',
     'get_database_filename',
     'get_database_handle',
     'get_vhost_regex',
     'get_vhosts',
     'get_virtual_host',
     'table_exists',
     'update',
    );

### exported subroutines ###

sub get_database_handle {
  # put the database in the same directory with this script and it
  # will be accessed okay, but now using an environment variable

  # provision for another db file
  my $dbf = shift @_;

  $_dbf = $dbf if (defined $dbf);
  # note that the absence of a file is not a show stopper!

  my $_dbh = DBI->connect("dbi:SQLite:dbname=$_dbf","","",);
  $_dbh->do("PRAGMA foreign_keys = ON");
  $_dbh->{AutoCommit} = 1;

  return $_dbh;
} # get_database_handle

sub get_column_count {
  my $dbh        = shift @_;
  my $tablename  = shift @_;
  my $vhost      = shift @_;

  $tablename = $_ttable if !$tablename;
  my $table_exists = table_exists($dbh, $tablename);
  return 0 if !$table_exists;

  my $colname = $vhosts{$vhost};
  my $colnum  = $cols{$colname};
  my ($result) = $dbh->selectrow_array(qq{
    SELECT COUNT($colname)
    FROM $tablename
    WHERE $tablename.$colname > 0;
  });

  my $count = defined $result ? $result : 0;

  if ($debug) {
    $result = defined $result ? $result : 'undef';
    printf STDERR "debug st line: %d\n", __LINE__;
    print  STDERR "  calc COUNT column '$colname' in table '$tablename'\n";
    print  STDERR "  result: $result\n";
    print  STDERR "  count : $count\n";
    #die "debug exit";
  }

  return $count;

} # get_column_count

sub get_column_sum {
  my $dbh        = shift @_;
  my $tablename  = shift @_;
  my $vhost      = shift @_;

  $tablename = $_ttable if !$tablename;
  my $table_exists = table_exists($dbh, $tablename);
  return 0 if !$table_exists;

  my $colname = $vhosts{$vhost};
  my $colnum  = $cols{$colname};
  my ($result) = $dbh->selectrow_array(qq{
    SELECT SUM($colname)
    FROM $tablename
    WHERE $tablename.$colname > 0;
  });

  my $sum = defined $result ? $result : 0;

  if ($debug) {
    $result = defined $result ? $result : 'undef';
    printf STDERR "debug st line: %d\n", __LINE__;
    print  STDERR "  calc SUM column '$colname' in table '$tablename'\n";
    print  STDERR "  result: $result\n";
    print  STDERR "  sum   : $sum\n";
    #die "debug exit";
  }

  return $sum;

} # get_column_sum

sub table_exists {
  my $dbh       = shift @_;
  my $tablename = shift @_;

  my ($result) = $dbh->selectrow_array(qq{
    SELECT name
    FROM sqlite_master
    WHERE type='table' and name='$tablename';
  });

  if ($debug) {
    my $res = defined $result ? $result : 'undef';
    printf STDERR "debug st line: %d\n", __LINE__;
    print  STDERR "  searching for existence of table '$tablename'\n";
    print  STDERR "  result: $res\n";
    #die "debug exit";
  }

  return $result;

} # table_exists

sub update {
  # inserts or updates two tables
  my $dbh        = shift @_;
  my $ipname     = shift @_;
  my $datestring = shift @_;
  my $vhost      = shift @_;

  return if exists $ignored_hosts{$vhost};

  die "Unknown vhost '$vhost'"
    if !exists $vhosts{$vhost};

  my $colname = $vhosts{$vhost};
  my $colnum  = $cols{$colname};

  #==========================================
  # first the individual IP table
  # does the table exist?
  if (table_exists($dbh, $ipname)) {
    # does the row exist
    my ($result) = $dbh->selectrow_array(qq{
      SELECT $colname
      FROM $ipname
      WHERE $ipname.date_time = '$datestring';
    });
    $result = defined $result ? $result : -1;

    if ($debug) {
      printf STDERR "debug st line: %d\n", __LINE__;
      print  STDERR "  get value of column '$colname' in table '$ipname'\n";
      print  STDERR "  result: $result\n";
      #die "debug exit";
    }

    if ($result == -1) {
      # row does not exist, need an INSERT
      # insert
      my $rows_affected = $dbh->do(qq{
        INSERT INTO $ipname(date_time, $colname)
        VALUES('$datestring', 1);
      });
      die "unexpected rows affected by INSERT: $rows_affected (should be 1)"
        if ($rows_affected != 1);
    }
    elsif ($result == 1) {
      # row DOES exist and colname has a value of 1, no UPDATE should be
      # needed for this table in fact, no further action is needed as this is a "duplicate"
      # visit as we have defined it
      return;
    }
    elsif ($result == 0) {
      # row DOES exist and colname has a value of 0, we need to update
      # that cell to 1
      my $rows_affected = $dbh->do(qq{
	 UPDATE $ipname
	 SET $colname = 1
	 WHERE date_time = '$datestring';
      });
      die "unexpected rows affected by UPDATE: $rows_affected (should be 1)"
        if ($rows_affected != 1);
    }
    else {
      die "\$result = $result (unexpected: should be -1, 0, or 1)";
    }
  }
  else {
    # create the table
    _create_ipname_table($dbh, $ipname);
    # insert
    my $rows_affected = $dbh->do(qq{
      INSERT INTO $ipname(date_time, $colname)
      VALUES('$datestring', 1);
    });
    die "unexpected rows affected by INSERT: $rows_affected (should be 1)"
      if ($rows_affected != 1);
  }

  #==========================================
  # then the total IP table
  # does the table exist?
  if (table_exists($dbh, $_ttable)) {
    # does the row exist
    my ($result) = $dbh->selectrow_array(qq{
      SELECT $colname
      FROM $_ttable
      WHERE $_ttable.ipname = '$ipname';
    });
    $result = defined $result ? $result : -1;

    if ($debug) {
      printf STDERR "debug st line: %d\n", __LINE__;
      print  STDERR "  get value of column '$colname' in table '$_ttable'\n";
      print  STDERR "  result: $result\n";
      #die "debug exit";
    }

    if ($result == -1) {
      # row does not exist, need an INSERT
      # insert
      my $rows_affected = $dbh->do(qq{
        INSERT INTO $_ttable(ipname, $colname)
        VALUES('$ipname', 1);
      });
      die "unexpected rows affected by INSERT: $rows_affected (should be 1)"
        if ($rows_affected != 1);
    }
    else {
      # row DOES exist, need UPDATE
      ++$result;
      my $rows_affected = $dbh->do(qq{
	 UPDATE $_ttable
	 SET $colname = $result
	 WHERE ipname = '$ipname';
      });
      die "unexpected rows affected by UPDATE: $rows_affected (should be 1)"
        if ($rows_affected != 1);
    }
  }
  else {
    # create the table
    _create_totals_table($dbh);
    # insert
    # print "debug: \$colname = '$colname'\n"; die "debug exit";
    my $rows_affected = $dbh->do(qq{
      INSERT INTO $_ttable(ipname, $colname)
      VALUES('$ipname', 1);
    });
    die "unexpected rows affected by INSERT: $rows_affected (should be 1)"
      if ($rows_affected != 1);
  }
  #die "debug exit";

} # update

sub dump_table {
  my $dbh       = shift @_;
  my $tablename = shift @_;

  return if !table_exists($dbh, $tablename);

  # get all rows
  my $sth = $dbh->prepare(qq{
    SELECT *
    FROM $tablename;
  });
  $sth->execute();
  my $nrows = $sth->dump_results();

} # dump_table

sub dump_all_tables {
  my $dbh = shift @_;

  # get a list of all user tables
  my $sth = $dbh->prepare(qq{
    SELECT name
    FROM sqlite_master
    WHERE type='table';
  });
  $sth->execute();

  my @tables = ();
  while (my ($tablename) = $sth->fetchrow_array) {
    push @tables, $tablename;
  }

  @tables = (sort @tables);
  foreach my $t (@tables) {
    print "Dumping table '$t':\n";
    dump_table($dbh, $t);
  }
} # dump_all_tables
