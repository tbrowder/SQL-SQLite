package VH1_sqlite3_funcs_mygnus;

my @export_ok_all
  = (
     'column_exists',
     'compare_datehours',
     'dump_all_tables',
     'dump_table',
     'get_column_count',
     'get_column_sum',
     'get_database_filename',
     'get_database_handle',
     'get_sqlite_dbdir',
     'get_tablename',
     'get_two_column_count',
     'get_two_column_sum',
     'table_exists',
     'update_stats',
    );

### exported subroutines ###
sub get_tablename {
  my $typ = shift @_;
  die "unknown type '$typ'"
    if !exists $table_type{$typ};
  my $tablename = $table_type{$typ}{tablename};
  return $tablename;
} # get_tablename

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
  my $colname    = shift @_; # incoming is vhost (domain name)

  my $dbg = 0;

  if ($debug && $dbg) {
    print STDERR "DEBUG (get_column_count): \n";
    print STDERR "  vhost     = '$colname'\n";
    print STDERR "  tablename = '$tablename'\n";
  }

  $colname = get_vhost_colname($colname); # exists $vhosts{$colname} ? $vhosts{$colname} : $colname;

  my $exists = column_exists($dbh, $tablename, $colname);

  if ($debug && $dbg) {
    print STDERR "  colname   = '$colname'\n";
    print STDERR "    exists? = '$exists'\n";
  }

  return 0 if !column_exists($dbh, $tablename, $colname);

  my ($result) = $dbh->selectrow_array(qq{
    SELECT COUNT(DISTINCT $colname)
    FROM $tablename
    WHERE $tablename.$colname > 0;
  });

  my $count = defined $result ? $result : 0;

  if ($debug && $dbg) {
    $result = defined $result ? $result : 'undef';
    printf STDERR "debug at line: %d\n", __LINE__;
    print  STDERR "  calc COUNT column '$colname' in table '$tablename'\n";
    print  STDERR "  result: $result\n";
    print  STDERR "  count : $count\n";
    die "debug exit";
  }

  return $count;

} # get_column_count

sub get_two_column_count {
  my $dbh        = shift @_;
  my $tablename  = shift @_;
  my $colname    = shift @_; # incoming is vhost (domain name)
  my $typ        = shift @_;

  my $dbg = 0;

  if ($debug && $dbg) {
    print STDERR "DEBUG (get_column_count): \n";
    print STDERR "  vhost     = '$colname'\n";
    print STDERR "  tablename = '$tablename'\n";
  }

  $colname = get_vhost_colname($colname); # exists $vhosts{$colname} ? $vhosts{$colname} : $colname;

  my $exists = column_exists($dbh, $tablename, $colname);

  if ($debug && $dbg) {
    print STDERR "  colname   = '$colname'\n";
    print STDERR "    exists? = '$exists'\n";
  }

  return 0 if !column_exists($dbh, $tablename, $colname);

  my $key_col = $table_type{$typ}{key_col};

  my ($result) = $dbh->selectrow_array(qq{
    SELECT COUNT(DISTINCT $key_col)
    FROM $tablename
    WHERE $tablename.$colname > 0;
  });

  my $count = defined $result ? $result : 0;

  if ($debug && $dbg) {
    $result = defined $result ? $result : 'undef';
    printf STDERR "debug at line: %d\n", __LINE__;
    print  STDERR "  calc COUNT column '$colname' in table '$tablename'\n";
    print  STDERR "  result: $result\n";
    print  STDERR "  count : $count\n";
    die "debug exit";
  }

  return $count;

} # get_two_column_count

sub get_column_sum {
  my $dbh        = shift @_;
  my $tablename  = shift @_;
  my $colname    = shift @_; # incoming is vhost (domain name)

  my $dbg = 1;

  if ($debug && $dbg) {
    print STDERR "DEBUG (get_two_column_sum): \n";
    print STDERR "  vhost     = '$colname'\n";
    print STDERR "  tablename = '$tablename'\n";
  }

  $colname = get_vhost_colname($colname); # exists $vhosts{$colname} ? $vhosts{$colname} : $colname;

  my $exists = column_exists($dbh, $tablename, $colname);

  if ($debug && $dbg) {
    print STDERR "  colname   = '$colname'\n";
    print STDERR "    exists? = '$exists'\n";
  }

  return 0 if !$exists;

  my ($result) = $dbh->selectrow_array(qq{
    SELECT SUM($colname)
    FROM $tablename
    WHERE $tablename.$colname > 0;
  });

  my $sum = defined $result ? $result : 0;

  if ($debug && $dbg) {
    $result = defined $result ? $result : 'undef';
    printf STDERR "debug at line: %d\n", __LINE__;
    print  STDERR "  calc SUM column '$colname' in table '$tablename'\n";
    print  STDERR "  result: $result\n";
    print  STDERR "  sum   : $sum\n";
    die "debug exit";
  }

  return $sum;

} # get_column_sum

sub get_two_column_sum {
  my $dbh        = shift @_;
  my $tablename  = shift @_;
  my $colname    = shift @_; # incoming is vhost (domain name)
  my $id         = shift @_;
  my $typ        = shift @_;

  my $dbg = 1;

  my $key_col = $table_type{$typ}{key_col};

  if ($debug && $dbg) {
    print STDERR "DEBUG (get_column_sum): \n";
    print STDERR "  vhost     = '$colname'\n";
    print STDERR "  tablename = '$tablename'\n";
    print STDERR "  id        = '$id'\n";
  }

  $colname = get_vhost_colname($colname); # exists $vhosts{$colname} ? $vhosts{$colname} : $colname;

  my $exists = column_exists($dbh, $tablename, $colname);

  if ($debug && $dbg) {
    print STDERR "  colname   = '$colname'\n";
    print STDERR "    exists? = '$exists'\n";
  }

  return 0 if !$exists;

  my ($result) = $dbh->selectrow_array(qq{
    SELECT SUM($colname)
    FROM  $tablename
    WHERE $tablename.$key_col = '$id'
      AND $tablename.$colname > 0;
  });

  my $sum = defined $result ? $result : 0;

  if ($debug && $dbg) {
    $result = defined $result ? $result : 'undef';
    printf STDERR "debug at line: %d\n", __LINE__;
    print  STDERR "  calc SUM column '$colname' in table '$tablename'\n";
    print  STDERR "  result: $result\n";
    print  STDERR "  sum   : $sum\n";
    die "debug exit";
  }

  return $sum;

} # get_two_column_sum

sub table_exists {
  my $dbh       = shift @_;
  my $tablename = shift @_;

  my ($result) = $dbh->selectrow_array(qq{
    SELECT name
    FROM sqlite_master
    WHERE type='table' and name='$tablename';
  });

  if ($debug && 0) {
    my $res = defined $result ? $result : 'undef';
    printf STDERR "debug at line: %d\n", __LINE__;
    print  STDERR "  searching for existence of table '$tablename'\n";
    print  STDERR "  result: $res\n";
    #die "debug exit";
  }

  return $result;

} # table_exists

sub column_exists {
  my $dbh       = shift @_;
  my $tablename = shift @_;
  my $colname   = shift @_;
  return 0 if !table_exists($dbh, $tablename);

  # use PRAGMA table_info($tablename)
  my ($result) = $dbh->selectall_hashref(qq{
    PRAGMA table_info($tablename);
  }, 'name');

  if ($debug && 0) {
    my $res = defined $result ? $result : 'undef';
    printf STDERR "debug at line: %d\n", __LINE__;
    print  STDERR "  searching for existence of table '$tablename', column '$colname'\n";
    print  STDERR Dumper($result);
    die "debug exit";
  }

  return (exists $result->{$colname});

} # column_exists

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

sub update_stats {
  # inserts or updates a table
  my $dbh         = shift @_;
  my $id          = shift @_; # IP name (xxx.xxx.xxx.xxx => IPxxx_xxx_xxx_xxx)
                              #   or email name
  my $datehour    = shift @_; # input as a datestring
  my $vhost       = shift @_; # domain name
  my $typ         = shift @_; # 'ip', 'email'

  # tmp
  #return if ($typ eq 'email');

  die "unknown type '$typ'"
    if !exists $table_type{$typ};

  return if is_ignored_vhost($vhost); # exists $ignored_hosts{$vhost};
  die "Unknown vhost '$vhost'"
    if !is_known_vhost($vhost); # exists $vhosts{$vhost};

  my $key_col   = $table_type{$typ}{key_col};
  my $key2_col  = $table_type{$typ}{key2_col};
  my $tablename = $table_type{$typ}{tablename};

  my $colname   = get_vhost_colname($vhost); # $vhosts{$vhost};

  #==========================================
  # all data are in a single table (one for each type: ip, email, etc.)
  # does the table exist?
  if (table_exists($dbh, $tablename)) {
    # does the row exist
    my ($ip, $thours, $result) = $dbh->selectrow_array(qq{
      SELECT $key_col, $key2_col, $colname
      FROM   $tablename
      WHERE  $tablename.$key_col  = '$id'
      AND    $tablename.$key2_col = '$datehour';
    });
    $result = defined $result ? $result : -1;

    if ($debug && 0) {
      printf STDERR "debug at line: %d\n", __LINE__;
      print  STDERR "  get value of columns '$key2_col' and '$colname' in table '$tablename'\n";
      print  STDERR "    hours: $thours result: $result\n";
      #die "debug exit";
    }

    if ($result == -1) {
      # row does not exist, need an INSERT
      # insert
      my $rows_affected = $dbh->do(qq{
        INSERT INTO $tablename($key_col, $key2_col, $colname)
        VALUES('$id', '$datehour', 1);
      });

      if (!defined($rows_affected) || $rows_affected != 1) {
        my $res = defined $rows_affected ? $rows_affected : 'undef';
        warn "unexpected rows affected by INSERT: '$res' (should be 1)";
      }
    }
    else {
      # the same, we can return
      #warn "this step should NOT happen!\n";
      # row DOES exist, we MAY need UPDATE, but first check the hours
      #   if epoch hours, numeric comparison, else alphs
      my $res = compare_datehours($datehour, $thours);
      return if ($res == 0); # $datehour  <= $thours
      ++$result;
      my $rows_affected = $dbh->do(qq{
	 UPDATE $tablename
	 SET $key2_col  = $datehour,
             $colname   = $result
	 WHERE $key_col = '$id';
      });
      die "unexpected rows affected by UPDATE: $rows_affected (should be 1)"
        if ($rows_affected != 1);
    }
  }
  else {
    # create the table
    _create_totals_table($dbh, $typ);
    # insert
    # print "debug: \$colname = '$colname'\n"; die "debug exit";
    my $rows_affected = $dbh->do(qq{
      INSERT INTO $tablename($key_col, $key2_col, $colname)
      VALUES('$id', $datehour, 1);
    });
    die "unexpected rows affected by INSERT: $rows_affected (should be 1)"
      if ($rows_affected != 1);
  }
  #die "debug exit";

} # update_stats
