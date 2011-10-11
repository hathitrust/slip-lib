package Db;

=head1 NAME

Db

=head1 DESCRIPTION

This class is a non-OO database interface


=head1 VERSION

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;

# App
use Utils;
use Debug::DUtils;
use Utils::Time;

use Context;
use DbUtils;
use Search::Constants;

use SLIP_Utils::States;
use SLIP_Utils::Common;

# Map from constants to integers for MySQL query building
my $C_NO_ERROR         = IX_NO_ERROR;
my $C_INDEX_FAILURE    = IX_INDEX_FAILURE;
my $C_INDEX_TIMEOUT    = IX_INDEX_TIMEOUT;
my $C_SERVER_GONE      = IX_SERVER_GONE;
my $C_ALREADY_FAILED   = IX_ALREADY_FAILED;
my $C_DATA_FAILURE     = IX_DATA_FAILURE;
my $C_METADATA_FAILURE = IX_METADATA_FAILURE;
my $C_CRITICAL_FAILURE = IX_CRITICAL_FAILURE;
my $C_NO_INDEXER_AVAIL = IX_NO_INDEXER_AVAIL;

$Db::MYSQL_ZERO_TIMESTAMP = '0000-00-00 00:00:00';
$Db::vSOLR_ZERO_TIMESTAMP = '00000000';

# =====================================================================
# =====================================================================
#
#  Shadow rights table [j_rights][j_rights_temp][j_vsolr_timestamp] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item initialize_j_rights_temp

Description

=cut

# ---------------------------------------------------------------------
sub initialize_j_rights_temp {
    my($C, $dbh) = @_;
    
    my ($statement, $sth);

    $statement = qq{DROP TABLE IF EXISTS j_rights_temp};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    $statement = qq{CREATE TABLE `j_rights_temp` (`nid` varchar(32) NOT NULL default '', `attr` tinyint(4) NOT NULL default '0', `reason` tinyint(4) NOT NULL default '0', `source` tinyint(4) NOT NULL default '0', `user` varchar(32) NOT NULL default '', `time` timestamp NOT NULL default CURRENT_TIMESTAMP, `sysid` varchar(32) NOT NULL default '', `update_time` int NOT NULL default '00000000', PRIMARY KEY (`nid`), KEY `update_time` (`update_time`), KEY `attr` (`attr`))};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}

# ---------------------------------------------------------------------

=item Drop_j_rights_Rename_j_rights_temp

Description

=cut

# ---------------------------------------------------------------------
sub Drop_j_rights_Rename_j_rights_temp {
    my ($C, $dbh) = @_;

    my ($statement, $sth);
    
    $statement = qq{DROP TABLE j_rights};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    $statement = qq{RENAME TABLE j_rights_temp TO j_rights};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}


# ---------------------------------------------------------------------

=item init_vSolr_timestamp

Description

=cut

# ---------------------------------------------------------------------
sub init_vSolr_timestamp {
    my ($C, $dbh, $time) = @_;

    my ($statement, $sth);

    my $timestamp = defined($time) ? $time : $Db::vSOLR_ZERO_TIMESTAMP;
    $statement = qq{DELETE FROM j_vsolr_timestamp};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    $statement = qq{INSERT INTO j_vsolr_timestamp SET time=$timestamp};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item Select_vSolr_timestamp

A pointer into j_rights

=cut

# ---------------------------------------------------------------------
sub Select_vSolr_timestamp {
    my($C, $dbh) = @_;

    my $statement = qq{SELECT time FROM j_vsolr_timestamp};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $timestamp = $sth->fetchrow_array || $Db::vSOLR_ZERO_TIMESTAMP;
    DEBUG('lsdb', qq{DEBUG: $statement ::: $timestamp});

    return $timestamp;
}

# ---------------------------------------------------------------------

=item update_vSolr_timestamp

Description

=cut

# ---------------------------------------------------------------------
sub update_vSolr_timestamp {
    my($C, $dbh, $Rebuild) = @_;

    my ($statement, $sth);

    my $J_RIGHTS_TABLE_NAME = ($Rebuild ? 'j_rights_temp' : 'j_rights');

    $statement = qq{SELECT MAX(update_time) FROM $J_RIGHTS_TABLE_NAME};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $latest_timestamp = $sth->fetchrow_array || $Db::vSOLR_ZERO_TIMESTAMP;
    DEBUG('lsdb', qq{DEBUG: $statement ::: $latest_timestamp});

    $statement = qq{UPDATE j_vsolr_timestamp SET time=$latest_timestamp};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}


# ---------------------------------------------------------------------

=item Select_count_from_j_rights

Description

=cut

# ---------------------------------------------------------------------
sub Select_count_from_j_rights {
    my ($C, $dbh) = @_;

    my $statement = qq{SELECT count(*) FROM j_rights};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $size = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: $size});

    return $size;
}

# ---------------------------------------------------------------------

=item Select_count_from_j_rights_temp

Description

=cut

# ---------------------------------------------------------------------
sub Select_count_from_j_rights_temp {
    my ($C, $dbh) = @_;

    my $statement = qq{SELECT count(*) FROM j_rights_temp};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $size = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: $size});

    return $size;
}


# ---------------------------------------------------------------------

=item Select_latest_rights_row

Description

=cut

# ---------------------------------------------------------------------
sub Select_latest_rights_row {
    my ($C, $dbh, $namespace, $id) = @_;

    my $statement =
        qq{SELECT CONCAT(namespace, '.', id) AS nid, attr, reason, source, user, time FROM rights_current WHERE namespace='$namespace' AND id='$id'};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $row_hashref = $sth->fetchrow_hashref();

    return $row_hashref;
}


# ---------------------------------------------------------------------

=item Replace_j_rights_id

We set the query timestamp back in time so there is overlap. The table
starts out empty during a full rebuild. nid in j_rights table is
PRIMARY KEY so an nid can't appear more than once.

=cut

# ---------------------------------------------------------------------
sub Replace_j_rights_id {
    my ($C, $dbh, $hashref, $Check_only, $Rebuild) = @_;

    # From mdp.rights, currently
    my $attr = $hashref->{'attr'};
    my $reason = $hashref->{'reason'};
    my $source = $hashref->{'source'};
    my $user = $hashref->{'user'};
    my $time = $hashref->{'time'};

    # From vSolr query result
    my $nid = $hashref->{'nid'};
    # For reasons unknown, we sometimes have trailing spaces
    Utils::trim_spaces(\$nid);

    my $sysid = $hashref->{'sysid'};
    my $updateTime_in_vSolr = $hashref->{'timestamp_of_nid'};

    my $case;
    my ($statement, $sth);

    my $J_RIGHTS_TABLE_NAME = ($Rebuild ? 'j_rights_temp' : 'j_rights');

    # See what we already have in $J_RIGHTS_TABLE_NAME
    $statement = qq{SELECT nid, update_time, sysid FROM $J_RIGHTS_TABLE_NAME WHERE nid='$nid'};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $ref_to_arr_of_hashref = $sth->fetchall_arrayref({});

    my $nid_exists_in_j_rights = $ref_to_arr_of_hashref->[0]->{'nid'};
    my $sysid_in_j_rights = $ref_to_arr_of_hashref->[0]->{'sysid'};
    my $updateTime_in_j_rights = $ref_to_arr_of_hashref->[0]->{'update_time'} || $Db::vSOLR_ZERO_TIMESTAMP;

    # Pass the j_rights timestamp in the input hashref
    $hashref->{'timestamp_in_j_rights'} = $updateTime_in_j_rights;
    # Pass the j_rights_sysid in the input hashref
    $hashref->{'sysid_in_j_rights'} = $sysid_in_j_rights;

    if (! $nid_exists_in_j_rights) {
        # CASE: nid is not in $J_RIGHTS_TABLE_NAME ==> NEW. Insert
        $case = 'NEW';
        DEBUG('lsdb', qq{DEBUG: $statement ::: (A) NEW});
    }
    else {
        # If nid's update_time is the is same as update_time recorded
        # in J_RIGHTS_TABLE_NAME then we're seeing an update we
        # already recorded due to range query [last_run_time-2d TO *]. 
        # Use '<=' even though it should be impossible for the nid
        # timestamp we are seeing now to be older than what we
        # recorded when we saw it for the first time.
        if ($updateTime_in_vSolr <= $updateTime_in_j_rights) {
            $case = 'NOOP';
            DEBUG('lsdb', qq{DEBUG: $statement ::: NOOP});
        }
        else {
            # Seen but updated since last save to J_RIGHTS_TABLE_NAME
            if ($sysid_in_j_rights eq $sysid) {
                # CASE: nid from vSolr newer (>) that timestamp in
                # j_rights, same sysid: UPDATED
                $case = 'UPDATED';
                DEBUG('lsdb', qq{DEBUG: $statement ::: (D) UPDATED});
            }
            else {
                # CASE: nid in j_rights but different sysid: MOVED
                $case = 'MOVED';
                DEBUG('lsdb', qq{DEBUG: $statement ::: (C) MOVED});
            }
        }
    }

    $statement = qq{REPLACE INTO $J_RIGHTS_TABLE_NAME SET nid='$nid', attr=$attr, reason=$reason, source=$source, user='$user', time='$time', sysid='$sysid', update_time=$updateTime_in_vSolr};
    DEBUG('lsdb', qq{DEBUG [Check=$Check_only, case=$case]: $statement});

    if (! $Check_only) {
        if ($case ne 'NOOP') {
            # insert or replace
            $sth = DbUtils::prep_n_execute($dbh, $statement);
        }
    }

    return $case;
}

# ---------------------------------------------------------------------

=item Select_j_rights_id_attr

Get the current attr value for the id in j_rights.

=cut

# ---------------------------------------------------------------------
sub Select_j_rights_id_attr
{
    my ($C, $dbh, $nid) = @_;

    my $statement = qq{SELECT attr FROM j_rights WHERE nid='$nid'};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $attr = $sth->fetchrow_array() || 0;

    return $attr;
}


# ---------------------------------------------------------------------

=item Select_j_rights_id_sysid

Get the current sysid value for the id in j_rights.

=cut

# ---------------------------------------------------------------------
sub Select_j_rights_id_sysid
{
    my ($C, $dbh, $nid) = @_;

    my $statement = qq{SELECT sysid FROM j_rights WHERE nid='$nid'};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $sysid = $sth->fetchrow_array() || 0;

    return $sysid;
}




# =====================================================================
# =====================================================================
#
#  Queue tables [j_rights_timestamp][j_queue][j_errors][j_timeouts] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item Test_j_rights_timestamp

Simple test to see if a run exists.

=cut

# ---------------------------------------------------------------------
sub Test_j_rights_timestamp {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT count(*) FROM j_rights_timestamp WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $ct = $sth->fetchrow_array;
    DEBUG('lsdb', qq{DEBUG: $statement ::: $ct});

    return $ct;
}


# ---------------------------------------------------------------------

=item Select_j_rights_timestamp

Description: holds timestamp into j_rights when last enqueue to j_queue
occured.

=cut

# ---------------------------------------------------------------------
sub Select_j_rights_timestamp {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT time FROM j_rights_timestamp WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $timestamp = $sth->fetchrow_array || $Db::vSOLR_ZERO_TIMESTAMP;
    DEBUG('lsdb', qq{DEBUG: $statement ::: $timestamp});

    return $timestamp;
}


# ---------------------------------------------------------------------

=item update_j_rights_timestamp

Description: update timestamp into j_rights when last enqueue to j_queue
occured.

=cut

# ---------------------------------------------------------------------
sub update_j_rights_timestamp {
    my ($C, $dbh, $run, $timestamp) = @_;

    my $statement = qq{UPDATE j_rights_timestamp SET time=$timestamp WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item init_j_rights_timestamp

Description

=cut

# ---------------------------------------------------------------------
sub init_j_rights_timestamp {
    my ($C, $dbh, $run, $time) = @_;

    my $timestamp = defined($time) ? $time : $Db::vSOLR_ZERO_TIMESTAMP;
    my $statement = qq{REPLACE INTO j_rights_timestamp SET run=$run, time=$timestamp};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item delete_j_rights_timestamp

Description

=cut

# ---------------------------------------------------------------------
sub delete_j_rights_timestamp {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM j_rights_timestamp WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item Renumber_j_rights_timestamp

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_j_rights_timestamp {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE j_rights_timestamp SET run=$to_run WHERE run=$from_run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item Select_id_slice_from_queue

Description

=cut

# ---------------------------------------------------------------------
sub Select_id_slice_from_queue {
    my ($C, $dbh, $run, $pid, $host, $slice_size) = @_;

    my $sth;
    my $statement;

    my $proc_status = $SLIP_Utils::States::Q_AVAILABLE;

    $statement = qq{LOCK TABLES j_queue WRITE};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    # mark a slice of available ids as being processed by a producer
    # process
    $statement = qq{UPDATE j_queue SET pid=$pid, host='$host', proc_status=$SLIP_Utils::States::Q_PROCESSING WHERE run=$run AND proc_status=$proc_status LIMIT $slice_size};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    # get the ids in the slice just marked for this process
    $statement = qq{SELECT id FROM j_queue WHERE run=$run AND proc_status=$SLIP_Utils::States::Q_PROCESSING AND pid=$pid AND host='$host'; };
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $ref_to_ary_of_hashref = $sth->fetchall_arrayref({});

    DEBUG('lsdb', qq{DEBUG: SELECT returned num_items=} . scalar(@$ref_to_ary_of_hashref));

    $statement = qq{UNLOCK TABLES};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    return $ref_to_ary_of_hashref;
}


# ---------------------------------------------------------------------

=item Delete_queue

Description

=cut

# ---------------------------------------------------------------------
my $DELETE_Q_SLICE_SIZE = 10000;
sub Delete_queue {
    my ($C, $dbh, $run) = @_;

    my $num_affected = 0;
    do {
        my $begin = time();

        my $statement = qq{DELETE FROM j_queue WHERE run=$run LIMIT $DELETE_Q_SLICE_SIZE};
        DEBUG('lsdb', qq{DEBUG: $statement});
        my $sth = DbUtils::prep_n_execute($dbh, $statement, \$num_affected);

        my $elapsed = time() - $begin;
        sleep $elapsed/2;

    } until ($num_affected <= 0);
}

# ---------------------------------------------------------------------

=item Renumber_queue

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_queue {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE j_queue SET run=$to_run WHERE run=$from_run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item insert_queue_items

Description: does not advance timestamp in j_rights_timestamp.  Just
used for static testing.

=cut

# ---------------------------------------------------------------------
sub insert_queue_items {
    my ($C, $dbh, $run, $ref_to_ary_of_ids) = @_;

    my $sth;
    my $statement;
    my $num_inserted = 0;

    $statement = qq{LOCK TABLES j_queue WRITE};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    foreach my $id (@$ref_to_ary_of_ids) {
        $statement = qq{REPLACE INTO j_queue SET run=$run, id='$id', pid=0, host='', proc_status=$SLIP_Utils::States::Q_AVAILABLE};
        DEBUG('lsdb', qq{DEBUG: $statement});
        $sth = DbUtils::prep_n_execute($dbh, $statement);
        $num_inserted++;
    }

    $statement = qq{UNLOCK TABLES};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    return $num_inserted;
}


# ---------------------------------------------------------------------

=item __get_update_time_WHERE_clause

Description

=cut

# ---------------------------------------------------------------------
sub __get_update_time_WHERE_clause {
    my ($C, $dbh, $run) = @_;
    
    my $timestamp = Db::Select_j_rights_timestamp($C, $dbh, $run);
    my $WHERE_clause;
    if ($timestamp eq $Db::vSOLR_ZERO_TIMESTAMP) {
        $WHERE_clause = qq{ WHERE update_time >= $timestamp};
    }
    else {
        $WHERE_clause = qq{ WHERE update_time > $timestamp};
    }

    return $WHERE_clause;
}

# ---------------------------------------------------------------------

=item count_insert_latest_into_queue

Coupled to insert_latest_into_queue via update_time > $timestamp

=cut

# ---------------------------------------------------------------------
sub count_insert_latest_into_queue {
    my ($C, $dbh, $run) = @_;

    # NOTE: non-overlap (>) Talk to Tim and see
    # insert_latest_into_queue(). If timestamp is 0, we want all.
    my $WHERE_clause = __get_update_time_WHERE_clause($C, $dbh, $run);
    my $statement = qq{SELECT count(*) FROM j_rights } . $WHERE_clause;
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $num = $sth->fetchrow_array || 0;

    return $num;
}

# ---------------------------------------------------------------------

=item insert_latest_into_queue

Coupled to count_insert_latest_into_queue via update_time > $timestamp

=cut

# ---------------------------------------------------------------------
sub insert_latest_into_queue {
    my ($C, $dbh, $run) = @_;

    my ($sth, $statement);

    # Lock
    $statement = qq{LOCK TABLES j_rights WRITE, j_queue WRITE, j_rights_timestamp WRITE};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    # Get the timestamp of the newest items last enqueued from
    # j_rights into j_queue. NOTE: non-overlap (>) Talk to Tim and see
    # count_insert_latest_into_queue()
    my $WHERE_clause = __get_update_time_WHERE_clause($C, $dbh, $run);
    my $SELECT_clause =
      qq{SELECT $run AS run, nid AS id, 0 AS pid, '' AS host, $SLIP_Utils::States::Q_AVAILABLE AS proc_status FROM j_rights} 
        . $WHERE_clause;

    $statement = qq{INSERT INTO j_queue ($SELECT_clause)};
    my $num_inserted = 0;
    $sth = DbUtils::prep_n_execute($dbh, $statement, \$num_inserted);
    DEBUG('lsdb', qq{DEBUG: $statement ::: inserted=$num_inserted});

    # Get the maximum update_time in j_rights to use as the new timestamp.
    $statement = qq{SELECT MAX(update_time) FROM j_rights};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $new_timestamp = $sth->fetchrow_array;
    Db::update_j_rights_timestamp($C, $dbh, $run, $new_timestamp);

    # Unlock
    $statement = qq{UNLOCK TABLES};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    return $num_inserted;
}


# ---------------------------------------------------------------------

=item dequeue

Description

=cut

# ---------------------------------------------------------------------
sub dequeue {
    my ($C, $dbh, $run, $id, $pid, $host) = @_;

    my $statement = qq{DELETE FROM j_queue WHERE run=$run AND id='$id' AND pid=$pid AND host='$host'};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item Delete_id_from_j_queue

Description

=cut

# ---------------------------------------------------------------------
sub Delete_id_from_j_queue {
    my ($C, $dbh, $run, $id) = @_;

    my $statement = qq{DELETE FROM j_queue WHERE run=$run AND id='$id'};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item update_unstick_inprocess

Description

=cut

# ---------------------------------------------------------------------
sub update_unstick_inprocess {
    my ($C, $dbh, $run) = @_;

    my $sth;
    my $statement;

    $statement = qq{SELECT count(*) FROM j_queue WHERE run=$run AND proc_status=$SLIP_Utils::States::Q_PROCESSING};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $num_inprocess = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: inprocess=$num_inprocess});

    if ($num_inprocess > 0) {
        # Mark a slice of ids being processed by a producer process as
        # available
        $statement = qq{UPDATE j_queue SET proc_status=$SLIP_Utils::States::Q_AVAILABLE WHERE run=$run AND proc_status=$SLIP_Utils::States::Q_PROCESSING};
        $sth = DbUtils::prep_n_execute($dbh, $statement);
        DEBUG('lsdb', qq{DEBUG: $statement});
    }

    return $num_inprocess;
}


# ---------------------------------------------------------------------

=item insert_restore_errors_to_queue

Description

=cut

# ---------------------------------------------------------------------
sub insert_restore_errors_to_queue {
    my ($C, $dbh, $run) = @_;

    my $sth;
    my $statement;
    my $num_inserted = 0;

    # Lock
    $statement = qq{LOCK TABLES j_errors WRITE, j_queue WRITE};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    $statement = qq{SELECT id FROM j_errors WHERE run=$run};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $ref_to_ary_of_hashref = $sth->fetchall_arrayref({});
    foreach my $ref (@$ref_to_ary_of_hashref) {
        my $id = $ref->{'id'};
        my $num = 0;
        $statement = qq{INSERT INTO j_queue SET run=$run, id='$id', pid=0, host='', proc_status=$SLIP_Utils::States::Q_AVAILABLE};
        $sth = DbUtils::prep_n_execute($dbh, $statement, \$num);
        DEBUG('lsdb', qq{DEBUG: $statement});
        $num_inserted += $num;
    }

    $statement = qq{DELETE FROM j_errors WHERE run=$run};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    $statement = qq{UNLOCK TABLES};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    return $num_inserted;
}


# ---------------------------------------------------------------------

=item insert_restore_timeouts_to_queue

Description

=cut

# ---------------------------------------------------------------------
sub insert_restore_timeouts_to_queue {
    my ($C, $dbh, $run) = @_;

    my $sth;
    my $statement;

    # Lock
    $statement = qq{LOCK TABLES j_timeouts WRITE, j_queue WRITE};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $SELECT_clause =
        qq{SELECT $run AS run, id AS id, 0 AS pid, '' AS host, $SLIP_Utils::States::Q_AVAILABLE AS proc_status FROM j_timeouts WHERE run=$run};

    $statement = qq{INSERT INTO j_queue ($SELECT_clause)};
    my $num_inserted = 0;
    $sth = DbUtils::prep_n_execute($dbh, $statement, \$num_inserted);
    DEBUG('lsdb', qq{DEBUG: $statement ::: inserted=$num_inserted});

    $statement = qq{DELETE FROM j_timeouts WHERE run=$run};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    $statement = qq{UNLOCK TABLES};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    return $num_inserted;
}

# ---------------------------------------------------------------------

=item Select_timeouts_count

idempotent

=cut

# ---------------------------------------------------------------------
sub Select_timeouts_count {
    my ($C, $dbh, $run, $shard) = @_;

    my $AND_clause = defined($shard) ? qq{ AND shard=$shard } : '';
    my $statement =
        qq{SELECT count(*) FROM j_timeouts WHERE run=$run}
            . $AND_clause;
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $count = $sth->fetchrow_array() || 0;

    return $count;
}

# ---------------------------------------------------------------------

=item Select_queue_data

Description: for reporting

=cut

# ---------------------------------------------------------------------
sub Select_queue_data {
    my ($C, $dbh, $run) = @_;

    my $sth;
    my $statement;

    $statement = qq{SELECT count(*) from j_queue WHERE run=$run; };
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $queue_size = $sth->fetchrow_array();

    $statement = qq{SELECT count(*) from j_queue WHERE run=$run AND proc_status=$SLIP_Utils::States::Q_AVAILABLE; };
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $queue_num_available = $sth->fetchrow_array();

    $statement = qq{SELECT count(*) from j_queue WHERE run=$run AND proc_status=$SLIP_Utils::States::Q_PROCESSING; };
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $queue_num_in_process = $sth->fetchrow_array();

    return ($queue_size, $queue_num_available, $queue_num_in_process);
}

# ---------------------------------------------------------------------

=item Select_timeout_ids

Description

=cut

# ---------------------------------------------------------------------
sub Select_timeout_ids {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT id, pid, host, timeout_time FROM j_timeouts WHERE run=$run AND shard=$shard};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $id_arr_hashref = $sth->fetchall_arrayref({});

    return $id_arr_hashref;
}


# ---------------------------------------------------------------------

=item Select_tot_error_count

Description

=cut

# ---------------------------------------------------------------------
sub Select_tot_error_count {
    my ($C, $dbh, $run) = @_;

    my $sth;
    my $statement;

    $statement = qq{SELECT count(*) from j_errors WHERE run=$run};
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $num_errors = $sth->fetchrow_array() || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: num_errors=$num_errors});

    return $num_errors;
}


# ---------------------------------------------------------------------

=item Select_error_data

Description for reporting and error abort

=cut

# ---------------------------------------------------------------------
sub Select_error_data {
    my ($C, $dbh, $run, $shard) = @_;

    my $sth;
    my $statement;

    $statement = qq{SELECT count(*) from j_errors WHERE run=$run AND shard=$shard; };
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $num_errors = $sth->fetchrow_array() || 0;

    $statement = qq{SELECT count(*) from j_errors WHERE run=$run AND shard=$shard AND reason=$C_INDEX_FAILURE};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $num_I = $sth->fetchrow_array() || 0;

    $statement = qq{SELECT count(*) from j_errors WHERE run=$run AND shard=$shard AND reason=$C_DATA_FAILURE};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $num_O = $sth->fetchrow_array() || 0;

    $statement = qq{SELECT count(*) from j_errors WHERE run=$run AND shard=$shard AND reason=$C_METADATA_FAILURE};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $num_M = $sth->fetchrow_array() || 0;

    $statement = qq{SELECT count(*) from j_errors WHERE run=$run AND shard=$shard AND reason=$C_CRITICAL_FAILURE};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $num_C = $sth->fetchrow_array() || 0;

    $statement = qq{SELECT count(*) from j_errors WHERE run=$run AND shard=$shard AND reason=$C_SERVER_GONE};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $num_S = $sth->fetchrow_array() || 0;

    $statement = qq{SELECT count(*) from j_errors WHERE run=$run AND shard=$shard AND reason=$C_NO_INDEXER_AVAIL};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $num_N = $sth->fetchrow_array() || 0;

    return ($num_errors, $num_I, $num_O, $num_M, $num_C, $num_S, $num_N);
}

# ---------------------------------------------------------------------

=item Select_error_ids

Description: for reporting ids of errors, by type

=cut

# ---------------------------------------------------------------------
sub Select_error_ids {
    my ($C, $dbh, $run, $shard, $reason) = @_;

    my $AND_shard_clause = defined($shard) ? qq{AND shard=$shard} : '';
    
    my $sth;
    my $statement = qq{SELECT id, pid, host, error_time FROM j_errors WHERE run=$run AND reason=$reason $AND_shard_clause};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $id_arr_hashref = $sth->fetchall_arrayref({});

    return $id_arr_hashref;
}

# ---------------------------------------------------------------------

=item Select_id_from_j_errors

Sniff error queue

=cut

# ---------------------------------------------------------------------
sub Select_id_from_j_errors {
    my ($C, $dbh, $run, $id) = @_;

    my $sth;
    my $statement;

    $statement = qq{SELECT reason FROM j_errors WHERE run=$run AND id='$id'};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $reason = $sth->fetchrow_array() || $C_NO_ERROR;

    return $reason;
}


# ---------------------------------------------------------------------

=item insert_item_id_error

Description; idempotent

=cut

# ---------------------------------------------------------------------
sub insert_item_id_error {
    my ($C, $dbh, $run, $shard, $id, $pid, $host, $index_state) = @_;

    my $statement =
        qq{REPLACE INTO j_errors SET run=$run, shard=$shard, id='$id', pid=$pid, host='$host', error_time=CURRENT_TIMESTAMP, reason=$index_state};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    # If called to handle critical errors must dequeue here due to
    # longjump out of processing loop that normally does the dequeue
    # call.
    dequeue($C, $dbh, $run, $id, $pid, $host);
}



# ---------------------------------------------------------------------

=item Delete_errors

Description

=cut

# ---------------------------------------------------------------------
sub Delete_errors {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM j_errors WHERE run=$run};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $ct = 0;
    my $sth = DbUtils::prep_n_execute($dbh, $statement, \$ct);
    
    return ($ct == '0E0') ? 0 : $ct;;
}


# ---------------------------------------------------------------------

=item Renumber_errors

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_errors {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE j_errors SET run=$to_run WHERE run=$from_run};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
}


# ---------------------------------------------------------------------

=item insert_item_id_timeout

Description

=cut

# ---------------------------------------------------------------------
sub insert_item_id_timeout {
    my ($C, $dbh, $run, $id, $shard, $pid, $host) = @_;

    my $statement =
        qq{INSERT INTO j_timeouts SET run=$run, id='$id', shard=$shard, pid=$pid, host='$host', timeout_time=CURRENT_TIMESTAMP};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item delete_timeouts

Description

=cut

# ---------------------------------------------------------------------
sub delete_timeouts {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM j_timeouts WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# =====================================================================
# =====================================================================
#
#                      Index size table [j_index_size] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item update_indexdir_size

Description

=cut

# ---------------------------------------------------------------------
sub update_indexdir_size {
    my ($C, $dbh, $run, $shard, $index_size) = @_;

    my $sth;
    my $statement;

    $statement = qq{REPLACE INTO j_index_size SET run=$run, shard=$shard, du=$index_size};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}

# ---------------------------------------------------------------------

=item Select_indexdir_size

Description

=cut

# ---------------------------------------------------------------------
sub Select_indexdir_size {
    my ($C, $dbh, $run, $shard) = @_;

    # Index size
    my $statement = qq{SELECT du FROM j_index_size WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $index_size = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: $index_size});

    return $index_size;
}

# ---------------------------------------------------------------------

=item Renumber_Index_size

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_Index_size {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE j_index_size SET run=$to_run WHERE run=$from_run};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
}


# ---------------------------------------------------------------------

=item Reset_Index_size

Description

=cut

# ---------------------------------------------------------------------
sub Reset_Index_size {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{UPDATE j_index_size SET du=0 WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}
# ---------------------------------------------------------------------

=item delete_Index_size

Description

=cut

# ---------------------------------------------------------------------
sub delete_Index_size {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM j_index_size WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# =====================================================================
# =====================================================================
#
#                         Indexed [j_indexed] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item Delete_indexed

Description

=cut

# ---------------------------------------------------------------------
my $DELETE_SLICE_SIZE = 10000;

sub Delete_indexed {
    my ($C, $dbh, $run) = @_;

    my ($statement, $sth);

    my $num_affected = 0;
    do {
        my $begin = time();
        
        $statement = qq{LOCK TABLES j_indexed WRITE};
        DEBUG('lsdb', qq{DEBUG: $statement});
        $sth = DbUtils::prep_n_execute($dbh, $statement);

        $statement = qq{DELETE FROM j_indexed WHERE run=$run LIMIT $DELETE_SLICE_SIZE};
        DEBUG('lsdb', qq{DEBUG: $statement});
        $sth = DbUtils::prep_n_execute($dbh, $statement, \$num_affected);

        $statement = qq{UNLOCK TABLES};
        DEBUG('lsdb', qq{DEBUG: $statement});
        $sth = DbUtils::prep_n_execute($dbh, $statement);

        my $elapsed = time() - $begin;
        sleep $elapsed/2;
        
    } until ($num_affected <= 0);
}

# ---------------------------------------------------------------------

=item insert_item_id_indexed

idempotent

=cut

# ---------------------------------------------------------------------
sub insert_item_id_indexed {
    my ($C, $dbh, $run, $shard, $id) = @_;

    my ($statement, $sth);

    $statement = qq{SELECT indexed_ct FROM j_indexed WHERE run=$run AND id='$id' AND shard=$shard};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $indexed_ct = $sth->fetchrow_array() || 0;
    $indexed_ct++;

    $statement = qq{REPLACE INTO j_indexed SET run=$run, shard=$shard, id='$id', time=CURRENT_TIMESTAMP, indexed_ct=$indexed_ct};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    return ($indexed_ct > 1);
}

# ---------------------------------------------------------------------

=item Delete_item_id_indexed

To handle Deletes

=cut

# ---------------------------------------------------------------------
sub Delete_item_id_indexed {
    my ($C, $dbh, $run, $shard, $id) = @_;

    my $statement = qq{DELETE FROM j_indexed WHERE run=$run AND id='$id' AND shard=$shard};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
}

# ---------------------------------------------------------------------

=item Reset_indexed_ct

For a given run to check how may were re-indexed, i.e. indexed_ct > 1.

=cut

# ---------------------------------------------------------------------
sub Reset_indexed_ct {
    my ($C, $dbh, $run) = @_;

    my ($statement, $sth);

    $statement = qq{UPDATE j_indexed SET indexed_ct=1 WHERE run=$run};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}

# ---------------------------------------------------------------------

=item Select_item_id_shard

idempotent

=cut

# ---------------------------------------------------------------------
sub Select_item_id_shard {
    my ($C, $dbh, $run, $id) = @_;

    my $statement = qq{SELECT shard FROM j_indexed WHERE run=$run AND id='$id'};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $shard = $sth->fetchrow_array() || 0;

    return $shard;
}

# ---------------------------------------------------------------------

=item Select_indexed_count

idempotent

=cut

# ---------------------------------------------------------------------
sub Select_indexed_count {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT count(*) FROM j_indexed WHERE run=$run AND shard=$shard};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $count = $sth->fetchrow_array() || 0;

    return $count;
}

# ---------------------------------------------------------------------

=item Select_indexed_tot_count

idempotent

=cut

# ---------------------------------------------------------------------
sub Select_indexed_tot_count {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT count(*) FROM j_indexed WHERE run=$run};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $count = $sth->fetchrow_array() || 0;

    return $count;
}

# ---------------------------------------------------------------------

=item Select_reindexed_tot_count

Depends on Reset_indexed_ct being called before a daily run.

=cut

# ---------------------------------------------------------------------
sub Select_reindexed_tot_count {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT count(*) FROM j_indexed WHERE run=$run AND indexed_ct > 1};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $count = $sth->fetchrow_array() || 0;

    return $count;
}


# ---------------------------------------------------------------------

=item Delete_id_from_shard

Description

=cut

# ---------------------------------------------------------------------
sub Delete_id_from_shard {
    my ($C, $dbh, $run, $id, $shard) = @_;

    my $statement = qq{DELETE FROM j_indexed WHERE run=$run AND id='$id' AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item Renumber_indexed

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_indexed {
    my ($C, $dbh, $from_run, $to_run) = @_;
    
    my $statement = qq{UPDATE j_indexed SET run=$to_run WHERE run=$from_run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item Delete_id_from_j_rights

Description

=cut

# ---------------------------------------------------------------------
sub Delete_id_from_j_rights {
    my ($C, $dbh, $id) = @_;

    my $statement = qq{DELETE FROM j_rights WHERE nid='$id'};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# =====================================================================
# =====================================================================
#
#        Stats tables [j_shard_stats][j_rate_stats] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item Reset_shard_stats

Description

=cut

# ---------------------------------------------------------------------
sub Reset_shard_stats {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM j_shard_stats WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item update_shard_stats

Update shard stats only if the id has not been indexed yet.  Error
redo and various re-enqueuing would increase the shard/checkpoint
counts is we didn't make this check.

=cut

# ---------------------------------------------------------------------
sub update_shard_stats {
    my ($C, $dbh, $run, $shard, $doc_size, $doc_time, $idx_time, $tot_time) = @_;

    my $sth;
    my $statement;

    $statement = qq{LOCK TABLES j_shard_stats WRITE};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    $statement = qq{SELECT s_num_docs, s_doc_size, s_doc_time, s_idx_time, s_tot_time FROM j_shard_stats WHERE run=$run AND shard=$shard};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    my ($s_num_docs, $s_doc_size, $s_doc_time, $s_idx_time, $s_tot_time);

    my $row_hashref = $sth->fetchrow_hashref();
    if (! $row_hashref) {
        # initialize
        $s_num_docs = 1;
        $s_doc_size = $doc_size || 0;
        $s_doc_time = $doc_time || 0;
        $s_idx_time = $idx_time || 0;
        $s_tot_time = $tot_time || 0;

        $statement = qq{INSERT INTO j_shard_stats SET run=$run, shard=$shard, s_num_docs=$s_num_docs, s_doc_size=$s_doc_size, s_doc_time=$s_doc_time, s_idx_time=$s_idx_time, s_tot_time=$s_tot_time};
        DEBUG('lsdb', qq{DEBUG: $statement});
        $sth = DbUtils::prep_n_execute($dbh, $statement);
    }
    else {
        # accumulate
        $s_num_docs = $$row_hashref{'s_num_docs'} + 1;
        $s_doc_size = $$row_hashref{'s_doc_size'} + $doc_size;
        $s_doc_time = $$row_hashref{'s_doc_time'} + $doc_time;
        $s_idx_time = $$row_hashref{'s_idx_time'} + $idx_time;
        $s_tot_time = $$row_hashref{'s_tot_time'} + $tot_time;

        $statement = qq{UPDATE j_shard_stats SET s_num_docs=$s_num_docs, s_doc_size=$s_doc_size, s_doc_time=$s_doc_time, s_idx_time=$s_idx_time, s_tot_time=$s_tot_time WHERE run=$run AND shard=$shard};
        DEBUG('lsdb', qq{DEBUG: $statement});
        $sth = DbUtils::prep_n_execute($dbh, $statement);
    }

    $statement = qq{UNLOCK TABLES};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    return ($s_num_docs, $s_doc_size, $s_doc_time, $s_idx_time, $s_tot_time);
}


# ---------------------------------------------------------------------

=item Select_shard_stats

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_stats {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT s_num_docs, s_doc_size, s_doc_time, s_idx_time, s_tot_time FROM j_shard_stats WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $row_hashref = $sth->fetchrow_hashref();

    my $s_num_docs = $$row_hashref{'s_num_docs'} || 0;
    my $s_doc_size = $$row_hashref{'s_doc_size'} || 0;
    my $s_doc_time = $$row_hashref{'s_doc_time'} || 0;
    my $s_idx_time = $$row_hashref{'s_idx_time'} || 0;
    my $s_tot_time = $$row_hashref{'s_tot_time'} || 0;

    return ($s_num_docs, $s_doc_size, $s_doc_time, $s_idx_time, $s_tot_time);
}


# ---------------------------------------------------------------------

=item Renumber_shard_stats 

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_shard_stats {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE j_shard_stats SET run=$to_run WHERE run=$from_run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item Reset_rate_stats

Description

=cut

# ---------------------------------------------------------------------
sub Reset_rate_stats {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM j_rate_stats WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item update_rate_stats

Description

=cut

# ---------------------------------------------------------------------
sub update_rate_stats {
    my ($C, $dbh, $run, $shard, $timeNow) = @_;

    my $sth;
    my $statement;
    my $ref_to_ary_of_hashref;

    $statement = qq{LOCK TABLES j_rate_stats WRITE};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    $statement = qq{SELECT * FROM j_rate_stats WHERE run=$run AND shard=$shard};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    # Initialize
    $ref_to_ary_of_hashref = $sth->fetchall_arrayref({});
    if (scalar(@$ref_to_ary_of_hashref) == 0) {
        $statement = qq{INSERT INTO j_rate_stats SET run=$run, shard=$shard};
        DEBUG('lsdb', qq{DEBUG: $statement});
        $sth = DbUtils::prep_n_execute($dbh, $statement);
    }

    $statement = qq{SELECT * FROM j_rate_stats WHERE run=$run AND shard=$shard};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    # No time delta if this is the first time the field has been updated
    # (default=0) so not possible to update the rate
    $ref_to_ary_of_hashref = $sth->fetchall_arrayref({});
    my $timeLast = $ref_to_ary_of_hashref->[0]->{'time_a_100'} || 0;
    if ($timeLast > 0) {
        my $deltaTime = $timeNow - $timeLast;
        my $docs_phour = $deltaTime ? sprintf("%0.2f", 100/$deltaTime*60*60) : 0;

        $statement = qq{UPDATE j_rate_stats SET time_a_100=$timeNow, rate_a_100=$docs_phour WHERE run=$run AND shard=$shard};
        DEBUG('lsdb', qq{DEBUG: $statement});
        $sth = DbUtils::prep_n_execute($dbh, $statement);
    }
    else {
        $statement = qq{UPDATE j_rate_stats SET time_a_100=$timeNow WHERE run=$run AND shard=$shard};
        DEBUG('lsdb', qq{DEBUG: $statement});
        $sth = DbUtils::prep_n_execute($dbh, $statement);
    }

    $statement = qq{UNLOCK TABLES};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}


# ---------------------------------------------------------------------

=item Select_rate_stats

Description

=cut

# ---------------------------------------------------------------------
sub Select_rate_stats {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT rate_a_100 FROM j_rate_stats WHERE run=$run AND shard=$shard};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $rate = $sth->fetchrow_array || 0;

    return $rate;
}


# ---------------------------------------------------------------------

=item Renumber_rate_stats

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_rate_stats {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE j_rate_stats SET run=$to_run WHERE run=$from_run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# =====================================================================
# =====================================================================
#
#    Control tables:  [j_shard_control] @@
#
# =====================================================================
# =====================================================================


# ---------------------------------------------------------------------

=item Renumber_shard_control

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_shard_control {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE j_shard_control SET run=$to_run WHERE run=$from_run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item delete_shard_control

Description

=cut

# ---------------------------------------------------------------------
sub delete_shard_control {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM j_shard_control WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item Select_shard_build_done

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_build_done {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT build_time FROM j_shard_control WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $time = $sth->fetchrow_array || '$Db::MYSQL_ZERO_TIMESTAMP';
    return $time;
}


# ---------------------------------------------------------------------

=item Select_shard_optimize_done

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_optimize_done {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT optimize_time FROM j_shard_control WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $time = $sth->fetchrow_array || '$Db::MYSQL_ZERO_TIMESTAMP';
    return $time;
}

# ---------------------------------------------------------------------

=item Select_shard_check_done

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_check_done {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT checkd_time FROM j_shard_control WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $time = $sth->fetchrow_array || '$Db::MYSQL_ZERO_TIMESTAMP';
    return $time;
}

# ---------------------------------------------------------------------

=item Select_shard_release_state

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_release_state {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT release_state FROM j_shard_control WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $state = $sth->fetchrow_array || 0;
    return $state;
}

# ---------------------------------------------------------------------

=item set_shard_release_state

Description

=cut

# ---------------------------------------------------------------------
sub set_shard_release_state {
    my ($C, $dbh, $run, $shard, $state) = @_;

    my $statement = qq{UPDATE j_shard_control SET release_state=$state WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item set_shard_build_done

Description

=cut

# ---------------------------------------------------------------------
sub set_shard_build_done {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{UPDATE j_shard_control SET build_time=NOW() WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item set_shard_optimize_done

Description

=cut

# ---------------------------------------------------------------------
sub set_shard_optimize_done {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{UPDATE j_shard_control SET optimize_time=NOW() WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item set_shard_check_done

Description

=cut

# ---------------------------------------------------------------------
sub set_shard_check_done {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{UPDATE j_shard_control SET checkd_time=NOW() WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item Reset_shard_control

Description

=cut

# ---------------------------------------------------------------------
sub Reset_shard_control {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{UPDATE j_shard_control SET build=0, optimiz=0, checkd=0 WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item init_shard_control

Description

=cut

# ---------------------------------------------------------------------
sub init_shard_control {
    my ($C, $dbh, $run, $shard) = @_;

    my ($statement, $sth);

    $statement = qq{DELETE FROM j_shard_control WHERE run=$run AND shard=$shard};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    $statement = qq{INSERT INTO j_shard_control SET run=$run, shard=$shard, enabled=0, suspended=0, build=0, optimiz=0, checkd=0, build_time='$Db::MYSQL_ZERO_TIMESTAMP', optimize_time='$Db::MYSQL_ZERO_TIMESTAMP', checkd_time='$Db::MYSQL_ZERO_TIMESTAMP', release_state=0};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item Select_run_num_shards_available

Description

=cut

# ---------------------------------------------------------------------
sub Select_run_num_shards_available {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT count(*) FROM j_shard_control WHERE run=$run AND enabled=1 AND suspended=0};

    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $num_enabled = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: num_enabled=$num_enabled});

    return $num_enabled;
}


# ---------------------------------------------------------------------

=item Select_shard_enabled

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_enabled {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT enabled, suspended FROM j_shard_control WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $ref_to_ary_of_hashref = $sth->fetchall_arrayref({});
    my $enabled = $ref_to_ary_of_hashref->[0]->{'enabled'};
    my $suspended = $ref_to_ary_of_hashref->[0]->{'suspended'};

    my $state = $enabled && (! $suspended);
    DEBUG('lsdb', qq{DEBUG: $statement ::: enabled=$state});

    return $state;
}


# ---------------------------------------------------------------------

=item update_shard_enabled

Description

=cut

# ---------------------------------------------------------------------
sub update_shard_enabled {
    my ($C, $dbh, $run, $shard, $enabled) = @_;

    my $statement = qq{UPDATE j_shard_control SET enabled=$enabled WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item suspend_shard

Description

=cut

# ---------------------------------------------------------------------
sub suspend_shard {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{UPDATE j_shard_control SET suspended=1 WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item unsuspend_shard

Description

=cut

# ---------------------------------------------------------------------
sub unsuspend_shard {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{UPDATE j_shard_control SET suspended=0 WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item shard_is_suspended

Description

=cut

# ---------------------------------------------------------------------
sub shard_is_suspended {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT suspended FROM j_shard_control WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $suspended = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: suspended=$suspended});

    return $suspended;
}

# ---------------------------------------------------------------------

=item set_shard_optimize_state

Description

=cut

# ---------------------------------------------------------------------
sub set_shard_optimize_state {
    my ($C, $dbh, $run, $shard, $state) = @_;

    my ($statement, $sth);

    $statement = qq{LOCK TABLES j_shard_control WRITE};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    # Error state is terminal
    my $current_state = Select_shard_optimize_state($C, $dbh, $run, $shard);
    if ($current_state == $SLIP_Utils::States::Sht_Optimize_Error) {
        $statement = qq{UNLOCK TABLES};
        DEBUG('lsdb', qq{DEBUG: $statement});
        $sth = DbUtils::prep_n_execute($dbh, $statement);

        return;
    }
    # POSSIBLY NOTREACHED

    $statement = qq{UPDATE j_shard_control SET optimiz=$state WHERE run=$run AND shard=$shard};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    $statement = qq{UNLOCK TABLES};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}

# ---------------------------------------------------------------------

=item Select_shard_optimize_state

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_optimize_state {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT optimiz FROM j_shard_control WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $state = $sth->fetchrow_array;

    return $state || $SLIP_Utils::States::Sht_Not_Optimized;
}

# ---------------------------------------------------------------------

=item set_shard_check_state

Description

=cut

# ---------------------------------------------------------------------
sub set_shard_check_state {
    my ($C, $dbh, $run, $shard, $state) = @_;

    my ($statement, $sth);

    $statement = qq{LOCK TABLES j_shard_control WRITE};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    # Error state is terminal
    my $current_state = Select_shard_check_state($C, $dbh, $run, $shard);
    if ($current_state == $SLIP_Utils::States::Sht_Check_Error) {
        $statement = qq{UNLOCK TABLES};
        DEBUG('lsdb', qq{DEBUG: $statement});
        $sth = DbUtils::prep_n_execute($dbh, $statement);

        return;
    }
    # POSSIBLY NOTREACHED

    $statement = qq{UPDATE j_shard_control SET checkd=$state WHERE run=$run AND shard=$shard};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    $statement = qq{UNLOCK TABLES};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}

# ---------------------------------------------------------------------

=item Select_shard_check_state

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_check_state {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT checkd FROM j_shard_control WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $state = $sth->fetchrow_array;

    return $state || $SLIP_Utils::States::Sht_Not_Checked;
}

# ---------------------------------------------------------------------

=item set_shard_build_error

Description

=cut

# ---------------------------------------------------------------------
sub set_shard_build_error {
    my ($C, $dbh, $run, $shard) = @_;

    my ($statement, $sth);

    $statement = qq{LOCK TABLES j_shard_control WRITE};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    $statement = qq{UPDATE j_shard_control SET build=$SLIP_Utils::States::Sht_Build_Error WHERE run=$run AND shard=$shard};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    $statement = qq{UNLOCK TABLES};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}

# ---------------------------------------------------------------------

=item Select_shard_build_state

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_build_state {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT build FROM j_shard_control WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $state = $sth->fetchrow_array || $SLIP_Utils::States::Sht_No_Build_Error;

    return $state;
}


# =====================================================================
# =====================================================================
#
#    Control tables:  [j_host_control] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item Renumber_host_control

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_host_control {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE j_host_control SET run=$to_run WHERE run=$from_run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item Reset_host_control

Description

=cut

# ---------------------------------------------------------------------
sub Reset_host_control {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM j_host_control WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item Select_host_config

Description

=cut

# ---------------------------------------------------------------------
sub Select_hosts_config {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT host, num_producers, enabled FROM j_host_control WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $ref_to_ary_of_hashref = $sth->fetchall_arrayref({});

    return $ref_to_ary_of_hashref;
}


# ---------------------------------------------------------------------

=item Select_host_enabled

Description

=cut

# ---------------------------------------------------------------------
sub Select_host_enabled {
    my ($C, $dbh, $run, $host) = @_;

    my $statement = qq{SELECT enabled FROM j_host_control WHERE run=$run AND host='$host'};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $enabled = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: enabled=$enabled});

    return $enabled;
}


# ---------------------------------------------------------------------

=item Select_num_producers

Description

=cut

# ---------------------------------------------------------------------
sub Select_num_producers {
    my ($C, $dbh, $run, $host) = @_;

    my $statement = qq{SELECT num_producers FROM j_host_control WHERE run=$run AND host='$host'};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $num_producers_configured = $sth->fetchrow_array || 0;

    return $num_producers_configured;
}


# ---------------------------------------------------------------------

=item update_host_num_producers

Serves to initialize rows as well.

=cut

# ---------------------------------------------------------------------
sub update_host_num_producers {
    my ($C, $dbh, $run, $num_producers, $host) = @_;

    my $sth;
    my $statement;

    # if producers are currently enabled for this host, keep them
    # enabled even though the number of them allowed to run is being
    # changed
    $statement = qq{SELECT enabled FROM j_host_control WHERE run=$run AND host='$host'};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $enabled = $sth->fetchrow_array || 0;

    $statement = qq{REPLACE INTO j_host_control SET num_producers=$num_producers, host='$host', run=$run, enabled=$enabled};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    return $enabled;
}

# ---------------------------------------------------------------------

=item update_host_enabled

Description

=cut

# ---------------------------------------------------------------------
sub update_host_enabled {
    my ($C, $dbh, $run, $host, $enabled) = @_;

    my $sth;
    my $statement;

    # if producers are currently configured for a number of producers,
    # preserve that number
    $statement = qq{SELECT num_producers FROM j_host_control WHERE run=$run AND host='$host'};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $num_producers = $sth->fetchrow_array || 0;

    $statement = qq{REPLACE INTO j_host_control SET enabled=$enabled, run=$run, host='$host', num_producers=$num_producers};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# =====================================================================
# =====================================================================
#
#    Control tables:  [j_enqueuer_control] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item Renumber_enqueuer_control

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_enqueuer_control {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE j_enqueuer_control SET run=$to_run WHERE run=$from_run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item Select_enqueuer_enabled

Description

=cut

# ---------------------------------------------------------------------
sub Select_enqueuer_enabled {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT enabled FROM j_enqueuer_control WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $enabled = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: enabled=$enabled});

    return $enabled;
}


# ---------------------------------------------------------------------

=item set_enqueuer_enabled

Description

=cut

# ---------------------------------------------------------------------
sub set_enqueuer_enabled {
    my ($C, $dbh, $run, $enabled) = @_;

    my $statement = qq{REPLACE INTO j_enqueuer_control SET run=$run, enabled=$enabled};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item delete_enqueuer

Description

=cut

# ---------------------------------------------------------------------
sub delete_enqueuer {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM j_enqueuer_control WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# =====================================================================
# =====================================================================
#
#    Control tables:  [j_rights_control] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item Select_rights_enabled

Description

=cut

# ---------------------------------------------------------------------
sub Select_rights_enabled {
    my ($C, $dbh) = @_;

    my $statement = qq{SELECT enabled FROM j_rights_control};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $enabled = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: enabled=$enabled});

    return $enabled;
}


# ---------------------------------------------------------------------

=item set_rights_enabled

Description

=cut

# ---------------------------------------------------------------------
sub set_rights_enabled {
    my ($C, $dbh, $enabled) = @_;

    my $sth;
    my $statement;

    $statement = qq{DELETE FROM j_rights_control};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
    $statement = qq{INSERT INTO j_rights_control SET enabled=$enabled};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# =====================================================================
# =====================================================================
#
#    Control tables:  [j_commit_control][j_check_control] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item Renumber_optimize_control

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_optimize_control {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE j_commit_control SET run=$to_run WHERE run=$from_run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item delete_optimize_control

Description

=cut

# ---------------------------------------------------------------------
sub delete_optimize_control {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM j_commit_control WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item set_optimize_enabled

Description

=cut

# ---------------------------------------------------------------------
sub set_optimize_enabled {
    my ($C, $dbh, $run, $shard, $enabled) = @_;

    my $statement = qq{REPLACE INTO j_commit_control SET run=$run, shard=$shard, enabled=$enabled};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item Select_optimize_enabled

Description

=cut

# ---------------------------------------------------------------------
sub Select_optimize_enabled {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT enabled FROM j_commit_control WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $enabled = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: enabled=$enabled});

    return $enabled;
}

# ---------------------------------------------------------------------

=item Renumber_check_control

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_check_control {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE j_check_control SET run=$to_run WHERE run=$from_run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item delete_check_control

Description

=cut

# ---------------------------------------------------------------------
sub delete_check_control {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM j_check_control WHERE run=$run};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}


# ---------------------------------------------------------------------

=item set_check_enabled

Description

=cut

# ---------------------------------------------------------------------
sub set_check_enabled {
    my ($C, $dbh, $run, $shard, $enabled) = @_;

    my $statement = qq{REPLACE INTO j_check_control SET run=$run, shard=$shard, enabled=$enabled};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item Select_check_enabled

Description

=cut

# ---------------------------------------------------------------------
sub Select_check_enabled {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT enabled FROM j_check_control WHERE run=$run AND shard=$shard};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $enabled = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: enabled=$enabled});

    return $enabled;
}

1;
__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2008-9 ©, The Regents of The University of Michigan, All Rights Reserved

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject
to the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

=cut
