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
use warnings;

use Time::HiRes qw( time );

# App
use Utils;
use Debug::DUtils;

use Context;
use DbUtils;
use Search::Constants;

use SLIP_Utils::States;
use SLIP_Utils::Common;

# Map from constants to integers for MySQL query building
our $C_NO_ERROR         = IX_NO_ERROR;
our $C_INDEX_FAILURE    = IX_INDEX_FAILURE;
our $C_INDEX_TIMEOUT    = IX_INDEX_TIMEOUT;
our $C_SERVER_GONE      = IX_SERVER_GONE;
our $C_ALREADY_FAILED   = IX_ALREADY_FAILED;
our $C_DATA_FAILURE     = IX_DATA_FAILURE;
our $C_METADATA_FAILURE = IX_METADATA_FAILURE;
our $C_CRITICAL_FAILURE = IX_CRITICAL_FAILURE;
our $C_NO_INDEXER_AVAIL = IX_NO_INDEXER_AVAIL;

our $MYSQL_ZERO_TIMESTAMP = '0000-00-00 00:00:00';
our $vSOLR_ZERO_TIMESTAMP = '00000000';

# ---------------------------------------------------------------------

=item __LOCK_TABLES, __UNLOCK_TABLES

Description

=cut

# ---------------------------------------------------------------------
sub __UNLOCK_TABLES {
    my $dbh = shift;

    my $statement = qq{UNLOCK TABLES};
    DEBUG('lsdb', qq{DEBUG: $statement});
    DbUtils::prep_n_execute($dbh, $statement);
}

sub __LOCK_TABLES {
    my ($dbh, @tables) = @_;

    my @table_statements = map { $_ . ' WRITE'} @tables;
    my $statement = qq{LOCK TABLES } . join(', ', @table_statements);
    DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}



# =====================================================================
# =====================================================================
#
#  Shadow rights table [slip_rights][slip_rights_temp][slip_vsolr_timestamp] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item initialize_j_rights_temp

No keys.  They are added after the table is fully populated.

=cut

# ---------------------------------------------------------------------
sub initialize_j_rights_temp {
    my($C, $dbh) = @_;

    my ($statement, $sth);

    $statement = qq{DROP TABLE IF EXISTS slip_rights_temp};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    $statement = qq{CREATE TABLE `slip_rights_temp` (`nid` varchar(32) NOT NULL DEFAULT '', `attr` tinyint(4) NOT NULL DEFAULT '0', `reason` tinyint(4) NOT NULL DEFAULT '0', `source` tinyint(4) NOT NULL DEFAULT '0', `user` varchar(32) NOT NULL DEFAULT '', `time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, `sysid` varchar(32) NOT NULL DEFAULT '', `update_time` int(11) NOT NULL DEFAULT '0', PRIMARY KEY (`nid`), KEY `update_time` (`update_time`), KEY `attr` (`attr`))};
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

    $statement = qq{DROP TABLE ht_maintenance.slip_rights};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    $statement = qq{ALTER TABLE slip_rights_temp RENAME TO ht_maintenance.slip_rights};
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
    $statement = qq{DELETE FROM slip_vsolr_timestamp};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    $statement = qq{INSERT INTO slip_vsolr_timestamp SET time=$timestamp};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item Select_vSolr_timestamp

A pointer into slip_rights

=cut

# ---------------------------------------------------------------------
sub Select_vSolr_timestamp {
    my($C, $dbh) = @_;

    my $statement = qq{SELECT time FROM slip_vsolr_timestamp};
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
    my($C, $dbh) = @_;

    my ($statement, $sth);

    $statement = qq{SELECT MAX(update_time) FROM slip_rights};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $latest_timestamp = $sth->fetchrow_array || $Db::vSOLR_ZERO_TIMESTAMP;
    DEBUG('lsdb', qq{DEBUG: $statement ::: $latest_timestamp});

    $statement = qq{UPDATE slip_vsolr_timestamp SET time=$latest_timestamp};
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

    my $statement = qq{SELECT count(*) FROM slip_rights};
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

    my $statement = qq{SELECT count(*) FROM slip_rights_temp};
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
        qq{SELECT CONCAT(namespace, '.', id) AS nid, attr, reason, source, user, time FROM rights_current WHERE namespace=? AND id=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $namespace, $id});
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $namespace, $id);
    my $row_hashref = $sth->fetchrow_hashref();

    return $row_hashref;
}


# ---------------------------------------------------------------------

=item Insert_j_rights_temp_id

The table
starts out empty during a full rebuild. nid in slip_rights table is
PRIMARY KEY so an nid can't appear more than once.

=cut

# ---------------------------------------------------------------------
sub Replace_j_rights_temp_id {
    my ($C, $dbh, $hashref) = @_;

    # From ht_repository.rights_current
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

    # CASE: nid is not in slip_rights_temp ==> NEW. Insert
    my $statement = qq{REPLACE INTO slip_rights_temp SET nid=?, attr=?, reason=?, source=?, user=?, time=?, sysid=?, update_time=?};
    DbUtils::prep_n_execute($dbh, $statement, $nid, $attr, $reason, $source, $user, $time, $sysid, $updateTime_in_vSolr);
    DEBUG('lsdb', qq{DEBUG: $statement : $nid, $attr, $reason, $source, $user, $time, $sysid, $updateTime_in_vSolr});
}


# ---------------------------------------------------------------------

=item Replace_j_rights_id

Description

=cut

# ---------------------------------------------------------------------
sub Replace_j_rights_id {
    my ($C, $dbh, $hashref, $Check_only) = @_;

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

    # See what we already have in slip_rights
    $statement = qq{SELECT nid, update_time, sysid FROM slip_rights WHERE nid=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $nid});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $nid);

    my $ref_to_arr_of_hashref = $sth->fetchall_arrayref({});

    my $nid_exists_in_slip_rights = $ref_to_arr_of_hashref->[0]->{'nid'};
    my $sysid_in_slip_rights = $ref_to_arr_of_hashref->[0]->{'sysid'};
    my $updateTime_in_slip_rights = $ref_to_arr_of_hashref->[0]->{'update_time'} || $Db::vSOLR_ZERO_TIMESTAMP;

    # Pass the slip_rights timestamp in the input hashref
    $hashref->{'timestamp_in_slip_rights'} = $updateTime_in_slip_rights;
    # Pass the slip_rights_sysid in the input hashref
    $hashref->{'sysid_in_slip_rights'} = $sysid_in_slip_rights;

    if (! $nid_exists_in_slip_rights) {
        # CASE: nid is not in slip_rights ==> NEW. Insert
        $case = 'NEW';
        DEBUG('lsdb', qq{DEBUG: $statement ::: (A) NEW});
    }
    else {
        # If nid's update_time is the is same as update_time recorded
        # in SLIP_RIGHTS_TABLE_NAME then we're seeing an update we
        # already recorded due to range query [last_run_time-2d TO *].
        # Use '<=' even though it should be impossible for the nid
        # timestamp we are seeing now to be older than what we
        # recorded when we saw it for the first time.
        if ($updateTime_in_vSolr <= $updateTime_in_slip_rights) {
            $case = 'NOOP';
            DEBUG('lsdb', qq{DEBUG: $statement ::: NOOP});
        }
        else {
            # Seen but updated since last save to SLIP_RIGHTS_TABLE_NAME
            if ($sysid_in_slip_rights eq $sysid) {
                # CASE: nid from vSolr newer (>) that timestamp in
                # slip_rights, same sysid: UPDATED
                $case = 'UPDATED';
                DEBUG('lsdb', qq{DEBUG: $statement ::: (D) UPDATED});
            }
            else {
                # CASE: nid in slip_rights but different sysid: MOVED
                $case = 'MOVED';
                DEBUG('lsdb', qq{DEBUG: $statement ::: (C) MOVED});
            }
        }
    }

    $statement = qq{REPLACE INTO slip_rights SET nid=?, attr=?, reason=?, source=?, user=?, time=?, sysid=?, update_time=?};
    DEBUG('lsdb', qq{DEBUG [Check=$Check_only, case=$case]: $statement : $nid, $attr, $reason, $source, $user, $time, $sysid, $updateTime_in_vSolr});

    if (! $Check_only) {
        if ($case ne 'NOOP') {
            # insert or replace
            $sth = DbUtils::prep_n_execute($dbh, $statement, $nid, $attr, $reason, $source, $user, $time, $sysid, $updateTime_in_vSolr);
        }
    }

    return $case;
}

# ---------------------------------------------------------------------

=item Select_j_rights_id_attr

Get the current attr value for the id in slip_rights.

=cut

# ---------------------------------------------------------------------
sub Select_j_rights_id_attr
{
    my ($C, $dbh, $nid) = @_;

    my $statement = qq{SELECT attr FROM slip_rights WHERE nid=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $nid);
    my $attr = $sth->fetchrow_array() || 0;

    return $attr;
}


# ---------------------------------------------------------------------

=item Select_j_rights_id_sysid

Get the current sysid value for the id in slip_rights.

=cut

# ---------------------------------------------------------------------
sub Select_j_rights_id_sysid
{
    my ($C, $dbh, $nid) = @_;

    my $statement = qq{SELECT sysid FROM slip_rights WHERE nid=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $nid);
    my $sysid = $sth->fetchrow_array() || 0;

    return $sysid;
}




# =====================================================================
# =====================================================================
#
#  Queue tables [slip_rights_timestamp][slip_queue][slip_errors][slip_timeouts] @@
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

    my $statement = qq{SELECT count(*) FROM slip_rights_timestamp WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    my $ct = $sth->fetchrow_array;
    DEBUG('lsdb', qq{DEBUG: $statement : $run ::: $ct});

    return $ct;
}


# ---------------------------------------------------------------------

=item Select_j_rights_timestamp

Description: holds timestamp into slip_rights when last enqueue to slip_queue
occured.

=cut

# ---------------------------------------------------------------------
sub Select_j_rights_timestamp {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT time FROM slip_rights_timestamp WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    my $timestamp = $sth->fetchrow_array || $Db::vSOLR_ZERO_TIMESTAMP;
    DEBUG('lsdb', qq{DEBUG: $statement : $run ::: $timestamp});

    return $timestamp;
}


# ---------------------------------------------------------------------

=item update_j_rights_timestamp

Description: update timestamp into slip_rights when last enqueue to slip_queue
occured.

=cut

# ---------------------------------------------------------------------
sub update_j_rights_timestamp {
    my ($C, $dbh, $run, $timestamp) = @_;

    my $statement = qq{UPDATE slip_rights_timestamp SET time=? WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $timestamp, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : $timestamp, $run});
}

# ---------------------------------------------------------------------

=item init_j_rights_timestamp

Description

=cut

# ---------------------------------------------------------------------
sub init_j_rights_timestamp {
    my ($C, $dbh, $run, $time) = @_;

    my $timestamp = defined($time) ? $time : $Db::vSOLR_ZERO_TIMESTAMP;
    my $statement = qq{REPLACE INTO slip_rights_timestamp SET run=?, time=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $timestamp);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $timestamp});
}

# ---------------------------------------------------------------------

=item delete_j_rights_timestamp

Description

=cut

# ---------------------------------------------------------------------
sub delete_j_rights_timestamp {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM slip_rights_timestamp WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : $run});
}

# ---------------------------------------------------------------------

=item Renumber_j_rights_timestamp

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_j_rights_timestamp {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE slip_rights_timestamp SET run=? WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $to_run, $from_run);
    DEBUG('lsdb', qq{DEBUG: $statement : $to_run, $from_run});
}


# ---------------------------------------------------------------------

=item Select_id_slice_from_queue

Description

=cut

# ---------------------------------------------------------------------
sub Select_id_slice_from_queue {
    my ($C, $dbh, $run, $shard, $pid, $host, $slice_size) = @_;

    my $sth;
    my $statement;

    my $proc_status = $SLIP_Utils::States::Q_AVAILABLE;

    __LOCK_TABLES($dbh, qw(slip_queue));

    # mark a slice of available ids as being processed by a producer
    # process
    $statement = qq{UPDATE slip_queue SET pid=?, host=?, proc_status=? WHERE run=? AND (shard=0 OR shard=?) AND proc_status=? LIMIT $slice_size};
    DEBUG('lsdb', qq{DEBUG: $statement : $pid, $host, $SLIP_Utils::States::Q_PROCESSING, $run, 0, $shard, $proc_status});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $pid, $host, $SLIP_Utils::States::Q_PROCESSING, $run, $shard, $proc_status);

    # get the ids in the slice just marked for this process
    $statement = qq{SELECT id FROM slip_queue WHERE run=? AND proc_status=? AND pid=? AND host=?; };
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $SLIP_Utils::States::Q_PROCESSING, $pid, $host});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $SLIP_Utils::States::Q_PROCESSING, $pid, $host);

    my $ref_to_ary_of_hashref = $sth->fetchall_arrayref({});
    DEBUG('lsdb', qq{DEBUG: SELECT returned num_items=} . scalar(@$ref_to_ary_of_hashref));

    __UNLOCK_TABLES($dbh);

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

    my $total_affected = 0;
    my $num_affected = 0;
    do {
        my $begin = time;

        my $statement = qq{DELETE FROM slip_queue WHERE run=? LIMIT $DELETE_Q_SLICE_SIZE};
        DEBUG('lsdb', qq{DEBUG: $statement : $run});
        my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, \$num_affected);

        $num_affected = ($num_affected == '0E0') ? 0 : $num_affected;
        $total_affected += $num_affected;

        my $elapsed = time - $begin;
        sleep $elapsed/2;

    } until ($num_affected <= 0);

    return $total_affected;
}

# ---------------------------------------------------------------------

=item Renumber_queue

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_queue {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE slip_queue SET run=? WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $to_run, $from_run);
    DEBUG('lsdb', qq{DEBUG: $statement : $to_run, $from_run});
}


# ---------------------------------------------------------------------

=item insert_queue_items

Description: does not advance timestamp in slip_rights_timestamp.  Just
used for static testing.

=cut

# ---------------------------------------------------------------------
sub insert_queue_items {
    my ($C, $dbh, $run, $ref_to_ary_of_hashref) = @_;

    my $sth;
    my $statement;
    my $num_inserted = 0;

    __LOCK_TABLES($dbh, qw(slip_queue));

    foreach my $hashref (@$ref_to_ary_of_hashref) {
        my $id = $hashref->{id};
        my $shard = $hashref->{shard};

        $statement = qq{REPLACE INTO slip_queue SET run=?, shard=?, id=?, pid=0, host='', proc_status=?};
        DEBUG('lsdb', qq{DEBUG: $statement : $run, $id, $SLIP_Utils::States::Q_AVAILABLE});
        $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $id, $SLIP_Utils::States::Q_AVAILABLE);
        $num_inserted++;
    }

    __UNLOCK_TABLES($dbh);

    return $num_inserted;
}


# ---------------------------------------------------------------------

=item handle_queue_insert

Description

=cut

# ---------------------------------------------------------------------
sub handle_queue_insert {
    my $C = shift;
    my $dbh = shift;
    my $run = shift;
    my $ref_to_arr_of_ids = shift;

    my $total_start = time;

    my $total_to_be_inserted = scalar @$ref_to_arr_of_ids;
    my $total_num_inserted = 0;

    while (1) {
        my $start = time;

        # Insert in blocks of 1000
        my @queue_array = splice(@$ref_to_arr_of_ids, 0, 1000);
        last
            if (scalar(@queue_array) <= 0);

        __LOCK_TABLES($dbh, qw(slip_indexed));

        my $ref_to_arr_of_hashref = [];
        foreach my $id (@queue_array) {
            my $shard = Select_item_id_shard($C, $dbh, $run, $id);
            push(@$ref_to_arr_of_hashref, {id => $id, shard => $shard});
        }

        __UNLOCK_TABLES($dbh);

        my $num_inserted = insert_queue_items($C, $dbh, $run, $ref_to_arr_of_hashref);
        $total_num_inserted += $num_inserted;

        my $elapsed = time - $start;
        my $ids_per_sec = $total_num_inserted / (time - $total_start);
        my @parts = gmtime int(($total_to_be_inserted - $total_num_inserted) * (1 / $ids_per_sec));
        my $time_remaining = sprintf("%dh %dm %ds", @parts[2,1,0]);

        my $s0 = sprintf("--> added $num_inserted ids to queue, total=%d elapsed=%.2f rate=%.2f ids/sec remains=%s\n", $total_num_inserted, $elapsed, $ids_per_sec, $time_remaining);
        __output($s0);
    }

    my $total_elapsed = time - $total_start;
    my $s1 = sprintf("added %d total items to queue in %.0f sec.\n", $total_num_inserted, $total_elapsed);
    __output($s1);

    return $total_num_inserted;
}



# ---------------------------------------------------------------------

=item __get_update_time_WHERE_clause

Description

=cut

# ---------------------------------------------------------------------
sub __get_update_time_WHERE_clause {
    my ($C, $dbh, $run) = @_;

    my $timestamp = Select_j_rights_timestamp($C, $dbh, $run);
    my $WHERE_clause;
    if ($timestamp eq $Db::vSOLR_ZERO_TIMESTAMP) {
        $WHERE_clause = qq{ WHERE update_time >= ?};
    }
    else {
        $WHERE_clause = qq{ WHERE update_time > ?};
    }

    return ($WHERE_clause, $timestamp);
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
    my ($WHERE_clause, @params) = __get_update_time_WHERE_clause($C, $dbh, $run);
    my $statement = qq{SELECT count(*) FROM slip_rights } . $WHERE_clause;
    DEBUG('lsdb', qq{DEBUG: $statement : @params});
    my $sth = DbUtils::prep_n_execute($dbh, $statement, @params);

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

    __LOCK_TABLES($dbh, qw(slip_rights slip_rights_timestamp));

    # Load IDs from slip_rights whose timestamp is > or >= than the
    # timestamp of the items last enqueued from slip_rights and update
    # the timestamp.  This takes about 10 seconds for 10M IDs.
    my ($WHERE_clause, @params) = __get_update_time_WHERE_clause($C, $dbh, $run);
    $statement = qq{SELECT nid FROM slip_rights } . $WHERE_clause;
    $sth = DbUtils::prep_n_execute($dbh, $statement, @params);

    my $ref_to_arr_of_arr_ref = $sth->fetchall_arrayref([0]);
    my $id_arr_ref = [];
    if (scalar(@$ref_to_arr_of_arr_ref)) {
        $id_arr_ref = [ map {$_->[0]} @$ref_to_arr_of_arr_ref ];
    }

    # Use the maximum update_time in slip_rights to update the timestamp.
    $statement = qq{SELECT MAX(update_time) FROM slip_rights};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $new_timestamp = $sth->fetchrow_array;
    update_j_rights_timestamp($C, $dbh, $run, $new_timestamp);

    __UNLOCK_TABLES($dbh);

    # Insert them in blocks into slip_queue so queue is not locked for a
    # very long time during large updates.
    my $num_inserted = handle_queue_insert($C, $dbh, $run, $id_arr_ref);

    return $num_inserted;
}


# ---------------------------------------------------------------------

=item dequeue

Description

=cut

# ---------------------------------------------------------------------
sub dequeue {
    my ($C, $dbh, $run, $id, $pid, $host) = @_;

    my $statement = qq{DELETE FROM slip_queue WHERE run=? AND id=? AND pid=? AND host=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $id, $pid, $host);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $id, $pid, $host});
}


# ---------------------------------------------------------------------

=item Delete_id_from_j_queue

Description

=cut

# ---------------------------------------------------------------------
sub Delete_id_from_j_queue {
    my ($C, $dbh, $run, $id) = @_;

    my $statement = qq{DELETE FROM slip_queue WHERE run=? AND id=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $id);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $id});
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

    $statement = qq{SELECT count(*) FROM slip_queue WHERE run=? AND proc_status=?};
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $SLIP_Utils::States::Q_PROCESSING);
    my $num_inprocess = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $SLIP_Utils::States::Q_PROCESSING ::: inprocess=$num_inprocess});

    if ($num_inprocess > 0) {
        # Mark a slice of ids being processed by a producer process as
        # available
        $statement = qq{UPDATE slip_queue SET proc_status=? WHERE run=? AND proc_status=?};
        $sth = DbUtils::prep_n_execute($dbh, $statement, $SLIP_Utils::States::Q_AVAILABLE, $run, $SLIP_Utils::States::Q_PROCESSING);
        DEBUG('lsdb', qq{DEBUG: $statement : $SLIP_Utils::States::Q_AVAILABLE, $run, $SLIP_Utils::States::Q_PROCESSING});
    }

    return $num_inprocess;
}


# ---------------------------------------------------------------------

=item insert_restore_errors_to_queue

Description

=cut

# ---------------------------------------------------------------------
sub insert_restore_errors_to_queue {
    my ($C, $dbh, $run, $type) = @_;

    my $sth;
    my $statement;
    my $num_inserted = 0;

    __LOCK_TABLES($dbh, qw(slip_errors slip_queue slip_indexed));

    $statement = qq{SELECT id, shard FROM slip_errors WHERE run=?};
    if (defined $type) {
        $statement .= qq{ AND reason=?};
        $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $type);
        DEBUG('lsdb', qq{DEBUG: $statement : $run $type});
    }
    else {
        $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
        DEBUG('lsdb', qq{DEBUG: $statement : $run});
    }

    my $ref_to_ary_of_hashref = $sth->fetchall_arrayref({});
    foreach my $ref (@$ref_to_ary_of_hashref) {
        my $id = $ref->{'id'};
        my $shard = $ref->{'shard'};
        if (! $shard) {
            # See if this got indexed added to slip_errors with shard 0 and in
            # the meantime successfully indexed to to a different shard.
            my $real_shard = Select_item_id_shard($C, $dbh, $run, $id);
            $shard = $real_shard if ($real_shard);
        }

        my $num = 0;
        $statement = qq{REPLACE INTO slip_queue SET run=?, shard=?, id=?, pid=0, host='', proc_status=?};
        $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $id, $SLIP_Utils::States::Q_AVAILABLE, \$num);
        DEBUG('lsdb', qq{DEBUG: $statement : $run, $id, $SLIP_Utils::States::Q_AVAILABLE});
        $num_inserted += $num;
    }

    $statement = qq{DELETE FROM slip_errors WHERE run=?};
    if (defined $type) {
        $statement .= qq{ AND reason=?};
        $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $type);
        DEBUG('lsdb', qq{DEBUG: $statement : $run $type});
    }
    else {
        $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
        DEBUG('lsdb', qq{DEBUG: $statement : $run});
    }

    __UNLOCK_TABLES($dbh);

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

    __LOCK_TABLES($dbh, qw(slip_timeouts slip_queue));

    my $SELECT_clause =
        qq{SELECT $run AS run, id AS id, shard AS shard, 0 AS pid, '' AS host, $SLIP_Utils::States::Q_AVAILABLE AS proc_status FROM slip_timeouts WHERE run=?};

    $statement = qq{REPLACE INTO slip_queue ($SELECT_clause)};
    my $num_inserted = 0;
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, \$num_inserted);
    DEBUG('lsdb', qq{DEBUG: $statement : $run ::: inserted=$num_inserted});

    $statement = qq{DELETE FROM slip_timeouts WHERE run=?};
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : $run});

    __UNLOCK_TABLES($dbh);

    return $num_inserted;
}

# ---------------------------------------------------------------------

=item Select_timeouts_count

idempotent

=cut

# ---------------------------------------------------------------------
sub Select_timeouts_count {
    my ($C, $dbh, $run, $shard) = @_;

    my @params;
    my $AND_clause = '';
    if ( defined($shard) ) {
        $AND_clause = qq{ AND shard=? };
        @params = ( $shard );
    }
    my $statement =
        qq{SELECT count(*) FROM slip_timeouts WHERE run=?}
            . $AND_clause;
    DEBUG('lsdb', qq{DEBUG: $statement : $run});
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, @params);

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

    $statement = qq{SELECT count(*) from slip_queue WHERE run=?; };
    DEBUG('lsdb', qq{DEBUG: $statement : $run});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run);

    my $queue_size = $sth->fetchrow_array();

    $statement = qq{SELECT count(*) from slip_queue WHERE run=? AND proc_status=?; };
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $SLIP_Utils::States::Q_AVAILABLE});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $SLIP_Utils::States::Q_AVAILABLE);

    my $queue_num_available = $sth->fetchrow_array();

    $statement = qq{SELECT count(*) from slip_queue WHERE run=? AND proc_status=?; };
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $SLIP_Utils::States::Q_PROCESSING});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $SLIP_Utils::States::Q_PROCESSING);

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

    my $statement = qq{SELECT id, pid, host, timeout_time FROM slip_timeouts WHERE run=? AND shard=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);

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

    $statement = qq{SELECT count(*) from slip_errors WHERE run=?};
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run);

    my $num_errors = $sth->fetchrow_array() || 0;
    DEBUG('lsdb', qq{DEBUG: $statement : $run ::: num_errors=$num_errors});

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

    $statement = qq{SELECT count(*) from slip_errors WHERE run=? AND shard=?; };
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);

    my $num_errors = $sth->fetchrow_array() || 0;

    $statement = qq{SELECT count(*) from slip_errors WHERE run=? AND shard=? AND reason=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard, $C_INDEX_FAILURE});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $C_INDEX_FAILURE);

    my $num_I = $sth->fetchrow_array() || 0;

    $statement = qq{SELECT count(*) from slip_errors WHERE run=? AND shard=? AND reason=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard, $C_INDEX_FAILURE});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $C_DATA_FAILURE);

    my $num_O = $sth->fetchrow_array() || 0;

    $statement = qq{SELECT count(*) from slip_errors WHERE run=? AND shard=? AND reason=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard, $C_METADATA_FAILURE});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $C_METADATA_FAILURE);

    my $num_M = $sth->fetchrow_array() || 0;

    $statement = qq{SELECT count(*) from slip_errors WHERE run=? AND shard=? AND reason=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard, $C_CRITICAL_FAILURE});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $C_CRITICAL_FAILURE);

    my $num_C = $sth->fetchrow_array() || 0;

    $statement = qq{SELECT count(*) from slip_errors WHERE run=? AND shard=? AND reason=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard, $C_SERVER_GONE});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $C_SERVER_GONE);

    my $num_S = $sth->fetchrow_array() || 0;

    $statement = qq{SELECT count(*) from slip_errors WHERE run=? AND shard=? AND reason=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard, $C_NO_INDEXER_AVAIL});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $C_NO_INDEXER_AVAIL);

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

    my @params;
    my $AND_shard_clause = '';
    if ( defined($shard) ) {
        @params = ( $shard );
        $AND_shard_clause = qq{AND shard=?};
    }

    my $sth;
    my $statement = qq{SELECT id, pid, host, error_time FROM slip_errors WHERE run=? AND reason=? $AND_shard_clause};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $reason, @params});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $reason, @params);

    my $id_arr_hashref = $sth->fetchall_arrayref({});

    return $id_arr_hashref;
}


# ---------------------------------------------------------------------

=item handle_error_insertion

An ID that has never been indexed (not in slip_indexed) and repeatedly
fails will be randomly assigned a (probably) different dedicated shard
with each attempt. Restoring this ID to the queue repeatedly will add
it to the queue with these several different shard numbers and cause
the system to index it to more than one shard. If never indexed, set
its shard=0 in the error list. If previously indexed use the dedicated
shard.

=cut

# ---------------------------------------------------------------------
sub handle_error_insertion {
    my ($C, $dbh, $run, $dedicated_shard, $id, $pid, $host, $reason) = @_;

    __LOCK_TABLES($dbh, qw(slip_indexed slip_errors slip_queue));

    my $use_shard = 0;

    my $shard = Select_item_id_shard($C, $dbh, $run, $id);
    if ($shard) {
        ASSERT(($shard == $dedicated_shard),
               qq{shard number mismatch: indexed_shard=$shard dedicated_shard=$dedicated_shard id=$id});
        $use_shard = $dedicated_shard;
    }

    insert_item_id_error($C, $dbh, $run, $use_shard, $id, $pid, $host, $reason);

    __UNLOCK_TABLES($dbh);
}

# ---------------------------------------------------------------------

=item insert_item_id_error

Description; idempotent

=cut

# ---------------------------------------------------------------------
sub insert_item_id_error {
    my ($C, $dbh, $run, $shard, $id, $pid, $host, $index_state) = @_;

    my $statement =
        qq{REPLACE INTO slip_errors SET run=?, shard=?, id=?, pid=?, host=?, error_time=CURRENT_TIMESTAMP, reason=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $id, $pid, $host, $index_state);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard, $id, $pid, $host, $index_state});

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

    my $statement = qq{DELETE FROM slip_errors WHERE run=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run});
    my $ct = 0;
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, \$ct);

    return ($ct == '0E0') ? 0 : $ct;
}


# ---------------------------------------------------------------------

=item Renumber_errors

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_errors {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE slip_errors SET run=? WHERE run=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $to_run, $from_run});
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $to_run, $from_run);
}


# ---------------------------------------------------------------------

=item insert_item_id_timeout

Description

=cut

# ---------------------------------------------------------------------
sub insert_item_id_timeout {
    my ($C, $dbh, $run, $id, $shard, $pid, $host) = @_;

    my $statement =
        qq{INSERT INTO slip_timeouts SET run=?, id=?, shard=?, pid=?, host=?, timeout_time=CURRENT_TIMESTAMP};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $id, $shard, $pid, $host);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $id, $shard, $pid, $host});
}

# ---------------------------------------------------------------------

=item delete_timeouts

Description

=cut

# ---------------------------------------------------------------------
sub delete_timeouts {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM slip_timeouts WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : $run});
}


# =====================================================================
# =====================================================================
#
#                         Indexed [slip_indexed] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item Delete_indexed

Description

=cut

# ---------------------------------------------------------------------
my $DELETE_SLICE_SIZE = 1000;

sub Delete_indexed {
    my ($C, $dbh, $run) = @_;

    my ($statement, $sth);

    my $num_affected = 0;
    do {
        my $begin = time;

        $statement = qq{DELETE FROM slip_indexed WHERE run=? LIMIT $DELETE_SLICE_SIZE};
        DEBUG('lsdb', qq{DEBUG: $statement : $run});
        $sth = DbUtils::prep_n_execute($dbh, $statement, $run, \$num_affected);

        my $elapsed = time - $begin;
        sleep $elapsed;

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

    $statement = qq{SELECT indexed_ct FROM slip_indexed WHERE run=? AND id=? AND shard=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $id, $shard});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $id, $shard);

    my $indexed_ct = $sth->fetchrow_array() || 0;
    $indexed_ct++;

    $statement = qq{REPLACE INTO slip_indexed SET run=?, shard=?, id=?, time=CURRENT_TIMESTAMP, indexed_ct=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard, $id, $indexed_ct});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $id, $indexed_ct);

    return ($indexed_ct > 1);
}

# ---------------------------------------------------------------------

=item Delete_item_id_indexed

To handle Deletes

=cut

# ---------------------------------------------------------------------
sub Delete_item_id_indexed {
    my ($C, $dbh, $run, $id, $shard) = @_;

    my ($statement, $sth);

    if (defined $shard) {
        $statement = qq{DELETE FROM slip_indexed WHERE run=? AND id=? AND shard=?};
        DEBUG('lsdb', qq{DEBUG: $statement : $run, $id, $shard});
        $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $id, $shard);
    }
    else {
        $statement = qq{DELETE FROM slip_indexed WHERE run=? AND id=?};
        DEBUG('lsdb', qq{DEBUG: $statement : $run, $id});
        $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $id);
    }
}


# ---------------------------------------------------------------------

=item Select_item_id_shard

idempotent

=cut

# ---------------------------------------------------------------------
sub Select_item_id_shard {
    my ($C, $dbh, $run, $id) = @_;

    my $statement = qq{SELECT shard FROM slip_indexed WHERE run=? AND id=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $id);

    my $shard = $sth->fetchrow_array() || 0;
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $id ::: shard=$shard});

    return $shard;
}

# ---------------------------------------------------------------------

=item Select_indexed_count

idempotent

=cut

# ---------------------------------------------------------------------
sub Select_indexed_count {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT count(*) FROM slip_indexed WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);

    my $count = $sth->fetchrow_array() || 0;
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard ::: count=$count});

    return $count;
}

# ---------------------------------------------------------------------

=item Select_indexed_tot_count

idempotent

=cut

# ---------------------------------------------------------------------
sub Select_indexed_tot_count {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT count(*) FROM slip_indexed WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);

    my $count = $sth->fetchrow_array() || 0;
    DEBUG('lsdb', qq{DEBUG: $statement : $run ::: count=$count});

    return $count;
}

# ---------------------------------------------------------------------

=item Renumber_indexed

Description

=cut

# ---------------------------------------------------------------------
my $RENUMBER_Q_SLICE_SIZE = 1000;
sub Renumber_indexed {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $num_affected = 0;
    do {
        my $begin = time;

        my $statement = qq{UPDATE slip_indexed SET run=? WHERE run=? LIMIT $RENUMBER_Q_SLICE_SIZE};
        my $sth = DbUtils::prep_n_execute($dbh, $statement, $to_run, $from_run, \$num_affected);
        DEBUG('lsdb', qq{DEBUG: $statement : $to_run, $from_run});

        my $elapsed = time - $begin;
        sleep $elapsed;

    } until ($num_affected <= 0);
}

# ---------------------------------------------------------------------

=item Delete_id_from_j_rights

Description

=cut

# ---------------------------------------------------------------------
sub Delete_id_from_j_rights {
    my ($C, $dbh, $id) = @_;

    my $statement = qq{DELETE FROM slip_rights WHERE nid=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $id);
    DEBUG('lsdb', qq{DEBUG: $statement : $id});
}


# =====================================================================
# =====================================================================
#
#        Stats tables [slip_shard_stats][slip_rate_stats] @@
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

    my $statement = qq{DELETE FROM slip_shard_stats WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : $run});
}

# ---------------------------------------------------------------------

=item update_shard_stats

Update shard stats only if the id has not been indexed yet.  Error
redo and various re-enqueuing would increase the shard/checkpoint
counts is we didn't make this check.

=cut

# ---------------------------------------------------------------------
sub update_shard_stats {
    my ($C, $dbh, $run, $shard, $reindexed, $deleted, $errored, $doc_size, $doc_time, $idx_time, $tot_time) = @_;

    my $sth;
    my $statement;

    __LOCK_TABLES($dbh, qw(slip_shard_stats));

    $statement = qq{SELECT s_reindexed_ct, s_deleted_ct, s_errored_ct, s_num_docs, s_doc_size, s_doc_time, s_idx_time, s_tot_time FROM slip_shard_stats WHERE run=? AND shard=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);

    my ($s_reindexed_ct, $s_deleted_ct, $s_errored_ct, $s_num_docs, $s_doc_size, $s_doc_time, $s_idx_time, $s_tot_time);

    my $row_hashref = $sth->fetchrow_hashref();
    if (! $row_hashref) {
        # initialize
        $s_reindexed_ct = $reindexed ? 1 : 0;
        $s_deleted_ct = $deleted ? 1 : 0;
        $s_errored_ct = $errored ? 1 : 0;

        $s_num_docs = 1;
        $s_doc_size = $doc_size || 0;
        $s_doc_time = $doc_time || 0;
        $s_idx_time = $idx_time || 0;
        $s_tot_time = $tot_time || 0;

        $statement = qq{INSERT INTO slip_shard_stats SET run=?, shard=?, s_reindexed_ct=?, s_deleted_ct=?, s_errored_ct=?, s_num_docs=?, s_doc_size=?, s_doc_time=?, s_idx_time=?, s_tot_time=?};
        DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard, $s_reindexed_ct, $s_deleted_ct, $s_errored_ct, $s_num_docs, $s_doc_size, $s_doc_time, $s_idx_time, $s_tot_time});
        $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $s_reindexed_ct, $s_deleted_ct, $s_errored_ct, $s_num_docs, $s_doc_size, $s_doc_time, $s_idx_time, $s_tot_time);
    }
    else {
        # accumulate
        $s_reindexed_ct = $$row_hashref{'s_reindexed_ct'} + ($reindexed ? 1 : 0);
        $s_deleted_ct   = $$row_hashref{'s_deleted_ct'} + ($deleted ? 1 : 0);
        $s_errored_ct   = $$row_hashref{'s_errored_ct'} + ($errored ? 1 : 0);

        $s_num_docs = $$row_hashref{'s_num_docs'} + 1;
        $s_doc_size = $$row_hashref{'s_doc_size'} + $doc_size;
        $s_doc_time = $$row_hashref{'s_doc_time'} + $doc_time;
        $s_idx_time = $$row_hashref{'s_idx_time'} + $idx_time;
        $s_tot_time = $$row_hashref{'s_tot_time'} + $tot_time;

        $statement = qq{UPDATE slip_shard_stats SET s_reindexed_ct=?, s_deleted_ct=?, s_errored_ct=?, s_num_docs=?, s_doc_size=?, s_doc_time=?, s_idx_time=?, s_tot_time=? WHERE run=? AND shard=?};
        DEBUG('lsdb', qq{DEBUG: $statement : $s_reindexed_ct, $s_deleted_ct, $s_errored_ct, $s_num_docs, $s_doc_size, $s_doc_time, $s_idx_time, $s_tot_time, $run, $shard});
        $sth = DbUtils::prep_n_execute($dbh, $statement, $s_reindexed_ct, $s_deleted_ct, $s_errored_ct, $s_num_docs, $s_doc_size, $s_doc_time, $s_idx_time, $s_tot_time, $run, $shard);
    }

    __UNLOCK_TABLES($dbh);

    return ($s_num_docs, $s_doc_size, $s_doc_time, $s_idx_time, $s_tot_time);
}


# ---------------------------------------------------------------------

=item Select_shard_stats

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_stats {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT s_reindexed_ct, s_deleted_ct, s_errored_ct, s_num_docs, s_doc_size, s_doc_time, s_idx_time, s_tot_time FROM slip_shard_stats WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});

    my $row_hashref = $sth->fetchrow_hashref();

    my $s_reindexed_ct = $$row_hashref{'s_reindexed_ct'} || 0;
    my $s_deleted_ct   = $$row_hashref{'s_deleted_ct'} || 0;
    my $s_errored_ct   = $$row_hashref{'s_errored_ct'} || 0;

    my $s_num_docs = $$row_hashref{'s_num_docs'} || 0;
    my $s_doc_size = $$row_hashref{'s_doc_size'} || 0;
    my $s_doc_time = $$row_hashref{'s_doc_time'} || 0;
    my $s_idx_time = $$row_hashref{'s_idx_time'} || 0;
    my $s_tot_time = $$row_hashref{'s_tot_time'} || 0;

    return ($s_reindexed_ct, $s_deleted_ct, $s_errored_ct, $s_num_docs, $s_doc_size, $s_doc_time, $s_idx_time, $s_tot_time);
}


# ---------------------------------------------------------------------

=item Renumber_shard_stats

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_shard_stats {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE slip_shard_stats SET run=? WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $to_run, $from_run);
    DEBUG('lsdb', qq{DEBUG: $statement : $to_run, $from_run});
}


# ---------------------------------------------------------------------

=item Reset_rate_stats

Description

=cut

# ---------------------------------------------------------------------
sub Reset_rate_stats {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM slip_rate_stats WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : $run});
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

    __LOCK_TABLES($dbh, qw(slip_rate_stats));

    $statement = qq{SELECT * FROM slip_rate_stats WHERE run=? AND shard=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);

    # Initialize
    $ref_to_ary_of_hashref = $sth->fetchall_arrayref({});
    if (scalar(@$ref_to_ary_of_hashref) == 0) {
        $statement = qq{INSERT INTO slip_rate_stats SET run=?, shard=?};
        DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
        $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    }

    $statement = qq{SELECT * FROM slip_rate_stats WHERE run=? AND shard=?};
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);

    # No time delta if this is the first time the field has been updated
    # (default=0) so not possible to update the rate
    $ref_to_ary_of_hashref = $sth->fetchall_arrayref({});
    my $timeLast = $ref_to_ary_of_hashref->[0]->{'time_a_100'} || 0;
    if ($timeLast > 0) {
        my $deltaTime = $timeNow - $timeLast;
        my $docs_phour = $deltaTime ? sprintf("%0.2f", 100/$deltaTime*60*60) : 0;

        $statement = qq{UPDATE slip_rate_stats SET time_a_100=?, rate_a_100=? WHERE run=? AND shard=?};
        DEBUG('lsdb', qq{DEBUG: $statement : $timeNow, $docs_phour, $run, $shard});
        $sth = DbUtils::prep_n_execute($dbh, $statement, $timeNow, $docs_phour, $run, $shard);
    }
    else {
        $statement = qq{UPDATE slip_rate_stats SET time_a_100=? WHERE run=? AND shard=?};
        DEBUG('lsdb', qq{DEBUG: $statement : $timeNow, $run, $shard});
        $sth = DbUtils::prep_n_execute($dbh, $statement, $timeNow, $run, $shard);
    }

    __UNLOCK_TABLES($dbh);
}


# ---------------------------------------------------------------------

=item Select_rate_stats

Description

=cut

# ---------------------------------------------------------------------
sub Select_rate_stats {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT rate_a_100 FROM slip_rate_stats WHERE run=? AND shard=?};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);

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

    my $statement = qq{UPDATE slip_rate_stats SET run=? WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $to_run, $from_run);
    DEBUG('lsdb', qq{DEBUG: $statement : $to_run, $from_run});
}



# =====================================================================
# =====================================================================
#
#    Control tables:  [slip_shard_control] @@
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

    my $statement = qq{UPDATE slip_shard_control SET run=? WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $to_run, $from_run);
    DEBUG('lsdb', qq{DEBUG: $statement : $to_run, $from_run});
}

# ---------------------------------------------------------------------

=item delete_shard_control

Description

=cut

# ---------------------------------------------------------------------
sub delete_shard_control {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM slip_shard_control WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : $run});
}


# ---------------------------------------------------------------------

=item Select_shard_build_done

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_build_done {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT build_time FROM slip_shard_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});

    my $time = $sth->fetchrow_array || 0;
    return $time;
}


# ---------------------------------------------------------------------

=item Select_shard_optimize_done

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_optimize_done {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT optimize_time FROM slip_shard_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});

    my $time = $sth->fetchrow_array || 0;
    return $time;
}

# ---------------------------------------------------------------------

=item Select_shard_check_done

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_check_done {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT checkd_time FROM slip_shard_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});

    my $time = $sth->fetchrow_array || 0;
    return $time;
}

# ---------------------------------------------------------------------

=item Select_shard_release_state

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_release_state {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT release_state FROM slip_shard_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});

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

    my $statement = qq{UPDATE slip_shard_control SET release_state=? WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $state, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $state, $run, $shard});
}


# ---------------------------------------------------------------------

=item set_shard_build_done

Description

=cut

# ---------------------------------------------------------------------
sub set_shard_build_done {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{UPDATE slip_shard_control SET build_time=NOW() WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
}


# ---------------------------------------------------------------------

=item set_shard_optimize_done

Description

=cut

# ---------------------------------------------------------------------
sub set_shard_optimize_done {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{UPDATE slip_shard_control SET optimize_time=NOW() WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
}

# ---------------------------------------------------------------------

=item set_shard_check_done

Description

=cut

# ---------------------------------------------------------------------
sub set_shard_check_done {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{UPDATE slip_shard_control SET checkd_time=NOW() WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
}


# ---------------------------------------------------------------------

=item Reset_shard_control

Description

=cut

# ---------------------------------------------------------------------
sub Reset_shard_control {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{UPDATE slip_shard_control SET enabled=0, selected=0, allocated=0, build=0, optimiz=0, checkd=0 WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
}


# ---------------------------------------------------------------------

=item init_shard_control

Description

=cut

# ---------------------------------------------------------------------
sub init_shard_control {
    my ($C, $dbh, $run, $shard) = @_;

    my ($statement, $sth);

    $statement = qq{DELETE FROM slip_shard_control WHERE run=? AND shard=?};
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});

    $statement = qq{INSERT INTO slip_shard_control SET run=?, shard=?, enabled=0, selected=0, num_producers=0, allocated=0, build=0, optimiz=0, checkd=0, build_time=0, optimize_time=0, checkd_time=0, release_state=0};
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
}

# ---------------------------------------------------------------------

=item Select_shard_num_producers

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_num_producers {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT num_producers FROM slip_shard_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);

    my $num_producers_configured = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard ::: configured=$num_producers_configured});

    return $num_producers_configured;
}

# ---------------------------------------------------------------------

=item Select_shard_num_allocated

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_num_allocated {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT allocated FROM slip_shard_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);

    my $num_allocated = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement : $run $shard ::: $num_allocated});

    return $num_allocated;
}


# ---------------------------------------------------------------------

=item update_shard_num_producers

Description

=cut

# ---------------------------------------------------------------------
sub update_shard_num_producers {
    my ($C, $dbh, $run, $shard, $num_producers) = @_;

    my $statement = qq{UPDATE slip_shard_control SET num_producers=? WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $num_producers, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $num_producers, $run, $shard});
}

# ---------------------------------------------------------------------

=item Select_run_num_shards_available

Description

=cut

# ---------------------------------------------------------------------
sub Select_run_num_shards_available {
    my ($C, $dbh, $run) = @_;

    my ($statement, $sth);

    $statement = qq{SELECT count(*) FROM slip_shard_control WHERE run=? AND enabled=1};
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run);

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

    my $statement = qq{SELECT enabled FROM slip_shard_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);

    my $ref_to_ary_of_hashref = $sth->fetchall_arrayref({});
    my $enabled = $ref_to_ary_of_hashref->[0]->{'enabled'} || 0;

    DEBUG('lsdb', qq{DEBUG: $statement: $run, $shard ::: enabled=$enabled});

    return $enabled;
}


# ---------------------------------------------------------------------

=item update_shard_allocation

Description

=cut

# ---------------------------------------------------------------------
sub update_shard_allocation {
    my ($C, $dbh, $run, $shard, $alloc) = @_;

    my $statement = qq{UPDATE slip_shard_control SET allocated=? WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $alloc, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $alloc, $run, $shard});
}

# ---------------------------------------------------------------------

=item update_shard_enabled

Description

=cut

# ---------------------------------------------------------------------
sub update_shard_enabled {
    my ($C, $dbh, $run, $shard, $enabled) = @_;

    # If disabling a shard, set its producer allocation to 0 in case the
    # terminating producers fail to deallocate themselves. Enabling a
    # shard is idempotent so assume the count of allocated producers
    # is correct, i.e. do no alter that value.
    my ($statement, $sth);

    if (! $enabled) {
        $statement = qq{UPDATE slip_shard_control SET enabled=?, allocated=? WHERE run=? AND shard=?};
        $sth = DbUtils::prep_n_execute($dbh, $statement, $enabled, 0, $run, $shard);
        DEBUG('lsdb', qq{DEBUG: $statement : $enabled, 0, $run, $shard});
    }
    else {
        $statement = qq{UPDATE slip_shard_control SET enabled=? WHERE run=? AND shard=?};
        $sth = DbUtils::prep_n_execute($dbh, $statement, $enabled, $run, $shard);
        DEBUG('lsdb', qq{DEBUG: $statement : $enabled, $run, $shard});
    }
}

# ---------------------------------------------------------------------

=item set_shard_optimize_state

Description

=cut

# ---------------------------------------------------------------------
sub set_shard_optimize_state {
    my ($C, $dbh, $run, $shard, $state) = @_;

    my ($statement, $sth);

    # Error state is terminal
    my $current_state = Select_shard_optimize_state($C, $dbh, $run, $shard);
    if ($current_state == $SLIP_Utils::States::Sht_Optimize_Error) {
        return;
    }
    # POSSIBLY NOTREACHED

    $statement = qq{UPDATE slip_shard_control SET optimiz=$state WHERE run=? AND shard=?};
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
}

# ---------------------------------------------------------------------

=item Select_shard_optimize_state

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_optimize_state {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT optimiz FROM slip_shard_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});

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

    # Error state is terminal
    my $current_state = Select_shard_check_state($C, $dbh, $run, $shard);
    if ($current_state == $SLIP_Utils::States::Sht_Check_Error) {
        return;
    }
    # POSSIBLY NOTREACHED

    $statement = qq{UPDATE slip_shard_control SET checkd=? WHERE run=? AND shard=?};
    $sth = DbUtils::prep_n_execute($dbh, $statement, $state, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $state, $run, $shard});
}

# ---------------------------------------------------------------------

=item Select_shard_check_state

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_check_state {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT checkd FROM slip_shard_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});

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

    __LOCK_TABLES($dbh, qw(slip_shard_control));

    $statement = qq{UPDATE slip_shard_control SET build=$SLIP_Utils::States::Sht_Build_Error WHERE run=? AND shard=?};
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});

    __UNLOCK_TABLES($dbh);
}

# ---------------------------------------------------------------------

=item Select_shard_build_state

Description

=cut

# ---------------------------------------------------------------------
sub Select_shard_build_state {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT build FROM slip_shard_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});

    my $state = $sth->fetchrow_array || $SLIP_Utils::States::Sht_No_Build_Error;

    return $state;
}


# =====================================================================
# =====================================================================
#
#    Control tables:  [slip_host_control] @@
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

    my $statement = qq{UPDATE slip_host_control SET run=? WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $to_run, $from_run);
    DEBUG('lsdb', qq{DEBUG: $statement : $to_run, $from_run});
}


# ---------------------------------------------------------------------

=item Delete_host_control

Description

=cut

# ---------------------------------------------------------------------
sub Delete_host_control {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM slip_host_control WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : $run});
}

# ---------------------------------------------------------------------

=item Select_host_config

ONLY used for reporting.

=cut

# ---------------------------------------------------------------------
sub Select_hosts_config {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT host, num_producers, num_running, enabled FROM slip_host_control WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : $run});

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

    my $statement = qq{SELECT enabled FROM slip_host_control WHERE run=? AND host=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $host);

    my $enabled = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: enabled=$enabled});

    return $enabled;
}


# ---------------------------------------------------------------------

=item update_host_enabled

Description

=cut

# ---------------------------------------------------------------------
sub update_host_enabled {
    my ($C, $dbh, $run, $host, $enabled) = @_;

    my $statement = qq{INSERT INTO slip_host_control(`run`, `host`, `enabled`) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE enabled=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $host, $enabled, $enabled);
    DEBUG('lsdb', qq{DEBUG: $statement : $run $host $enabled  $enabled});
}


# ---------------------------------------------------------------------

=item Select_num_producers

Description

=cut

# ---------------------------------------------------------------------
sub Select_num_producers {
    my ($C, $dbh, $run, $host) = @_;

    my $statement = qq{SELECT num_producers FROM slip_host_control WHERE run=? AND host=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $host);

    my $num_producers_configured = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $host ::: configured=$num_producers_configured});

    return $num_producers_configured;
}

# ---------------------------------------------------------------------

=item update_host_num_producers

Serves to initialize rows as well.

=cut

# ---------------------------------------------------------------------
sub update_host_num_producers {
    my ($C, $dbh, $run, $num_producers, $host) = @_;

    my $statement = qq{INSERT INTO slip_host_control(`run`, `host`, `num_producers`) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE num_producers=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $host, $num_producers, $num_producers);
    DEBUG('lsdb', qq{DEBUG: $statement : $num_producers, $run, $host, $num_producers $num_producers});
}

# ---------------------------------------------------------------------

=item update_host_num_running

Description

=cut

# ---------------------------------------------------------------------
sub update_host_num_running {
    my ($C, $dbh, $run, $host, $num_running) = @_;

    my $statement = qq{INSERT INTO slip_host_control(`run`, `host`, `num_running`) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE num_running=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $host, $num_running, $num_running);
    DEBUG('lsdb', qq{DEBUG: $statement : $num_running, $run, $host});
}

# ---------------------------------------------------------------------

=item Select_host_num_running

Description

=cut

# ---------------------------------------------------------------------
sub Select_host_num_running {
    my ($C, $dbh, $run, $host) = @_;

    my $statement = qq{SELECT num_running FROM slip_host_control WHERE run=? AND host=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $host);

    my $num_running = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement : $run $host ::: $num_running});

    return $num_running;
}

# =====================================================================
# =====================================================================
#
#    Control tables:  [slip_enqueuer_control] @@
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

    my $statement = qq{UPDATE slip_enqueuer_control SET run=? WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $to_run, $from_run);
    DEBUG('lsdb', qq{DEBUG: $statement : $to_run, $from_run});
}

# ---------------------------------------------------------------------

=item Select_enqueuer_enabled

Description

=cut

# ---------------------------------------------------------------------
sub Select_enqueuer_enabled {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT enabled FROM slip_enqueuer_control WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);

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

    my $statement = qq{REPLACE INTO slip_enqueuer_control SET run=?, enabled=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $enabled);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $enabled});
}

# ---------------------------------------------------------------------

=item delete_enqueuer

Description

=cut

# ---------------------------------------------------------------------
sub delete_enqueuer {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM slip_enqueuer_control WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : $run});
}

# =====================================================================
# =====================================================================
#
#    Control tables:  [slip_rights_control] @@
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

    my $statement = qq{SELECT enabled FROM slip_rights_control};
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

    $statement = qq{DELETE FROM slip_rights_control};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
    $statement = qq{INSERT INTO slip_rights_control SET enabled=$enabled};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement : $enabled});
}

# =====================================================================
# =====================================================================
#
#    Control tables:  [slip_commit_control][slip_check_control] @@
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

    my $statement = qq{UPDATE slip_commit_control SET run=? WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $from_run, $to_run);
    DEBUG('lsdb', qq{DEBUG: $statement : $from_run, $to_run});
}

# ---------------------------------------------------------------------

=item delete_optimize_control

Description

=cut

# ---------------------------------------------------------------------
sub delete_optimize_control {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM slip_commit_control WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : $run});
}

# ---------------------------------------------------------------------

=item set_optimize_enabled

Description

=cut

# ---------------------------------------------------------------------
sub set_optimize_enabled {
    my ($C, $dbh, $run, $shard, $enabled) = @_;

    my $statement = qq{REPLACE INTO slip_commit_control SET run=?, shard=?, enabled=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $enabled);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard, $enabled});
}

# ---------------------------------------------------------------------

=item Select_optimize_enabled

Description

=cut

# ---------------------------------------------------------------------
sub Select_optimize_enabled {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT enabled FROM slip_commit_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});

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

    my $statement = qq{UPDATE slip_check_control SET run=? WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $to_run, $from_run);
    DEBUG('lsdb', qq{DEBUG: $statement : $to_run, $from_run});
}


# ---------------------------------------------------------------------

=item delete_check_control

Description

=cut

# ---------------------------------------------------------------------
sub delete_check_control {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM slip_check_control WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : $run});
}


# ---------------------------------------------------------------------

=item set_check_enabled

Description

=cut

# ---------------------------------------------------------------------
sub set_check_enabled {
    my ($C, $dbh, $run, $shard, $enabled) = @_;

    my $statement = qq{REPLACE INTO slip_check_control SET run=?, shard=?, enabled=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard, $enabled);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard, $enabled});
}

# ---------------------------------------------------------------------

=item Select_check_enabled

Description

=cut

# ---------------------------------------------------------------------
sub Select_check_enabled {
    my ($C, $dbh, $run, $shard) = @_;

    my $statement = qq{SELECT enabled FROM slip_check_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});

    my $enabled = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: enabled=$enabled});

    return $enabled;
}

# =====================================================================
# =====================================================================
#
#    Producer monitors:  [slip_shard_control, slip_host_control] @@
#
# =====================================================================
# =====================================================================


# ---------------------------------------------------------------------

=item decrement_allocation

Description

=cut

# ---------------------------------------------------------------------
sub decrement_allocation {
    my ($C, $dbh, $run, $host, $shard) = @_;

    my ($statement, $sth);

    __LOCK_TABLES($dbh, qw(slip_host_control slip_shard_control));

    my $allocated = Select_shard_num_allocated($C, $dbh, $run, $shard);
    if ($allocated > 0) {
        $allocated--;
        update_shard_allocation($C, $dbh, $run, $shard, $allocated);
    }

    my $num_running = Select_host_num_running($C, $dbh, $run, $host);
    if ($num_running > 0) {
        $num_running--;
        update_host_num_running($C, $dbh, $run, $host, $num_running);
    }

    __UNLOCK_TABLES($dbh);

    return ($allocated, $num_running);
}

# ---------------------------------------------------------------------

=item __shard_is_overallocated

Description

=cut

# ---------------------------------------------------------------------
sub __shard_is_overallocated {
    my ($C, $dbh, $run, $shard) = @_;

    my $overallocated = 0;

    my $allocated = Select_shard_num_allocated($C, $dbh, $run, $shard);
    my $num_producers_configured = Select_shard_num_producers($C, $dbh, $run, $shard);

    if ($allocated > $num_producers_configured) {
        $overallocated = 1;
    }

    return $overallocated;
}

# ---------------------------------------------------------------------

=item __host_is_overallocated

Description

=cut

# ---------------------------------------------------------------------
sub __host_is_overallocated {
    my ($C, $dbh, $run, $host) = @_;

    my $overallocated = 0;

    my $num_running = Select_host_num_running($C, $dbh, $run, $host);
    my $num_configured = Select_num_producers($C, $dbh, $run, $host);

    if ($num_running > $num_configured) {
        $overallocated = 1;
    }

    return $overallocated;
}

# ---------------------------------------------------------------------

=item dedicated_producer_monitor

Description

=cut

# ---------------------------------------------------------------------
sub dedicated_producer_monitor {
    my ($C, $dbh, $run, $host, $dedicated_shard) = @_;

    my $state = 'Mon_continue';

    __LOCK_TABLES($dbh, qw(slip_shard_control slip_host_control));

    if (! Select_host_enabled($C, $dbh, $run, $host)) {
        $state = 'Mon_host_disabled';
    }
    elsif (! Select_shard_enabled($C, $dbh, $run, $dedicated_shard)) {
        $state = 'Mon_shard_disabled';
    }
    elsif (__host_is_overallocated($C, $dbh, $run, $host)) {
        $state = 'Mon_host_overallocated';
    }
    elsif (__shard_is_overallocated($C, $dbh, $run, $dedicated_shard)) {
        $state = 'Mon_shard_overallocated';
    }

    __UNLOCK_TABLES($dbh);

    return ($dedicated_shard, $state);
}

# ---------------------------------------------------------------------

=item __get_queued_shards_list

When an ID is queued, if it has been indexed, its shard number
(1,2,...) is recorded. If it has never been indexed, its shard number
is 0.  Get a list of shard numbers for IDs that are available to be
indexed. Those are the candidate shards that a producer can lock onto.

=cut

# ---------------------------------------------------------------------
sub __get_queued_shards_list {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT DISTINCT shard FROM slip_queue WHERE run=? AND proc_status=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $SLIP_Utils::States::Q_AVAILABLE);
    my $ref_to_arr_of_arr_ref = $sth->fetchall_arrayref([]);

    my $queued_shards_ref = [];
    if (scalar(@$ref_to_arr_of_arr_ref)) {
        @$queued_shards_ref = ( map {$_->[0]} @$ref_to_arr_of_arr_ref );
    }
    DEBUG('lsdb',
          sub {
              my $s = join(' ', @$queued_shards_ref);
              return qq{DEBUG: $statement : $run, $SLIP_Utils::States::Q_AVAILABLE ::: $s}
          });

    return $queued_shards_ref;
}


# ---------------------------------------------------------------------

=item __allocate_shard_test

Description

=cut

# ---------------------------------------------------------------------
sub __allocate_shard_test {
    my ($C, $dbh, $run, $shard_list_ref, $queued_shards_list_ref) = @_;

    my ($statement, $sth);

    my $allocated = 0;
    my $allocated_shard = 0;
    my $exists_undedicated_ids = grep(/^0$/, @$queued_shards_list_ref);

    foreach my $shard (@$shard_list_ref) {
        if ($exists_undedicated_ids || (grep(/^$shard$/, @$queued_shards_list_ref))) {
            if (Select_shard_enabled($C, $dbh, $run, $shard)) {

                $allocated = Select_shard_num_allocated($C, $dbh, $run, $shard);
                my $num_producers_configured = Select_shard_num_producers($C, $dbh, $run, $shard);

                if ($allocated < $num_producers_configured) {
                    $allocated_shard = $shard;
                    $allocated++;
                    last;
                }
            }
        }
    }
    DEBUG('lsdb', qq{DEBUG: allocate shard test : shard=$allocated_shard num_allocated=$allocated});

    return ($allocated_shard, $allocated);
}

# ---------------------------------------------------------------------

=item __allocate_host_test

Description

=cut

# ---------------------------------------------------------------------
sub __allocate_host_test {
    my ($C, $dbh, $run, $host) = @_;

    my $allocated = 0;

    my $num_running = Select_host_num_running($C, $dbh, $run, $host);
    my $num_configured = Select_num_producers($C, $dbh, $run, $host);

    if ($num_running < $num_configured) {
        $num_running++;
        $allocated = 1;
    }
    DEBUG('lsdb', qq{DEBUG: allocate host test : host=$host success=$allocated num_allocated=$num_running});

    return ($allocated, $num_running);
}

# ---------------------------------------------------------------------

=item undedicated_producer_monitor

Description

=cut

# ---------------------------------------------------------------------
sub undedicated_producer_monitor {
    my ($C, $dbh, $run, $pid, $host, $shard_list_ref) = @_;

    my ($allocated_shard, $num_to_allocate) = (0, 0);
    my ($host_has_room, $set_num_running) =  (0, 0);

    my $state = 'Mon_undef';

    __LOCK_TABLES($dbh, qw(slip_shard_control slip_host_control slip_queue));

    if (! Select_host_enabled($C, $dbh, $run, $host)) {
        $state = 'Mon_host_disabled';
    }
    elsif (__host_is_overallocated($C, $dbh, $run, $host)) {
        $state = 'Mon_host_overallocated';
    }
    else {
        my $queued_shards_list_ref = __get_queued_shards_list($C, $dbh, $run);

        ($allocated_shard, $num_to_allocate) = __allocate_shard_test($C, $dbh, $run, $shard_list_ref, $queued_shards_list_ref);
        ($host_has_room, $set_num_running) = __allocate_host_test($C, $dbh, $run, $host);

        if ($allocated_shard) {
            if ($host_has_room) {
                update_shard_allocation($C, $dbh, $run, $allocated_shard, $num_to_allocate);
                update_host_num_running($C, $dbh, $run, $host, $set_num_running);
                $state = 'Mon_shard_and_host_allocated';
            }
            else {
                $state = 'Mon_host_fully_allocated';
            }
        }
        else {
            if ($host_has_room) {
                $state = 'Mon_shards_fully_allocated';
            }
            else  {
                $state = 'Mon_resource_fully_allocated';
            }
        }
    }

    __UNLOCK_TABLES($dbh);

    return ($allocated_shard, $state);
}

# =====================================================================
# =====================================================================
#
#        Holdings tables [slip_holdings_version][holdings_deltas] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item get_holdings_slice_size

Description

=cut

# ---------------------------------------------------------------------
sub get_holdings_slice_size {
    my ($C, $dbh, $last_loaded_version, $max_version) = @_;

    my $statement = qq{SELECT count(*) FROM holdings_deltas WHERE (version > ? AND version <= ?)};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $last_loaded_version, $max_version);
    my $size = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: size=$size});

    return $size;
}

# ---------------------------------------------------------------------

=item set_holdings_version

Description

=cut

# ---------------------------------------------------------------------
sub set_holdings_version {
    my ($C, $dbh, $run, $version) = @_;

    my $statement = qq{UPDATE slip_holdings_version SET last_loaded_version=?, load_time=NOW() WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $version, $run);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item delete_holdings_record

Description

=cut

# ---------------------------------------------------------------------
sub delete_holdings_record {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{DELETE FROM slip_holdings_version WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item init_holdings_version

Somewhat counter-intuitive: initialize version to MAX(version) in
holdings_deltas because initializing a run that reads from the
queue will read IDs enqueued from slip_rights which contains ALL IDs
to be indexed.  There cannot be IDs in holdings_deltas that are not in
slip_rights. Any IDs added to holdings_deltas during a run will have
MAX(version)+1 and so will be updated into queue when enqueuer is run
next.

Runs that read from a file do not participate in shared queue or
holding deltas.

=cut

# ---------------------------------------------------------------------
sub init_holdings_version {
    my ($C, $dbh, $run) = @_;

    my $max_version = get_holdings_max_version($C, $dbh, $run);

    delete_holdings_record($C, $dbh, $run);

    my $statement = qq{INSERT INTO slip_holdings_version SET run=?, last_loaded_version=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $max_version, $run);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item get_holdings_max_version

Description

=cut

# ---------------------------------------------------------------------
sub get_holdings_max_version {
    my ($C, $dbh) = @_;

    __LOCK_TABLES($dbh, qw(holdings_deltas));

    my $statement = qq{SELECT MAX(version) FROM holdings_deltas};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $max = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: max=$max});

    __UNLOCK_TABLES($dbh);

    return $max;
}

# ---------------------------------------------------------------------

=item get_last_loaded_holdings_version

Description

=cut

# ---------------------------------------------------------------------
sub get_last_loaded_holdings_version {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT last_loaded_version FROM slip_holdings_version WHERE run=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    my $version = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement ::: $version});

    return $version;
}

# ---------------------------------------------------------------------

=item read_holdings_deltas_item_ids

Description

=cut

# ---------------------------------------------------------------------
sub read_holdings_deltas_item_ids {
    my ($C, $dbh, $last_loaded_version, $max_version, $slice_size, $offset) = @_;

    my $id_arr_ref = [];

    my $statement = qq{SELECT volume_id FROM holdings_deltas WHERE (version > ? AND version <= ?) LIMIT $offset, $slice_size};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $last_loaded_version, $max_version);

    my $ref_to_arr_of_arr_ref = $sth->fetchall_arrayref([0]);
    if (scalar(@$ref_to_arr_of_arr_ref)) {
        $id_arr_ref = [ map {$_->[0]} @$ref_to_arr_of_arr_ref ];
    }

    return $id_arr_ref;
}

# ---------------------------------------------------------------------

=item optimize_select_shard

Description

=cut

# ---------------------------------------------------------------------
sub optimize_select_shard {
    my ($C, $dbh, $run, $shard, $max_selected) = @_;

    __LOCK_TABLES($dbh, qw(slip_shard_control));

    my $selected = 0;
    my ($statement, $sth);

    $statement = qq{SELECT count(*) FROM slip_shard_control WHERE run=? AND selected=?};
    $sth = DbUtils::prep_n_execute($dbh, $statement, $run, 1);
    my $count = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard :: $count});

    if ($count < $max_selected) {
        $statement = qq{UPDATE slip_shard_control SET selected=? WHERE run=? AND shard=?};
        $sth = DbUtils::prep_n_execute($dbh, $statement, 1, $run, $shard);
        DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard});
        $selected = 1;
    }

    __UNLOCK_TABLES($dbh);

    return $selected;
}

# ---------------------------------------------------------------------

=item optimize_shard_is_selected

Description

=cut

# ---------------------------------------------------------------------
sub optimize_shard_is_selected {
    my ($C, $dbh, $run, $shard) = @_;

    __LOCK_TABLES($dbh, qw(slip_shard_control));

    my $statement = qq{SELECT selected FROM slip_shard_control WHERE run=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $shard);
    my $selected = $sth->fetchrow_array || 0;
    DEBUG('lsdb', qq{DEBUG: $statement : $run, $shard ::: $selected});

    __UNLOCK_TABLES($dbh);

    return $selected;
}


1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2008-12, 2013 ©, The Regents of The University of Michigan, All Rights Reserved

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
