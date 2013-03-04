package Index_Module;


=head1 NAME

Index_Module;

=head1 DESCRIPTION

This package provides the indexing service for a given id.  It is
shared between index-j and SlipUtils::*

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;
use Time::HiRes;

use Db;
use Utils;
use Identifier;
use Debug::DUtils;
use RightsGlobals;

use Search::Constants;
use Document::Generator;
use Document::Wrapper;

use SLIP_Utils::Log;
use SLIP_Utils::States;
use SLIP_Utils::IndexerMgr;

# Initialize a pool of Indexers with HTTP timeout=30 sec (default)
my $INDEXER_Mgr;

my $MAX_ERRORS_SEEN = 0;

use constant INDEX_OP => 0;
use constant DELETE_OP => 1;

# ---------------------------------------------------------------------

=item Service_ID

Description

=cut

# ---------------------------------------------------------------------
sub Service_ID {
    my ($C, $dbh, $run, $dedicated_shard, $pid, $host, $id, $item_ct) = @_;

    my $start = Time::HiRes::time();

    # Get the correct indexer for this id.  If this is a re-index and
    # the shard it belongs in is not enabled, the id will be added to
    # the error list.
    my ($index_state, $data_status, $metadata_status, $stats_ref);
    my $indexer = get_Shard_indexer($C, $dbh, $run, $pid, $host, $id, $dedicated_shard);
    my $op = get_required_op($C, $dbh, $id);
    if ($indexer) {
        # Index or delete
        if ($op == INDEX_OP) {
            ($index_state, $data_status, $metadata_status, $stats_ref) =
              index_one_id($C, $dbh, $run, $id, $indexer);
        }
        elsif ($op == DELETE_OP) {
            ($index_state, $data_status, $metadata_status, $stats_ref) =
              delete_one_id($C, $dbh, $id, $indexer);
        }
        else {
            ASSERT(0, qq{invalid opcode="$op"});
        }
    }
    else {
        ($index_state, $data_status, $metadata_status) =
          (IX_NO_INDEXER_AVAIL, IX_NO_ERROR, IX_NO_ERROR);
    }

    my $result_was_error =
      handle_i_result($C, $dbh, $run, $dedicated_shard, $id, $pid, $host,
                      $index_state, $data_status, $metadata_status);

    my $reindexed = 0;
    my $deleted = 0;
    if (! $result_was_error) {
        if ($op == INDEX_OP) {
            $reindexed = update_ids_indexed($C, $dbh, $run, $dedicated_shard, $id);
        }
        elsif ($op == DELETE_OP) {
            $deleted = update_ids_deleted($C, $dbh, $run, $dedicated_shard, $id);
        }
    }

    Log_item($C, $run, $dedicated_shard, $id, $pid, $host, $stats_ref, $item_ct,
             $index_state, $data_status, $metadata_status, $reindexed, $deleted);

    # Item is now recorded in either slip_errors or slip_indexed or
    # in slip_indexed AND slip_timeouts.  Unless deleted.
    my $shard_num_docs_processed =
      update_stats($C, $dbh, $run, $dedicated_shard, $reindexed, $deleted, $result_was_error, $stats_ref, $start);

    update_checkpoint($C, $dbh, $run, $dedicated_shard, time(), $shard_num_docs_processed);

    handle_timeout_delay($C, $dbh, $run, $pid, $dedicated_shard, $host, $index_state, $item_ct, $id);

    return ($index_state, $data_status, $metadata_status, $stats_ref);
}

# ---------------------------------------------------------------------

=item get_required_op

As of Wed Oct 12 12:06:32 2011, any id that has attr==8 (nobody)
regardless of "reason" will send a delete to Solr for that id.  It
could be a no-op if the item is not in the index.

=cut

# ---------------------------------------------------------------------
sub get_required_op {
    my ($C, $dbh, $id) = @_;

    my $stripped_id = Identifier::get_id_wo_namespace($id);
    my $namespace = Identifier::the_namespace($id);

    my $statement = qq{SELECT attr FROM ht.rights_current WHERE  namespace=? AND id=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $namespace, $stripped_id);

    my $row_hashref = $sth->fetchrow_hashref();
    my $attr = $$row_hashref{'attr'};

    my $op = INDEX_OP;
    if ($attr == $RightsGlobals::g_available_to_no_one_attribute_value) {
        $op = DELETE_OP;
    }

    return $op;
}

# ---------------------------------------------------------------------

=item get_INDEXER_Mgr

Description

=cut

# ---------------------------------------------------------------------
sub get_INDEXER_Mgr {
    my ($C, $dbh, $run, $shard) = @_;

    # Initialize a pool of Indexers
    my $indexer_mgr =
      defined($INDEXER_Mgr)
        ? $INDEXER_Mgr
          : ($INDEXER_Mgr = new SLIP_Utils::IndexerMgr($C, $dbh, $run, $shard));

    return $indexer_mgr;
}


# ---------------------------------------------------------------------

=item delete_one_id

Handles deletion of the different document types Volume, Page by using
delete by query

=cut

# ---------------------------------------------------------------------
sub delete_one_id {
    my ($C, $dbh, $id, $indexer) = @_;

    my ($index_state, $data_status, $metadata_status) = (IX_NO_ERROR, IX_NO_ERROR, IX_NO_ERROR);
    #
    # --------------------  Delete Document By Query  --------------------
    #
    my $del_stats_ref;
    my $delete_field = $C->get_object('MdpConfig')->get('default_Solr_delete_field');
    my $safe_id = Identifier::get_safe_Solr_id($id);
    my $query = qq{$delete_field:$safe_id};
    
    ($index_state, $del_stats_ref) = $indexer->delete_by_query($C, $query);

    return ($index_state, $data_status, $metadata_status, $del_stats_ref);
}


# ---------------------------------------------------------------------

=item index_one_id

Description

=cut

# ---------------------------------------------------------------------
sub index_one_id {
    my ($C, $dbh, $run, $id, $indexer) = @_;

    my %merged_stats;

    my $dGen = new Document::Generator($C, $id);

    my $doc;
    my $doc_arr_ref = [];
    my $doc_build_failure = 0;

    my ($index_state, $data_status, $metadata_status) = (IX_NO_ERROR, IX_NO_ERROR, IX_NO_ERROR);
    #
    # --------------------  Create Document(s)  --------------------
    #
    while ( $doc = $dGen->generate_next($C) ) {

        my $doc_stats_ref = $doc->get_document_stats($C);
        SLIP_Utils::Common::merge_stats($C, \%merged_stats, $doc_stats_ref);

        ($data_status, $metadata_status) = $doc->get_document_status();
        $doc_build_failure = ($data_status != IX_NO_ERROR) || ($metadata_status != IX_NO_ERROR);

        if ($doc_build_failure) {
            last;
        }
        else {
            my $doc_content_ref = $doc->get_document_content($C);
            push(@$doc_arr_ref, $doc_content_ref)
              if ($doc_content_ref && $$doc_content_ref);
        }
    }

    #
    # --------------------  Index Document  --------------------
    #
    if (! $doc_build_failure) {
        my $full_Solr_doc_ref = Document::Wrapper::wrap($C, $doc_arr_ref);

        my $idx_stats_ref;
        ($index_state, $idx_stats_ref) = $indexer->index_Solr_document($C, $full_Solr_doc_ref);
        SLIP_Utils::Common::merge_stats($C, \%merged_stats, $idx_stats_ref);
    }

    return ($index_state, $data_status, $metadata_status, \%merged_stats);
}


# ---------------------------------------------------------------------

=item update_ids_indexed

Description

=cut

# ---------------------------------------------------------------------
sub update_ids_indexed {
    my ($C, $dbh, $run, $shard, $id) = @_;

    return Db::insert_item_id_indexed($C, $dbh, $run, $shard, $id);
}

# ---------------------------------------------------------------------

=item update_ids_deleted

Description

=cut

# ---------------------------------------------------------------------
sub update_ids_deleted {
    my ($C, $dbh, $run, $id) = @_;

    Db::Delete_item_id_indexed($C, $dbh, $run, $id);
    return 1;
}


# ---------------------------------------------------------------------

=item update_stats

each shard producer updates its row by adding its stats for EACH ITEM
processed - may be more than one producer per shard

=cut

# ---------------------------------------------------------------------
sub update_stats {
    my ($C, $dbh, $run, $shard, $reindexed, $deleted, $errored, $stats_ref, $start) = @_;

    my $tot_Time = Time::HiRes::time() - $start;

    my $doc_size = $$stats_ref{'create'}{'doc_size'} || 0;
    my $doc_Time = $$stats_ref{'create'}{'elapsed'} || 0;
    my $idx_Time = $$stats_ref{'update'}{'elapsed'} || $$stats_ref{'delete'}{'elapsed'} || 0;

    my ($shard_num_docs_processed) =
        Db::update_shard_stats($C, $dbh, $run, $shard, $reindexed, $deleted, $errored, $doc_size, $doc_Time, $idx_Time, $tot_Time);

    my $t = sprintf(qq{sec=%.2f}, $tot_Time);
    DEBUG('doc,idx', qq{TOTAL: processed in $t});

    return $shard_num_docs_processed;
}


# ---------------------------------------------------------------------

=item update_checkpoint

Description. try every 10 instead of 100 for finer granularity

=cut

# ---------------------------------------------------------------------
sub update_checkpoint {
    my ($C, $dbh, $run, $shard, $now, $shard_num_docs_processed) = @_;

    if (($shard_num_docs_processed % 100) == 0) {
        Db::update_rate_stats($C, $dbh, $run, $shard, $now);
    }
}


# ---------------------------------------------------------------------

=item get_MAX_ERRORS_SEEN

Description

=cut

# ---------------------------------------------------------------------
sub get_MAX_ERRORS_SEEN {
    return $MAX_ERRORS_SEEN;
}



# ---------------------------------------------------------------------

=item max_errors_reached

Description

=cut

# ---------------------------------------------------------------------
sub max_errors_reached {
    my ($C, $dbh, $run, $shard) = @_;

    my $config = $C->get_object('MdpConfig');

    # Solr could not parse doc
    my $max_I = $config->get('max_indx_errors');
    # Could not create OCR for Solr doc
    my $max_O = $config->get('max_ocr__errors');
    # Could not get metadata for Solr doc
    my $max_M = $config->get('max_meta_errors');
    # Server unavailable
    my $max_S = $config->get('max_serv_errors');
    # Serious stuff
    my $max_C = $config->get('max_crit_errors');
    my $max_N = $config->get('max_no_indexer_avail');

    my ($num_errors, $num_I, $num_O, $num_M, $num_C, $num_S, $num_N) =
        Db::Select_error_data($C, $dbh, $run, $shard);

    my ($condition, $num, $max);

    my $max_I_seen = ($num_I > $max_I);
    if ($max_I_seen) {
        $condition = 'I'; $num = $num_I; $max = $max_I;
    }
    my $max_O_seen = ($num_O > $max_O);
    if ($max_O_seen) {
        $condition = 'O'; $num = $num_O; $max = $max_O;
    }
    my $max_M_seen = ($num_M > $max_M);
    if ($max_M_seen) {
        $condition = 'M'; $num = $num_M; $max = $max_M;
    }
    my $max_C_seen = ($num_C > $max_C);
    if ($max_C_seen) {
        $condition = 'C'; $num = $num_C; $max = $max_C;
    }
    my $max_S_seen = ($num_S > $max_S);
    if ($max_S_seen) {
        $condition = 'S'; $num = $num_S; $max = $max_S;
    }
    my $max_N_seen = ($num_N > $max_N);
    if ($max_N_seen) {
        $condition = 'N'; $num = $num_N; $max = $max_N;
    }

    my $max_errors_seen =
        (
         $max_I_seen
         ||
         $max_O_seen
         ||
         $max_M_seen
         ||
         $max_S_seen
         ||
         $max_C_seen
         ||
         $max_N_seen
        );

    return ($max_errors_seen, $condition, $num, $max);
}

# ---------------------------------------------------------------------

=item get_Shard_indexer

Select the indexer for the given shard. This call will block until the
wait, if any, has expired for the shard.

=cut

# ---------------------------------------------------------------------
sub get_Shard_indexer {
    my ($C, $dbh, $run, $pid, $host, $id, $shard) = @_;

    ASSERT(($shard != 0), qq{invalid shard value="$shard"});
    my $indexer = get_INDEXER_Mgr($C, $dbh, $run, $shard)->get_indexer_For_shard($C);
    ASSERT($indexer, qq{get_Shard_indexer: indexer not defined});

    return $indexer;
}


# ---------------------------------------------------------------------

=item handle_timeout_delay

We saw JVM thread exhaustion when we continued to submit doc updates
while Solr was under a heavy load.  This code may prevent that.

By setting a wait for the indexer in the pool we prevent it from being
returned when we ask for an indexer until the wait time has elapsed.

=cut

# ---------------------------------------------------------------------
sub handle_timeout_delay {
    my ($C, $dbh, $run, $pid, $shard, $host, $index_state, $ct, $id, $was_indexed) = @_;

    if ($index_state == IX_INDEX_TIMEOUT) {
        my $Wait_For_secs = get_INDEXER_Mgr($C, $dbh, $run)->set_shard_waiting($C);
        Log_timeout($C, $run, $pid, $shard, $host, $Wait_For_secs, $ct, $id);
    }
    else {
        my $was_waiting = get_INDEXER_Mgr($C, $dbh, $run)->Reset_shard_waiting($C);
        Log_timeout($C, $run, $pid, $shard, $host, 0, $ct, 'noop')
            if ($was_waiting);
    }
}

# ---------------------------------------------------------------------

=item handle_i_result

All errors (indexing, ocr, metadata) are put in the error list and not
counted as indexed in slip_indexed.

Timesouts

=cut

# ---------------------------------------------------------------------
sub handle_i_result {
    my ($C, $dbh, $run, $dedicated_shard, $id, $pid, $host, $index_state, $data_status, $metadata_status) = @_;

    # Optimistic
    my $result_was_error = 0;

    # determine reason code in priority order: 1)indexing, 2)ocr, 3)metadata.
    my $index_ok = (! Search::Constants::indexing_failed($index_state));
    my $ocr_ok = ($data_status == IX_NO_ERROR);
    my $metadata_ok = ($metadata_status == IX_NO_ERROR);

    my $reason;
    if (! $index_ok) {
        $reason = $index_state;
        $result_was_error = 1;
    }
    elsif (! $ocr_ok) {
        $reason = $data_status;
        $result_was_error = 1;
    }
    elsif (! $metadata_ok) {
        $reason = $metadata_status;
        $result_was_error = 1;
    }

    # IX_INDEX_TIMEOUT is NOT an indexing error and thus the id will
    # be recorded in the slip_indexed table (downstream).
    # IX_INDEX_TIMEOUT is (probably) an HTTP timeout and the server
    # will complete the request. Still, to be sure the server
    # completed the request, record the id in the timeout table so
    # that when timeouts are reprocessed the request can be re-tried
    # and with the correct shard (now recorded in slip_indexed).
    if ($index_state == IX_INDEX_TIMEOUT) {
        Db::insert_item_id_timeout($C, $dbh, $run, $id, $dedicated_shard, $pid, $host);
    }

    if ($result_was_error) {
        Db::handle_error_insertion($C, $dbh, $run, $dedicated_shard, $id, $pid, $host, $reason);
    
        my ($max_errors_seen, $condition, $num, $max) = max_errors_reached($C, $dbh, $run, $dedicated_shard);
        if ($max_errors_seen && (! $MAX_ERRORS_SEEN)) {
            $MAX_ERRORS_SEEN = $SLIP_Utils::States::RC_MAX_ERRORS;

            Log_error_stop($C, $run, $dedicated_shard, $pid, $host, "MAX ERRORS condition=$condition num=$num)");

            my $subj = qq{[SLIP] MAX ERRORS: run=$run shard=$dedicated_shard disabled};
            my $msg =
                qq{ERROR point reached for run=$run shard=$dedicated_shard pid=$pid host=$host\n} .
                    qq{condition=$condition num=$num  max=$max};
            # One email, not one for every unprocessed slice item
            SLIP_Utils::Common::Send_email($C, 'report', $subj, $msg);

            Db::update_shard_enabled($C, $dbh, $run, $dedicated_shard, 0);
            Db::set_shard_build_error($C, $dbh, $run, $dedicated_shard);
        }
        
        if (! $metadata_ok) {
            # Dump the HTTP response dump to the log
            if ($C->has_object('Result')) {
                my $rs = $C->get_object('Result');
                my $dump = $rs->get_failed_HTTP_dump();
                Log_metadata_error($C, $run, $dedicated_shard, $pid, $host, $dump);
            }
        }
    }

    return $result_was_error;
}



# ---------------------------------------------------------------------

=item Log_item

Description

=cut

# ---------------------------------------------------------------------
sub Log_item {
    my ($C, $run, $shard, $id, $pid, $host, $stats_ref, $ct, $index_state, $data_status, $metadata_status, $reindexed, $deleted) = @_;

    my $buf;

    # DOC d_t = sec, d_kb = Kb, d_kbs = Kb/sec
    my $d_t   = $stats_ref->{create}{elapsed};
    my $d_k   = $stats_ref->{create}{doc_size}/1024;
    my $md_k  = $stats_ref->{create}{meta_size}/1024;
    my $da_k  = $stats_ref->{create}{data_size}/1024;
    my $d_kbs = ($d_t > 0) ? $d_k/$d_t : 0;

    $buf .= sprintf(qq{ d_k=%.1f d_t=%.2f d_kbs=%.2f da_k=%.2f md_k=%.2f}, $d_k, $d_t, $d_kbs, $da_k, $md_k);

    # IDX i_t = sec i_mbs = Mb/sec
    my $i_t = $$stats_ref{'update'}{'elapsed'};
    my $i_mbs = ($i_t > 0) ? $$stats_ref{'create'}{'doc_size'}/1024/1024/$i_t : 0;

    $buf .= sprintf(qq{ i_t=%.2f i_mbs=%.2f}, $i_t, $i_mbs);

    my $error = '';
    $error .= ' - ' . SLIP_Utils::Common::IXconstant2string($index_state)
        if (Search::Constants::indexing_failed($index_state));
    $error .= ' - ' . SLIP_Utils::Common::IXconstant2string($data_status)
        if ($data_status != IX_NO_ERROR);
    $error .= ' - ' . SLIP_Utils::Common::IXconstant2string($metadata_status)
        if ($metadata_status != IX_NO_ERROR);

    my $ri = '';
    if ($reindexed) {
        $ri = ' - REINDEX';
    }
    elsif ($deleted) {
        $ri = ' - DELETE';
    }

    $shard = $reindexed ? qq{REQD_$shard} : qq{rand_$shard};
    my $s = qq{ITEM[$ct$ri$error]: } . Utils::Time::iso_Time() . qq{ r=$run s=$shard id=$id pid=$pid h=$host} . $buf;
    SLIP_Utils::Log::this_string($C, $s, 'indexer_logfile', '___RUN___', $run);
}

# ---------------------------------------------------------------------

=item Log_timeout

Description

=cut

# ---------------------------------------------------------------------
sub Log_timeout {
    my ($C, $run, $pid, $shard, $host, $delay, $ct, $id) = @_;

    my $delay_str = $delay ? "***TIMEOUT DELAY[$ct]" : "***TIMEOUT RESET[$ct]" ;

    my $s = qq{$delay_str: } . Utils::Time::iso_Time() . qq{ r=$run s=$shard $id pid=$pid host=$host delay=$delay};
    SLIP_Utils::Log::this_string($C, $s, 'indexer_logfile', '___RUN___', $run);
}


# ---------------------------------------------------------------------

=item Log_error_stop

Description

=cut

# ---------------------------------------------------------------------
sub Log_error_stop {
    my ($C, $run, $shard, $pid, $host, $s) = @_;

    my $ss = qq{***ERROR STOP: } . Utils::Time::iso_Time() . qq{ run=$run pid=$pid host=$host shard=$shard stop=$s};
    SLIP_Utils::Log::this_string($C, $ss, 'indexer_logfile', '___RUN___', $run);
}


# ---------------------------------------------------------------------

=item Log_metadata_error

Description

=cut

# ---------------------------------------------------------------------
sub Log_metadata_error {
    my ($C, $run, $shard, $pid, $host, $s) = @_;

    my $ss = qq{***METADATA ERROR: } . Utils::Time::iso_Time() . qq{ run=$run pid=$pid host=$host shard=$shard error=$s};
    SLIP_Utils::Log::this_string($C, $ss, 'indexer_logfile', '___RUN___', $run);
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2011 ©, The Regents of The University of Michigan, All Rights Reserved

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
