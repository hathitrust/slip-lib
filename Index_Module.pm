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
use Debug::DUtils;

use Search::Constants;
use Document::Generator;
use Document::Wrapper;

use SLIP_Utils::Log;
use SLIP_Utils::States;
use SLIP_Utils::IndexerPool;

# Initialize a pool of Indexers with HTTP timeout=30 sec (default)
my $INDEXER_POOL;

my $GLOBAL_SHARD_FOR_ERROR_REPORT = 0;
my $MAX_ERRORS_SEEN = 0;

# ---------------------------------------------------------------------

=item Service_ID

Description

=cut

# ---------------------------------------------------------------------
sub Service_ID {
    my ($C, $dbh, $run, $pid, $host, $id, $item_ct) = @_;
    
    my $start = Time::HiRes::time();
    
    # Get the correct indexer for this id.  If this is a
    # re-index and the shard it belongs in is suspended, the
    # id will be added to the error list.
    my ($index_state, $data_status, $metadata_status, $stats_ref);
    my ($indexer, $shard, $random) = get_Next_indexer($C, $dbh, $run, $pid, $host, $id);
    if ($indexer) {
        # Index
        ($index_state, $data_status, $metadata_status, $stats_ref) =
          process_one_id($C, $dbh, $run, $id, $indexer);
    }
    else {
        ($index_state, $data_status, $metadata_status) =
          (IX_NO_INDEXER_AVAIL, IX_NO_ERROR, IX_NO_ERROR);
    }
    
    my $item_is_Solr_indexed =
      handle_i_result($C, $dbh, $run, $shard, $id, $pid, $host,
                      $index_state, $data_status, $metadata_status);
    
    my $reindexed = 0;
    if ($item_is_Solr_indexed) {
        $reindexed = update_ids_indexed($C, $dbh, $run, $shard, $id);
    }
    
    Log_item($C, $run, $shard, $id, $pid, $host, $stats_ref, $item_ct,
             $index_state, $data_status, $metadata_status, $random, $reindexed);
    
    # Item is now recorded in either mdp.j_errors or
    # mdp.j_indexed or in mdp.j_indexed AND mdp.j_timeouts.
    my $shard_num_docs_processed =
      update_stats($C, $dbh, $run, $shard, $stats_ref, $start, $index_state);
    
    update_checkpoint($C, $dbh, $run, $shard, time(), $shard_num_docs_processed);
    
    handle_timeout_delay($C, $dbh, $run, $pid, $shard, $host, $index_state, $item_ct, $id);    
}


# ---------------------------------------------------------------------

=item get_INDEXER_POOL

Description

=cut

# ---------------------------------------------------------------------
sub get_INDEXER_POOL {
    my ($C, $dbh, $run) = @_;
    
    # Initialize a pool of Indexers with HTTP timeout=30 sec (default)
    my $indexer_pool = 
      defined($INDEXER_POOL)
        ? $INDEXER_POOL 
          : ($INDEXER_POOL = new SLIP_Utils::IndexerPool($C, $dbh, $run));
    
    return $indexer_pool;
}

# ---------------------------------------------------------------------

=item process_one_id

Description

=cut

# ---------------------------------------------------------------------
sub process_one_id {
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
            push(@$doc_arr_ref, $doc_content_ref);
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

=item update_stats

each shard producer updates its row by adding its stats for EACH ITEM
processed - may be more than one producer per shard

=cut

# ---------------------------------------------------------------------
sub update_stats {
    my ($C, $dbh, $run, $shard, $stats_ref, $start) = @_;

    my $tot_Time = Time::HiRes::time() - $start;

    my $doc_size = $$stats_ref{'create'}{'doc_size'};
    my $doc_Time = $$stats_ref{'create'}{'elapsed'};
    my $idx_Time = $$stats_ref{'update'}{'elapsed'};

    my ($shard_num_docs_processed) =
        Db::update_shard_stats($C, $dbh, $run, $shard, $doc_size, $doc_Time, $idx_Time, $tot_Time);

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

=item get_GLOBAL_error_shard

Description

=cut

# ---------------------------------------------------------------------
sub get_GLOBAL_error_shard {
    return $GLOBAL_SHARD_FOR_ERROR_REPORT;
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

=item get_Next_indexer

If id already indexed, select an indexer that will re-index it in into
the correct shard otherwise just take whichever indexer comes up next.

If all indexers in the pool or the specific indexer for the shard
are/is waiting, this call will block until the wait has expired for at
least one indexer in the pool or the specific indexer for the shard,
as the case may be.

=cut

# ---------------------------------------------------------------------
sub get_Next_indexer {
    my ($C, $dbh, $run, $pid, $host, $id) = @_;

    my ($indexer, $shard);

    my $s = '';
    my $shard_of_id = Db::Select_item_id_shard($C, $dbh, $run, $id);
    my $random = (! $shard_of_id);

    if ($shard_of_id) {
        ($indexer, $shard) = get_INDEXER_POOL($C, $dbh, $run)->get_indexer_For_shard($C, $shard_of_id);
    }
    else {
        ($indexer, $shard) = get_INDEXER_POOL($C, $dbh, $run)->get_indexer($C);
    }

    $GLOBAL_SHARD_FOR_ERROR_REPORT = $shard;

    return ($indexer, $shard, $random);
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
        my $Wait_For_secs = get_INDEXER_POOL($C, $dbh, $run)->set_shard_waiting($C, $dbh, $run, $shard);
        Log_timeout($C, $run, $pid, $shard, $host, $Wait_For_secs, $ct, $id);
    }
    else {
        my $was_waiting = get_INDEXER_POOL($C, $dbh, $run)->Reset_shard_waiting($C, $shard);
        Log_timeout($C, $run, $pid, $shard, $host, 0, $ct, 'noop')
            if ($was_waiting);
    }
}


# ---------------------------------------------------------------------

=item handle_i_result

All errors (indexing, ocr, metadata) are put in the error list and not
counted as indexed in j_indexed.

Timesouts

=cut

# ---------------------------------------------------------------------
sub handle_i_result {
    my ($C, $dbh, $run, $shard, $id, $pid, $host, $index_state, $data_status, $metadata_status) = @_;

    # Optimistic
    my $item_is_Solr_indexed = 1;

    # determine reason code in priority order: 1)indexing, 2)ocr, 3)metadata.
    my $index_ok = (! Search::Constants::indexing_failed($index_state));
    my $ocr_ok = ($data_status == IX_NO_ERROR);
    my $metadata_ok = ($metadata_status == IX_NO_ERROR);

    my $reason;
    if (! $index_ok) {
        $reason = $index_state;
        $item_is_Solr_indexed = 0;
    }
    elsif (! $ocr_ok) {
        $reason = $data_status;
        $item_is_Solr_indexed = 0;
    }
    elsif (! $metadata_ok) {
        $reason = $metadata_status;
        $item_is_Solr_indexed = 0;
    }

    # IX_INDEX_TIMEOUT is NOT an indexing error and thus the id will
    # be recorded in the j_indexed table (downstream).
    # IX_INDEX_TIMEOUT is (probably) an HTTP timeout and the server
    # will complete the request. Still, to be sure the server
    # completed the request, record the id in the timeout table so
    # that when timeouts are reprocessed the request can be re-tried
    # and with the correct shard (now recorded in j_indexed).
    if ($index_state == IX_INDEX_TIMEOUT) {
        Db::insert_item_id_timeout($C, $dbh, $run, $id, $shard, $pid, $host);
    }

    if (! $item_is_Solr_indexed) {
        Db::insert_item_id_error($C, $dbh, $run, $shard, $id, $pid, $host, $reason);

        my ($max_errors_seen, $condition, $num, $max) = max_errors_reached($C, $dbh, $run, $shard);
        if ($max_errors_seen && (! $MAX_ERRORS_SEEN)) {
            $MAX_ERRORS_SEEN = $SLIP_Utils::States::RC_MAX_ERRORS;

            Log_error_stop($C, $run, $shard, $pid, $host, "MAX ERRORS condition=$condition num=$num)");

            my $subj = qq{[SLIP] MAX ERRORS: run=$run shard=$shard disabled};
            my $msg =
                qq{ERROR point reached for run=$run shard=$shard pid=$pid host=$host\n} .
                    qq{condition=$condition num=$num  max=$max};
            # One email, not one for every unprocessed slice item
            SLIP_Utils::Common::Send_email($C, 'report', $subj, $msg);

            Db::update_shard_enabled($C, $dbh, $run, $shard, 0);
            Db::set_shard_build_error($C, $dbh, $run, $shard);
        }
    }

    return $item_is_Solr_indexed;
}



# ---------------------------------------------------------------------

=item Log_item

Description

=cut

# ---------------------------------------------------------------------
sub Log_item {
    my ($C, $run, $shard, $id, $pid, $host, $stats_ref, $ct, $index_state, $data_status, $metadata_status, $random, $reindexed) = @_;

    my $buf;

    # DOC d_t = sec, d_kb = Kb, d_kbs = Kb/sec
    my $d_t = $$stats_ref{'create'}{'elapsed'};
    my $d_k = $$stats_ref{'create'}{'doc_size'}/1024;
    my $d_kbs = ($d_t > 0) ? $d_k/$d_t : 0;

    $buf .= sprintf(qq{ d_k=%.1f d_t=%.2f d_kbs=%.2f}, $d_k, $d_t, $d_kbs);

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

    $shard = $random ? qq{rand_$shard} : qq{REQD_$shard};
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
