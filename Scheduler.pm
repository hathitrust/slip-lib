package Scheduler;

=head1 NAME

Scheduler.pm

=head1 DESCRIPTION

Routines for scheduling full optimization

=cut

use strict;
use warnings;

# App
use Utils;
use MdpConfig;

# SLIP
use Db;
use SLIP_Utils::Log;
use SLIP_Utils::Common;


# ---------------------------------------------------------------------

=item __get_segsizes

Description

=cut

# ---------------------------------------------------------------------
sub __get_segsizes {
    my ($C, $run, $shard) = @_;

    my $optional_shard_arg = (defined($shard) ? "-R$shard" : "");
    my $cmd = "$ENV{SDRROOT}/slip/scripts/segsizes -r$run -S $optional_shard_arg";
    my $output = qx{$cmd 2>&1};
    my $rc = ($? >> 8);
    
    return ($rc, $output);
}

# ---------------------------------------------------------------------

=item get_segsizes

Description

=cut

# ---------------------------------------------------------------------
sub get_segsizes {
    my ($C, $run, $shard) = @_;
    
    my ($rc, $sizes) = __get_segsizes($C, $run, $shard);
    if ($rc > 0) {
        return '0 0';
    }
    else {
        chomp($sizes);
        return $sizes;
    }
}

# ---------------------------------------------------------------------

=item get_segsizes_count

Description

=cut

# ---------------------------------------------------------------------
sub get_segsizes_count {
    my ($C, $run, $shard) = @_;
    
    my $cmd = "$ENV{SDRROOT}/slip/scripts/segsizes -r$run -C$shard";
    my $count = qx{$cmd 2>&1};
    my $rc = ($? >> 8);
    
    if ($rc > 0) {
        return '0';
    }
    else {
        chomp($count);    
        return $count;
    }
}

# ---------------------------------------------------------------------

=item optimize_try_full_optimize

PUBLIC.  If the trigger condition applies try to select myself to
optimize to one segment.

=cut

# ---------------------------------------------------------------------
sub optimize_try_full_optimize {
    my ($C, $run, $shard) = @_;

    if (! full_optimize_supported($C)) {
        return 0;
    }

    my $trigger_size = get_full_optimize_trigger_size($C);
    my ($rc, $sizes) = __get_segsizes($C, $run, $shard);
    if ($rc > 0) {
        return 0;
    }
    my @sizes = split(/[ \n]+/, $sizes);
    
    # "baby" segment size 
    my $snd_segment_size = $sizes[1];
    
    my $try = ($snd_segment_size > $trigger_size);

    return ($try, $snd_segment_size);
}

# ---------------------------------------------------------------------

=item get_max_full_optimizing_shards

When indexing a large update all shards will be > trigger_size so
apply the criterion to do a full optimize on all shards

=cut

# ---------------------------------------------------------------------
sub get_max_full_optimizing_shards {
    my ($C, $dbh, $run, $snd_segment_size) = @_;

    my $config = $C->get_object('MdpConfig');    

    my $max = 1;
    my $all_shards_trigger_size = get_all_full_optimize_trigger_size($C);
    
    if ($snd_segment_size > $all_shards_trigger_size) {
        my @shards = $config->get('num_shards_list');
        $max = scalar @shards;
    }
    else {
        $max = get_conf_max_full_optimizing_shards($C);
    }
    
    return $max;
}


# ---------------------------------------------------------------------

=item full_optimize_supported

PUBLIC.  True if, the run is configured to do full optimization and the
schedule file is in place.

=cut

# ---------------------------------------------------------------------
sub full_optimize_supported {
    my $C = shift;

    my $is_supported = $C->get_object('MdpConfig')->get('full_optimize_supported');
    return $is_supported;
}

# ---------------------------------------------------------------------

=item get_full_optimize_trigger_size

PUBLIC.

=cut

# ---------------------------------------------------------------------
sub get_full_optimize_trigger_size {
    my $C = shift;

    my $config = $C->get_object('MdpConfig');
    my $size = $config->get('full_optimize_trigger_size');

    return $size;
}

# ---------------------------------------------------------------------

=item get_conf_max_full_optimizing_shards

PUBLIC.

=cut

# ---------------------------------------------------------------------
sub get_conf_max_full_optimizing_shards {
    my $C = shift;

    my $config = $C->get_object('MdpConfig');
    my $max = $config->get('max_full_optimizing_shards');

    return $max;
}

# ---------------------------------------------------------------------

=item get_all_full_optimize_trigger_size

PUBLIC.

=cut

# ---------------------------------------------------------------------
sub get_all_full_optimize_trigger_size {
    my $C = shift;

    my $config = $C->get_object('MdpConfig');
    my $size = $config->get('full_optimize_all_shards_trigger_size');

    return $size;
}

1;


=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2013 ©, The Regents of The University of Michigan, All Rights Reserved

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



