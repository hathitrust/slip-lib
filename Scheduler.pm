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
use SLIP_Utils::Log;
use SLIP_Utils::Common;

# ---------------------------------------------------------------------

=item Log_schedule

Description

=cut

# ---------------------------------------------------------------------
sub Log_schedule {
    my ($C, $run, $msg) = @_;

    my $s = qq{***SCHEDULER: } . Utils::Time::iso_Time() . qq{ r=$run $msg};
    SLIP_Utils::Log::this_string($C, $s, 'indexer_logfile', '___RUN___', $run);
}

# ---------------------------------------------------------------------

=item driver_do_full_optimize

PUBLIC.  If the second segment exceeds the trigger size the optimize
phase in driver-j will optimize to a single (1) segment.

=cut

# ---------------------------------------------------------------------
sub driver_do_full_optimize {
    my $C = shift;
    my $run = shift;

    return __do_full_optimize($C, $run, 0, 'driver');
}


# ---------------------------------------------------------------------

=item do_full_optimize

PUBLIC.

=cut

# ---------------------------------------------------------------------
sub do_full_optimize {
    my $C = shift;
    my $run = shift;
    
    return __do_full_optimize($C, $run, 0, undef);
}

# ---------------------------------------------------------------------

=item __get_segsizes

Description

=cut

# ---------------------------------------------------------------------
sub __get_segsizes {
    my ($C, $run) = @_;

    my $cmd = "$ENV{SDRROOT}/slip/scripts/segsizes -r$run";
    my $output = qx{$cmd 2>&1};
    my $rc = ($? >> 8);
    
    return ($rc, $output);
}

# ---------------------------------------------------------------------

=item __do_full_optimize

PRIVATE.  If the trigger condition applies, optimize-j will
optimize to one segment.

=cut

# ---------------------------------------------------------------------
sub __do_full_optimize {
    my ($C, $run, $shard, $what) = @_;

    if (! full_optimize_supported($C)) {
        return 0;
    }

    my $trigger_size = get_full_optimize_trigger_size($C);
    my ($rc, $sizes) = __get_segsizes($C, $run);
    if ($rc > 0) {
        return 0;
    }
    my @sizes = split(/[ \n]+/, $sizes);
    
    # "baby" segment
    my $do = ($sizes[1] > $trigger_size);

    if (defined($what)) {
        my $now = Utils::Time::iso_Time();
        my $msg = qq{$what: shard=$shard, do full optimize=} . ($do ? 1 : 0) . qq{ at $now};
        Log_schedule($C, $run, $msg);
    }

    return $do;
}

# ---------------------------------------------------------------------

=item optimize_do_full_optimize

PUBLIC.  If the trigger condition applies, optimize-j will optimize to
one segment.

=cut

# ---------------------------------------------------------------------
sub optimize_do_full_optimize {
    my $C = shift;
    my $run = shift;
    my $shard = shift;

    return __do_full_optimize($C, $run, $shard, 'optimize phase');
}

# ---------------------------------------------------------------------

=item check_do_full_optimize

PUBLIC.  If the trigger condition applies, check-j will
check for one segment.

=cut

# ---------------------------------------------------------------------
sub check_do_full_optimize {
    my $C = shift;
    my $run = shift;
    my $shard = shift;

    return __do_full_optimize($C, $run, $shard, 'check phase');
}

# ---------------------------------------------------------------------

=item full_optimize_supported

PUBLIC.  True if, the run is configured to do full optimization and the
schedule file is in place.

=cut

# ---------------------------------------------------------------------
sub full_optimize_supported {
    my $C = shift;

    my $config = $C->get_object('MdpConfig');
    my $is_supported = $config->get('full_optimize_supported');

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



