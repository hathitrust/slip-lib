package Scheduler;

=head1 NAME

Scheduler.pm

=head1 DESCRIPTION

Routines for scheduling:

* full optimization

=cut

use strict;

# Perl
use Date::Calc;

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

PUBLIC.  Assumes a full optimize is kicked off immediately after
indexing finishes and that these two events occur on the same day, not
split across midnight.  driver-j will validate that all shards have
exactly one segment after completion of optimization and update the
schedule file.

Change delta days by negative one if the schedule spans midnight.

=cut

# ---------------------------------------------------------------------
sub driver_do_full_optimize {
    my $C = shift;
    my $run = shift;
    my $msg_ref = shift;

    if (! full_optimize_supported($C, $run)) {
        if ($msg_ref) {
            $$msg_ref = qq{driver: do full optimize not supported};
        }
        return 0;
    }

    my ($oyear, $omonth, $oday, $ohour, $omin, $interval) = __read_optimize_flag_file($C, $run);
    my ($year, $month, $day) = Date::Calc::Add_Delta_Days($oyear, $omonth, $oday, 0);
    my ($tyear, $tmonth, $tday) = Date::Calc::Today();

    my $do = (($tyear == $year) && ($tmonth == $month) && ($tday == $day));
    if ($msg_ref) {
        $$msg_ref = qq{driver: do full optimize=} . ($do ? 1 : 0) . qq{ today=$tyear-$tmonth-$tday schedule=$oyear-$omonth-$oday};
    }
    
    return $do; 
}

# ---------------------------------------------------------------------

=item __do_full_optimize

PRIVATE.  If the scheduled day and time have arrived, optimize-j will
optimize to one segment.  Time should be configured to be sometime
after the time the driver starts running from cron.

=cut

# ---------------------------------------------------------------------
sub __do_full_optimize {
    my ($C, $run, $shard, $what) = @_;

    if (! full_optimize_supported($C, $run)) {
        return 0;
    }

    my ($oyear, $omonth, $oday, $ohour, $omin, $interval) = __read_optimize_flag_file($C, $run);
    my ($tyear, $tmonth, $tday, $thour, $tmin, $tsec) = Date::Calc::Today_and_Now();

    my $oTimeTime = Date::Calc::Date_to_Time($oyear, $omonth, $oday, $ohour, $omin, 1);
    my $tTimeTime = Date::Calc::Date_to_Time($tyear, $tmonth, $tday, $thour, $tmin, $tsec);
    
    my $do = ($tTimeTime >= $oTimeTime);
    
    my $msg = qq{$what: shard=$shard, do full optimize=} . ($do ? 1 : 0) . qq{ today_now=$tyear-$tmonth-$tday $thour:$tmin schedule=$oyear-$omonth-$oday $ohour:$omin };
    __output("$msg\n");
    Log_schedule($C, $run, $msg);
    
    return $do; 
}


# ---------------------------------------------------------------------

=item optimize_do_full_optimize

PUBLIC.  If the scheduled day and time have arrived, optimize-j will
optimize to one segment.  Time should be configured to be sometime
after the time the driver starts running from cron.

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

PUBLIC.  If the scheduled day and time have arrived, check-j will
check for one segment.  Time should be configured to be sometime
after the time the driver starts running from cron.

=cut

# ---------------------------------------------------------------------
sub check_do_full_optimize {
    my $C = shift;
    my $run = shift;
    my $shard = shift;

    return __do_full_optimize($C, $run, $shard, 'check phase');
}

# ---------------------------------------------------------------------

=item advance_full_optimize_date

PUBLIC

=cut

# ---------------------------------------------------------------------
sub advance_full_optimize_date {
    my $C = shift;
    my $run = shift;
    
    if (! full_optimize_supported($C, $run)) {
        return 0;
    }

    my ($oyear, $omonth, $oday, $ohour, $omin, $interval) = __read_optimize_flag_file($C, $run);
    my ($year, $month, $day) = Date::Calc::Add_Delta_Days($oyear, $omonth, $oday, $interval);

    my $next_schedule = "$year $month $day $ohour $omin $interval";
    my $msg = "advance next schedule=$next_schedule";
    __output("$msg\n");
    Log_schedule($C, $run, $msg);

    __write_optimize_flag_file($C, $run, $next_schedule);
}


# ---------------------------------------------------------------------

=item get_schedule_filepath

PUBLIC

=cut

# ---------------------------------------------------------------------
sub get_schedule_filepath {
    my $C = shift;
    my $run = shift;

    my $config = $C->get_object('MdpConfig');
    my $logdir = Utils::get_tmp_logdir();    
    my $schedule_filepath = 
      ($ENV{HT_DEV} ? $logdir : $config->get('shared_flags_dir')) 
        . '/' 
          . $config->get('full_optimize_flag_file');
    $schedule_filepath =~ s,__RUN__,$run,;

    return $schedule_filepath;
}

# ---------------------------------------------------------------------

=item __read_optimize_flag_file

Private. Read a line:

YYYY MM DD HH MM N[N]

where NN is number of days between full optimizations 

=cut

# ---------------------------------------------------------------------
sub __read_optimize_flag_file {
    my $C = shift;
    my $run = shift;

    my $schedule_filepath = get_schedule_filepath($C, $run);
    open(SCHED, "<$schedule_filepath") || die("$schedule_filepath i/o error: $!");
    local $/;
    my $schedule = <SCHED>;
    close(SCHED);
    chomp($schedule);
    
    return split(/\s+/, $schedule);
}

# ---------------------------------------------------------------------

=item __write_optimize_flag_file

Private

=cut

# ---------------------------------------------------------------------
sub __write_optimize_flag_file {
    my $C = shift;
    my $run = shift;
    my $schedule = shift;

    my $schedule_filepath = get_schedule_filepath($C, $run);
    open(SCHED, ">$schedule_filepath") || die("$schedule_filepath i/o error: $!");
    print SCHED $schedule;
    close(SCHED);
}


# ---------------------------------------------------------------------

=item full_optimize_supported

PUBLIC.  True if, the run is configured to do full optimization and the
schedule file is in place.

=cut

# ---------------------------------------------------------------------
sub full_optimize_supported {
    my $C = shift;
    my $run = shift;

    my $config = $C->get_object('MdpConfig');
    my $is_supported = $config->get('full_optimize_supported');
    if ($is_supported) {
        # Is the schedule file present?
        my $schedule_filepath = get_schedule_filepath($C, $run);
        if (! -e $schedule_filepath) {
            $is_supported = 0;
        }
    }

    return $is_supported;
}

1;


=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2010 ©, The Regents of The University of Michigan, All Rights Reserved

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



