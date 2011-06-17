package SLIP_Utils::IndexerPool;


=head1 NAME

IndexerPool;

=head1 DESCRIPTION

This class encapsulates the creation and access to Indexer objects.

Access is granted in a round-robin schedule based on whether a
shard is enabled, i.e. whether we are indexing to that shard.

Indexer object creation complies with the "num_shards_list"
configuration parameter in the config fiile for the run.  To change
that, it is necessary to stop the run, change the config and resume
the run so producers will read the new value.


=head1 VERSION

=head1 SYNOPSIS

my $pool = new SLIP_Utils::IndexerPool($C, $dbh, $run);

my ($indexer, $shard) = $pool->get_indexer($C);

=head1 METHODS

=over 8

=cut

use strict;

# App
use Utils;
use Debug::DUtils;
use Context;
use MdpConfig;
use Database;


# Local
use Db;
use SLIP_Utils::Common;
use SLIP_Utils::Solr;

# Timeout to take load off server.  
# Progression: 20, 20*2, 20*2*2, 20*2*2*2,
#              20, 40,   80,     160 
use constant NO_DELAY => 0;
use constant DEFAULT_DELAY => 20;
use constant DEFAULT_DELAY_MULTIPLIER => 2;

sub new
{
    my $class = shift;

    my $self = {};
    bless $self, $class;
    $self->_initialize(@_);

    return $self;
}


# ---------------------------------------------------------------------

=item _initialize

Initialize IndexerPool object.

=cut

# ---------------------------------------------------------------------
sub _initialize {
    my $self = shift;
    my ($C, $dbh, $run) = @_;

    $self->{'dbh'} = $dbh;
    $self->{'run'} = $run;

    my @io_arr = ();

    my $config = $C->get_object('MdpConfig');
    my @num_shards_list = $config->get('num_shards_list');
    my $timeout =  $config->get('solr_indexer_timeout');
    foreach my $__shard (@num_shards_list) {
        my $indexer = SLIP_Utils::Solr::create_shard_Indexer_by_alias($C, $__shard, $timeout);
        # Init obj
        $io_arr[__sh_2_io_idx($__shard)] = IndexerPool::IndexerObj->new($indexer, $__shard);
    }

    $self->{'indexers'} = \@io_arr;
    $self->{'num_indexers'} = scalar(@num_shards_list);

    # Randomize the round robin start index to not favor the
    # indexer at 0 at startup to better balance the number of ids in
    # each shard. Select an integer between 0 and number of shards in
    # @num_shards_list - 1
    my $index_of_start_shard = int(rand(scalar(@num_shards_list)));
    $self->{'indexer_index'} = $index_of_start_shard;
}



# ---------------------------------------------------------------------

=item set_shard_waiting

Description PUBLIC

=cut

# ---------------------------------------------------------------------
sub set_shard_waiting
{
    my $self = shift;
    my ($C, $dbh, $run, $shard) = @_;

    my $Wait_For_secs = 0;

    # If a concurrent process has disabled this shard (error, job
    # control), do not mark it waiting.  It could be re-enabled but
    # would still not run even if the timeout situation no longer
    # applied.  Let it run and prove whether it is ok to run or not.
    if (Db::Select_shard_enabled($C, $dbh, $run, $shard))
    {
        my $io = $self->__get_indexer_obj_at(__sh_2_io_idx($shard));

        if ($io->___get_delay_multiplier() > NO_DELAY)
        {
            # Already waiting: Escalate the wait
            my $new_Wait_multiplier = $io->___get_delay_multiplier() * DEFAULT_DELAY_MULTIPLIER;
            $Wait_For_secs = (DEFAULT_DELAY * $new_Wait_multiplier);
            my $new_Wait_Until = time() + $Wait_For_secs;

            $io->___set_Wait($new_Wait_Until, $new_Wait_multiplier);
        }
        else
        {
            $Wait_For_secs = DEFAULT_DELAY;
            my $Wait_Until = time() + $Wait_For_secs;
            $io->___set_Wait($Wait_Until, 1);
        }
    }

    return $Wait_For_secs;
}

# ---------------------------------------------------------------------

=item Reset_shard_waiting

Description PUBLIC

=cut

# ---------------------------------------------------------------------
sub Reset_shard_waiting
{
    my $self = shift;
    my ($C, $shard) = @_;

    my $io = $self->__get_indexer_obj_at(__sh_2_io_idx($shard));
    my $was_waiting = ($io->___get_delay_multiplier() > NO_DELAY);

    $io->___Reset_Wait();

    return $was_waiting;
}


# ---------------------------------------------------------------------

=item get_num_indexers_available
Description PUBLIC

=cut

# ---------------------------------------------------------------------
sub get_num_indexers_available
{
    my $self = shift;
    my $C = shift;

    my $dbh = $self->__get_dbh();
    my $run = $self->__get_run();

    return Db::Select_run_num_shards_available($C, $dbh, $run);
}


# ---------------------------------------------------------------------

=item get_indexer

Description PUBLIC

Always return an indexer.  This is so that caller can finish
processing its slice of the queue before halting.  Otherwise caller
would have to re-enqueue the unfinished ids in the slice which is
messier that indexing a few extra docs before halting.

If all indexers are disabled, return one, even if asleep, so the
caller can finish its slice and terminate before the next slice.
Sending a few extra docs to Solr while it's busy merging is ok.

If all enabled indexers are sleeping, suspend the call to this method
for the minimum sleep time over enabled indexers.

=cut

# ---------------------------------------------------------------------
sub get_indexer
{
    my $self = shift;
    my $C = shift;

    use constant MAX_SLEEP_TIME => 1000000;

    my $min_Sleep_Time = MAX_SLEEP_TIME;
    my $io_min = undef;

    my $num_indexers = $self->__get_num_indexers();

    my $tries = 0;

    while (1)
    {
        my $io = $self->__get_Next_enabled_indexer_obj($C);
        $tries++;

        if ($io)
        {
            my $wait_Time = $io->___get_Wait_Time();
            if ($wait_Time > 0)
            {
                $min_Sleep_Time = min($min_Sleep_Time, $wait_Time);
                $io_min = $io;
            }
            else
            {
                return ($io->___get_indexer(), $io->___get_shard());
            }
        }
        last if ($tries >= $num_indexers);
    }

    # If we got to here, either no indexers are enabled or all enabled
    # indexers are sleeping. If there's a minimum sleeping indexer,
    # block until it wakes up and return it. If there isn't a minimum
    # sleeping indexer, return an arbitrary disabled indexer.
    if ($io_min)
    {
        sleep $min_Sleep_Time;
        return ($io_min->___get_indexer(), $io_min->___get_shard());
    }
    else
    {
        my $arbitrary_io = $self->__get_arbitrary_indexer_obj($C);
        return ($arbitrary_io->___get_indexer(), $arbitrary_io->___get_shard());
    }
}



# ---------------------------------------------------------------------

=item get_indexer_For_shard

Description PUBLIC

Return the correct indexer for the shard unless the shard is disabled.
Caller will put the id on the error list in that case. If the indexer
is asleep, block for the time remaining in its nap because we have to
return this indexer.

=cut

# ---------------------------------------------------------------------
sub get_indexer_For_shard
{
    my $self = shift;
    my ($C, $shard) = @_;

    my $dbh = $self->__get_dbh();
    my $run = $self->__get_run();

    my $indexer_idx = __sh_2_io_idx($shard);
    if ($self->__indexer_available($C, $indexer_idx)) {
        my $io = $self->__get_indexer_obj_at(__sh_2_io_idx($shard));
        my $wait_Time = $io->___get_Wait_Time();

        if ($wait_Time > 0)
        {
            sleep $wait_Time;
        }

        return ($io->___get_indexer(), $io->___get_shard());
    }

    return (undef, $shard);
}

# ---------------------------------------------------------------------

=item __indexer_available

Description

=cut

# ---------------------------------------------------------------------
sub __indexer_available
{
    my $self = shift;
    my $C = shift;
    my $indexer_idx = shift;

    my $dbh = $self->__get_dbh();
    my $run = $self->__get_run();

    my $indexer_shard = __io_idx_2_sh($indexer_idx);

    return
        (
         Db::Select_shard_enabled($C, $dbh, $run, $indexer_shard)
         &&
         (! Db::shard_is_suspended($C, $dbh, $run, $indexer_shard))
        );
}

# ---------------------------------------------------------------------

=item __get_indexer_obj_at

Description

=cut

# ---------------------------------------------------------------------
sub __get_indexer_obj_at
{
    my $self = shift;
    my $indexer_idx = shift;

    return $self->{'indexers'}[$indexer_idx];
}


# ---------------------------------------------------------------------

=item __get_Next_enabled_indexer_obj

The "shard enabled" control table is not locked for the duration of
the loop below which raises the possibility of an infinite loop if no
indexer is enabled or not suspended.  If the count reaches the number
of configured indexers, fail.

=cut

# ---------------------------------------------------------------------
sub __get_Next_enabled_indexer_obj
{
    my $self = shift;
    my $C = shift;

    my $tries = 0;
    my $num_enabled = $self->get_num_indexers_available($C);

    while (1)
    {
        my $indexer_idx = $self->__get_Next_indexer_idx();
        $tries++;

        if ($self->__indexer_available($C, $indexer_idx))
        {
            # Success
            my $io = $self->__get_indexer_obj_at($indexer_idx);
            return $io;
        }

        last if ($tries >= $num_enabled);
    }

    # Failure
    return undef;
}

# ---------------------------------------------------------------------

=item __get_arbitrary_indexer_obj

Description

=cut

# ---------------------------------------------------------------------
sub __get_arbitrary_indexer_obj
{
    my $self = shift;
    my $C = shift;

    my $indexer_idx = $self->__get_Next_indexer_idx();
    my $io = $self->__get_indexer_obj_at($indexer_idx);
    return $io;
}


# ---------------------------------------------------------------------

=item __get_Next_indexer_idx

Description

=cut

# ---------------------------------------------------------------------
sub __get_Next_indexer_idx
{
    my $self = shift;

    my $current_idx = $self->{'indexer_index'};
    $self->{'indexer_index'} = ($current_idx + 1) % $self->__get_num_indexers();

    return $current_idx;
}

# ---------------------------------------------------------------------

=item __io_idx_2_sh, __sh_2_io_idx

Description

=cut

# ---------------------------------------------------------------------
sub __sh_2_io_idx
{
    my $shard = shift;
    return $shard - 1;
}
sub __io_idx_2_sh
{
    my $indexer_obj_idx = shift;
    return $indexer_obj_idx + 1;
}

# ---------------------------------------------------------------------

=item __get_run

Description

=cut

# ---------------------------------------------------------------------
sub __get_run
{
    my $self = shift;
    return $self->{'run'};
}

# ---------------------------------------------------------------------

=item __get_dbh

Description

=cut

# ---------------------------------------------------------------------
sub __get_dbh
{
    my $self = shift;
    return $self->{'dbh'};
}

# ---------------------------------------------------------------------

=item __get_num_indexers

Description

=cut

# ---------------------------------------------------------------------
sub __get_num_indexers
{
    my $self = shift;
    return $self->{'num_indexers'};
}



#
#  ----------------- Indexer Object ------------------
#
package IndexerPool::IndexerObj;

sub new
{
    my $class = shift;

    my $self = {};
    bless $self, $class;

    my $indexer = shift;
    my $shard = shift;

    $self->{'indexer'} = $indexer;
    $self->{'shard'} = $shard;

    $self->___Reset_Wait();

    return $self;
}

# ---------------------------------------------------------------------

=item ___set_Wait

Description

=cut

# ---------------------------------------------------------------------
sub ___set_Wait
{
    my $self = shift;
    my ($wait_Until, $multiplier) = @_;

    $self->{'wait_until'} = $wait_Until;
    $self->{'multiplier'} = $multiplier;
}

# ---------------------------------------------------------------------

=item ___get_delay_multiplier

Description

=cut

# ---------------------------------------------------------------------
sub ___get_delay_multiplier
{
    my $self = shift;
    return $self->{'multiplier'};
}

# ---------------------------------------------------------------------

=item ___Reset_Wait

Description:

=cut

# ---------------------------------------------------------------------
sub ___Reset_Wait
{
    my $self = shift;

    $self->{'wait_until'} = 0;
    $self->{'multiplier'} = SLIP_Utils::IndexerPool::NO_DELAY;
}

# ---------------------------------------------------------------------

=item ___get_indexer

Description

=cut

# ---------------------------------------------------------------------
sub ___get_indexer
{
    my $self = shift;
    return $self->{'indexer'};
}

# ---------------------------------------------------------------------

=item ___get_Wait_multiplier

Description

=cut

# ---------------------------------------------------------------------
sub ___get_Wait_multiplier
{
    my $self = shift;
    return $self->{'multiplier'};
}

# ---------------------------------------------------------------------

=item ___get_shard

Description

=cut

# ---------------------------------------------------------------------
sub ___get_shard
{
    my $self = shift;
    return $self->{'shard'};
}

# ---------------------------------------------------------------------

=item ___get_Wait_Time

Description

=cut

# ---------------------------------------------------------------------
sub ___get_Wait_Time
{
    my $self = shift;

    my $wait_Time = $self->{'wait_until'} - time();
    return $wait_Time;
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2008 ©, The Regents of The University of Michigan, All Rights Reserved

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
