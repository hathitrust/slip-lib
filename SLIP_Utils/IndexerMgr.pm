package SLIP_Utils::IndexerMgr;


=head1 NAME

indexerMgr

=head1 DESCRIPTION

This class encapsulates the creation and access to an Indexer object
for a given shard adding support for waiting when teh HTTP request to
Solr times out to prevent servelet containter thread exhaustion.

=head1 SYNOPSIS

my $indexer_mgr = new SLIP_Utils::IndexerMgr($C, $dbh, $run, $shard);

my $indexer = $indexer_mgr->get_indexer_For_shard($C);

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

sub new {
    my $class = shift;

    my $self = {};
    bless $self, $class;
    $self->_initialize(@_);

    return $self;
}


# ---------------------------------------------------------------------

=item _initialize

Initialize IndexerMgr object.

=cut

# ---------------------------------------------------------------------
sub _initialize {
    my $self = shift;
    my ($C, $dbh, $run, $shard) = @_;

    $self->{'dbh'} = $dbh;
    $self->{'run'} = $run;
    $self->{'shard'} = $shard;

    my $timeout =  $C->get_object('MdpConfig')->get('solr_indexer_timeout');
    my $indexer = SLIP_Utils::Solr::create_shard_Indexer_by_alias($C, $shard, $timeout);
    # Init obj
    my $io = IndexerPool::IndexerObj->new($indexer, $shard);

    $self->{'indexerobj'} = $io;
}

# ---------------------------------------------------------------------

=item set_shard_waiting

Description PUBLIC

=cut

# ---------------------------------------------------------------------
sub set_shard_waiting {
    my $self = shift;
    my $C = shift;

    my $Wait_For_secs = 0;

    # If a concurrent process has disabled this shard (error, job
    # control), do not mark it waiting.  It could be re-enabled but
    # would still not run even if the timeout situation no longer
    # applied.  Let it run and prove whether it is ok to run or not.

    my $dbh = $self->__get_dbh();
    my $run = $self->__get_run();
    my $shard = $self->__get_shard();

    if (Db::Select_shard_enabled($C, $dbh, $run, $shard)) {
        my $io = $self->__get_indexer_obj();

        if ($io->___get_delay_multiplier() > NO_DELAY) {
            # Already waiting: Escalate the wait
            my $new_Wait_multiplier = $io->___get_delay_multiplier() * DEFAULT_DELAY_MULTIPLIER;
            $Wait_For_secs = (DEFAULT_DELAY * $new_Wait_multiplier);
            my $new_Wait_Until = time() + $Wait_For_secs;

            $io->___set_Wait($new_Wait_Until, $new_Wait_multiplier);
        }
        else {
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
sub Reset_shard_waiting {
    my $self = shift;
    my $C = shift;

    my $io = $self->__get_indexer_obj();
    my $was_waiting = ($io->___get_delay_multiplier() > NO_DELAY);

    $io->___Reset_Wait();

    return $was_waiting;
}


# ---------------------------------------------------------------------

=item get_indexer_For_shard

Description PUBLIC

Return the indexer for the shard.  Caller must check shard is enabled
before selecting an indexer. If indexed asleep, block for the time
remaining in its nap because we have to return this indexer.

=cut

# ---------------------------------------------------------------------
sub get_indexer_For_shard {
    my $self = shift;
    my $C = shift;

    my $io = $self->__get_indexer_obj();
    my $wait_Time = $io->___get_Wait_Time();

    if ($wait_Time > 0) {
        sleep $wait_Time;
    }

    return $io->___get_indexer();
}

# ---------------------------------------------------------------------

=item __get_indexer_obj

Description

=cut

# ---------------------------------------------------------------------
sub __get_indexer_obj {
    my $self = shift;
    return $self->{'indexerobj'};
}

# ---------------------------------------------------------------------

=item __get_run

Description

=cut

# ---------------------------------------------------------------------
sub __get_run {
    my $self = shift;
    return $self->{'run'};
}

# ---------------------------------------------------------------------

=item __get_dbh

Description

=cut

# ---------------------------------------------------------------------
sub __get_dbh {
    my $self = shift;
    return $self->{'dbh'};
}


# ---------------------------------------------------------------------

=item __get_shard

Description

=cut

# ---------------------------------------------------------------------
sub __get_shard {
    my $self = shift;
    return $self->{'shard'};
}



#
#  ----------------- Indexer Object ------------------
#
package IndexerPool::IndexerObj;

sub new {
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
sub ___set_Wait {
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
sub ___get_delay_multiplier {
    my $self = shift;
    return $self->{'multiplier'};
}

# ---------------------------------------------------------------------

=item ___Reset_Wait

Description:

=cut

# ---------------------------------------------------------------------
sub ___Reset_Wait {
    my $self = shift;

    $self->{'wait_until'} = 0;
    $self->{'multiplier'} = SLIP_Utils::IndexerMgr::NO_DELAY;
}

# ---------------------------------------------------------------------

=item ___get_indexer

Description

=cut

# ---------------------------------------------------------------------
sub ___get_indexer {
    my $self = shift;
    return $self->{'indexer'};
}

# ---------------------------------------------------------------------

=item ___get_Wait_multiplier

Description

=cut

# ---------------------------------------------------------------------
sub ___get_Wait_multiplier {
    my $self = shift;
    return $self->{'multiplier'};
}


# ---------------------------------------------------------------------

=item ___get_Wait_Time

Description

=cut

# ---------------------------------------------------------------------
sub ___get_Wait_Time {
    my $self = shift;

    my $wait_Time = $self->{'wait_until'} - time();
    return $wait_Time;
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2008-12 ©, The Regents of The University of Michigan, All Rights Reserved

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
