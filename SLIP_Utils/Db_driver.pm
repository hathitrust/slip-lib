package SLIP_Utils::Db_driver;


=head1 NAME

Db_driver

=head1 DESCRIPTION

This class is a non-OO database interface


=head1 VERSION

$Id: Db_driver.pm,v 1.6 2009/09/21 13:15:10 pfarber Exp $

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;

# App
use Utils;
use Debug::DUtils;

use Context;
use DbUtils;


# =====================================================================
# =====================================================================
#
#    Control tables:   [slip_driver_control] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item Renumber_driver

Description

=cut

# ---------------------------------------------------------------------
sub Renumber_driver {
    my ($C, $dbh, $from_run, $to_run) = @_;

    my $statement = qq{UPDATE slip_driver_control SET run=$to_run WHERE run=$from_run};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
}



# ---------------------------------------------------------------------

=item delete_driver

Description

=cut

# ---------------------------------------------------------------------
sub delete_driver {
    my ($C, $dbh, $run) = @_;

    my ($statement, $sth);

    $statement = qq{DELETE FROM slip_driver_control WHERE run=$run};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}

# ---------------------------------------------------------------------

=item init_driver

Description

=cut

# ---------------------------------------------------------------------
sub init_driver {
    my ($C, $dbh, $run, $stage) = @_;

    my ($statement, $sth);

    $statement = qq{REPLACE INTO slip_driver_control SET run=$run, enabled=0, stage='$stage'};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}

# ---------------------------------------------------------------------

=item Select_driver_enabled

Description

=cut

# ---------------------------------------------------------------------
sub Select_driver_enabled {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT enabled FROM slip_driver_control WHERE run=$run};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    my $enabled = $sth->fetchrow_array || 0;

    return $enabled;
}


# ---------------------------------------------------------------------

=item set_driver_enabled

Description

=cut

# ---------------------------------------------------------------------
sub set_driver_enabled {
    my ($C, $dbh, $run, $enabled) = @_;

    my $sth;
    my $statement;

    $statement = qq{UPDATE slip_driver_control SET enabled=$enabled WHERE run=$run};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}


# ---------------------------------------------------------------------

=item set_driver_stage

Description

=cut

# ---------------------------------------------------------------------
sub set_driver_stage {
    my ($C, $dbh, $run, $stage) = @_;

    my $statement = qq{UPDATE slip_driver_control SET stage='$stage' WHERE run=$run};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
}


# ---------------------------------------------------------------------

=item Select_driver_stage

Description

=cut

# ---------------------------------------------------------------------
sub Select_driver_stage {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT stage FROM slip_driver_control WHERE run=$run};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $stage = $sth->fetchrow_array;

    return $stage;
}


1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2009-13 ©, The Regents of The University of Michigan, All Rights Reserved

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
