package SLIP_Utils::DatabaseWrapper;

=head1 NAME

DatabaseWrapper

=head1 DESCRIPTION

This package wraps calls to Database->new allowing for retry over an
extended time interval to handle database maintenenace intervals.

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;
use warnings;

use Database;

use SLIP_Utils::States;
use SLIP_Utils::Common;

use constant DATABASE_RETRY_SLEEP => 300; # 5 minutes
use constant DATABASE_MAX_TRIES   => 1;


# ---------------------------------------------------------------------

=item GetDatabaseConnection

Description

=cut

# ---------------------------------------------------------------------
sub GetDatabaseConnection {
    my ($C, $script) = @_;

    my $db;
    my $tries = 0;
    my $trying = 1;
    
    while ($trying) {
        eval {
            $db = new Database('ht_maintenance');
            $tries++;
        };
        if ($@) {
            if ($tries > DATABASE_MAX_TRIES) { 
                my $rc = $SLIP_Utils::States::RC_DATABASE_CONNECT;
                SLIP_Utils::Common::Log_database_connection_error($C, $script, $@);
                my $s = "Database connect error: $@";
                __output($s);
                __non_interactive_err_output($rc, $s);
                exit $rc;
            }
            else {
                sleep DATABASE_RETRY_SLEEP;
            }
        }
        else {
            $trying = 0;
        }
    }
    
    # Ensure connection following database maintenance intervals in
    # DbUtils::prep_n_execute
    $ENV{DATABASE_LONG_RETRY} = 1;
    
    $C->set_object('Database', $db);
    my $dbh = $db->get_DBH();

    return $dbh;
}


1;

__END__

=back

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
