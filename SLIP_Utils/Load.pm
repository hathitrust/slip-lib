package SLIP_Utils::Load;


=head1 NAME

SLIP_Utils::Load

=head1 DESCRIPTION

This package contains a file loading routine

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;
use SLIP_Utils::States;

# ---------------------------------------------------------------------

=item load_ids_from_file

Description

=cut

# ---------------------------------------------------------------------
sub load_ids_from_file {
    my $C = shift;
    my $filename = shift;

    my @arr;
    my $ok;
    eval {
        $ok = open(IDS, $filename);
    };
    if ($@) {
        my $s0 = qq{i/o ERROR:($@) reading file="$filename"\n};
        __output($s0);

        exit $SLIP_Utils::States::RC_BAD_ARGS;
    }

    if (! $ok) {
        my $s1 = qq{could not open file="$filename"\n};
        __output($s1);

        exit $SLIP_Utils::States::RC_BAD_ARGS;
    }

    while (my $id = <IDS>) {
        chomp($id);
        push(@arr, $id)
            if($id);
    }
    close (IDS);

    return \@arr;
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
