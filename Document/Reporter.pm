package Document::Reporter;


=head1 NAME

Document::Reporter

=head1 DESCRIPTION

This package combined debugging and logging.

=head1 SYNOPSIS

use Document::Reporter;
report($where, $what);

=head1 METHODS

=over 8

=cut

use Debug::DUtils;
use Utils::Logger;

use Exporter;
use base qw(Exporter);

our @EXPORT = qw( report );


# ---------------------------------------------------------------------

=item reprot

Description

=cut

# ---------------------------------------------------------------------
sub report {
    my $msg = shift;
    my $log = shift;
    my $debug_switch = shift;

    if ( DEBUG($debug_switch) || $log ) {
        my $host = `hostname`; chomp($host);
        my ($package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hints, $bitmask, $hinthash) = caller(1);
        my $s = $msg . qq{ $host $$ $package $filename $line $subroutine};

        DEBUG($debug_switch, $s);
        Utils::Logger::__Log_simple($s) if ($log);
    }
}

1;

__END__

=back

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2014 ©, The Regents of The University of Michigan, All Rights Reserved

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
