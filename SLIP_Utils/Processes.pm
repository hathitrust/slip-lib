package SLIP_Utils::Processes;


=head1 NAME

Processes

=head1 DESCRIPTION

Some useful subs.

=head1 VERSION

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut


# Perl
use Proc::ProcessTable;

# ---------------------------------------------------------------------

=item num_producers_running

Description

=cut

# ---------------------------------------------------------------------
sub num_producers_running {
    my ($C, $producer_pattern, $exclude_pattern) = @_;

    my $gp = new Proc::ProcessTable;
    my $num_producers = 0;

    # look for producer processes
    foreach $proc ( @{$gp->table} ) {
        # does this process match the pattern
        if ($proc->cmndline =~ /$producer_pattern/) {
            if ($proc->cmndline !~ /$exclude_pattern/) {
                $num_producers++;
            }
        }
    }

    return $num_producers;
}

# ---------------------------------------------------------------------

=item is_tomcat_running

Description

=cut

# ---------------------------------------------------------------------
sub is_tomcat_running {
    my ($C, $tomcat_pattern) = @_;

    my $gp = new Proc::ProcessTable;

    # look for tomcat processes
    foreach $proc ( @{$gp->table} ) {
        if ($proc->cmndline =~ /$tomcat_pattern/) {
            return 1;
        }
    }

    return 0;
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2008-10 ©, The Regents of The University of Michigan, All Rights Reserved

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
