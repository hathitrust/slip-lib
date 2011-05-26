package SLIP_Utils::Released;


=head1 NAME

SLIP_Utils::Released

=head1 DESCRIPTION

Check state of index release flag for the active production index

=head1 VERSION

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

# ---------------------------------------------------------------------

=item released

Description

=cut

# ---------------------------------------------------------------------
sub released {
    my $TODAY = `date +%Y-%m-%d`;
    chomp($TODAY);

    my $release_flag = "$ENV{SDRROOT}/flags/web/lss-release-$TODAY";
    my $index_released = (-e $release_flag);

    my $msg = $index_released 
      ? "Release flag=$release_flag found"
        : "Release flag=$release_flag not present";

    return ($index_released, $msg);
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
