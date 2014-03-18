package Document::Doc::Data::File;

=head1 NAME

Document::Doc::Data::File

=head1 DESCRIPTION

This class encapsulates the retrieval of text to build a Solr document
that contains the text of one or all "pages" of an item.

A "page" consists of the contents of one or all OCR .txt files or
of the entire textual content of an XML structured document.

This subclass builds auxillary fields for the seq and pgnum Solr
document fields.

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;
use warnings;

use base qw( Document::Doc::Data );

use Utils;

# ---------------------------------------------------------------------

=item build_auxiliary_data_fields 

Description

=cut

# ---------------------------------------------------------------------
sub build_auxiliary_data_fields {
    my $self = shift;
    my ($C, $state) = @_;
    
    # seq == state
    my $seq_field = wrap_string_in_tag($state, 'field', [['name', 'seq']]);

    # pgnum field
    my $pgnum = $self->d_my_METS->seq2pgnum_map->{$state} || '0';
    my $pgnum_field = wrap_string_in_tag($pgnum, 'field', [['name', 'pgnum']]);

    my $aux = $seq_field . $pgnum_field;
    
    return \$aux;
}

1;

__END__

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
