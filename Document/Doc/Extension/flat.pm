package Document::Doc::Extension::flat;


=head1 NAME

Document::Doc::Extension::flat

=head1 DESCRIPTION

This subclass of Document::Doc::Extension handles
the logic for field creation for flat structured Solr documents.

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;
use warnings;

use base qw(Document::Doc::Extension);
use Utils;

use Search::Constants;

# ---------------------------------------------------------------------

=item build_extension_fields

Description

=cut

# ---------------------------------------------------------------------
sub build_extension_fields {
    my $self = shift;
    my ($C, $state, $level, $granularity, $type) = @_;

    my $status = $self->SUPER::build_extension_fields(@_);
    return $status unless ($status == IX_NO_ERROR);
    # POSSIBLY NOTREACHED

    my $item_id = $self->__e_my_facade->D_get_doc_id;

    my $id =
      ($granularity eq '0')
        ? wrap_string_in_tag($item_id, 'field', [['name', 'id']])
          : wrap_string_in_tag(($item_id . "_$state"), 'field', [['name', 'id']]);
    
    my $chunk_seq =
      ($type eq 'token') 
        ? wrap_string_in_tag($state, 'field', [['name', 'chunk_seq']])
          : '';

    my $extension_fields = $id . $chunk_seq;

    $self->extension_fields(\$extension_fields);

    return $status;
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
