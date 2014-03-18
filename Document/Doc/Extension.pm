package Document::Doc::Extension;


=head1 NAME

Document::Doc::Extension

=head1 DESCRIPTION

This base class is a sibling of ::Data and ::vSolrMetadataAPI.  Its
subclasses handle the logic for field creation that includes the id,
vol_id, chunk_seq, type_s and child Document::Doc objects of nested
document structures.

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;
use warnings;

use base qw(Document::Doc);
use Utils;

use Search::Constants;

sub new {
    my $class = shift;
    my $param_hashref = shift;

    my $self = {};
    bless $self, $class;

    my $facade = $param_hashref->{_facade};
    $self->__e_my_facade($facade);

    return $self;
}

# ---------------------------------------------------------------------

=item build_extension_fields

Description

=cut

# ---------------------------------------------------------------------
sub build_extension_fields {
    my $self = shift;
    my ($C, $state, $level, $granularity, $type) = @_;

    my $status = IX_NO_ERROR;

    my $item_id = $self->__e_my_facade->D_get_doc_id;
    my $vol_id = wrap_string_in_tag($item_id, 'field', [['name', 'vol_id']]);

    $self->extension_fields(\$vol_id);

    return $status;
}


# ---------------------------------------------------------------------

=item Accessors

Description

=cut

# ---------------------------------------------------------------------
sub __e_my_facade {
    my $self = shift;
    my $facade = shift;

    if (defined $facade) {
        $self->{_e_my_facade} = $facade;
    }
    return $self->{_e_my_facade};
}

# ---------------------------------------------------------------------

=item PUBLIC: extension_fields

Description

=cut

# ---------------------------------------------------------------------
sub extension_fields {
    my $self = shift;
    if (scalar @_) {
        push( @{ $self->{_extension_fields_ref} }, @_ );
    }
    else {
        my $data_ref;
        foreach my $ref (@{ $self->{_extension_fields_ref} }) {
            $$data_ref .= $$ref;
        }
        return $data_ref;
    }
    return undef;
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
