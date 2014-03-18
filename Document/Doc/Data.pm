package Document::Doc::Data;


=head1 NAME

Document::Doc::Data

=head1 DESCRIPTION

This class is the base class encapsulating the Data service on behalf
of Document::Doc

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;
use warnings;

use base qw( Document::Doc );

use Utils;
use Debug::DUtils;
use MdpConfig;
use DataTypes;

use SLIP_Utils::Common;
use Search::Constants;

use Document::METS;
use Document::Extractor;
use Document::Plugger;


sub new {
    my $class = shift;
    my $param_hashref = shift;

    my $self = {};
    bless $self, $class;

    my $facade = $param_hashref->{_facade};
    $self->d_my_facade($facade);

    my $C = $self->d_my_facade->D_get_doc_context;
    $self->initialize_plugins($C);

    return $self;
}


# ---------------------------------------------------------------------

=item Accessors

Description

=cut

# ---------------------------------------------------------------------
sub d_my_facade {
    my $self = shift;
    my $facade = shift;

    if (defined $facade) {
        $self->{_d_my_facade} = $facade;
    }
    return $self->{_d_my_facade};
}

sub d_my_tokenizer {
    my $self = shift;
    return $self->d_my_facade->D_get_doc_tokenizer;
}

sub d_my_extractor {
    my $self = shift;
    return $self->d_my_facade->D_get_doc_extractor;
}

sub d_my_METS {
    my $self = shift;
    return $self->d_my_facade->D_get_doc_METS;
}

sub d_my_num_chars {
    my $self = shift;
    my $num = shift;

    unless (exists $self->{_d_my_num_chars}) {
        $self->{_d_my_num_chars} = 0;
    }

    if (defined $num) {
        $self->{_d_my_num_chars} += $num;
    }
    return $self->{_d_my_num_chars};
}

sub data_fields {
    my $self = shift;
    if (scalar @_) {
        push( @{ $self->{_data_fields_ref} }, @_ );
    }
    else {
        my $data_ref;
        foreach my $ref (@{ $self->{_data_fields_ref} }) {
            $$data_ref .= $$ref;
        }
        return $data_ref;
    }
    return undef;
}

# ---------------------------------------------------------------------

=item PUBLIC: build_auxiliary_data_fields

Description

=cut

# ---------------------------------------------------------------------
sub build_auxiliary_data_fields {
    my $self = shift;
    my ($C, $state) = @_;

    my $aux = '';
    return \$aux;
}

# ---------------------------------------------------------------------

=item PUBLIC: build_data_fields

For fields like ocr, etc. that do not come directly from a metadata
source like the catalog.

=cut

# ---------------------------------------------------------------------
sub build_data_fields {
    my $self = shift;
    my ($C, $state) = @_;

    # METS must be valid to proceed
    unless ($self->d_my_METS->dataset_is_valid) {
        return IX_DATA_FAILURE;
    }

    # Extraction must be OK to proceed
    unless ($self->d_my_extractor->E_status == IX_NO_ERROR) {
        return IX_DATA_FAILURE;
    }

    my $item_id = $self->D_get_doc_id;

    # text field
    my $text_ref = $self->d_my_tokenizer->T_get_chunk($state);
    $self->d_my_num_chars( length($$text_ref) );
    wrap_string_in_tag_by_ref($text_ref, 'field', [['name', 'ocr']]);



    my $aux_fields_ref = $self->build_auxiliary_data_fields($C, $state);

    $self->data_fields($text_ref, $aux_fields_ref);

    return IX_NO_ERROR;
}


1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2012-14 ©, The Regents of The University of Michigan, All Rights Reserved

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
