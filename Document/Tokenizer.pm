package Document::Tokenizer;

=head1 NAME

Document::Tokenizer

=head1 DESCRIPTION

This class implements division the textual content of a repository
object into a number of chunks.

=head1 SYNOPSIS

my $tokenizer = new Document::Tokenizer::{File,Token}($C, $granularity, $METS, $extractor);
my $chunk_of_text = $tokenizer->T_get_chunk($N);

=head1 METHODS

=over 8

=cut


use strict;
use warnings;

use Utils;
use ObjFactory;

use SLIP_Utils::Common;

my $Algorithm_Root = 'Document::Algorithms::';

sub new {
    my $class = shift;
    my $param_hashref = shift;

    my $self = {};
    bless $self, $class;

    $self->{_context} = $param_hashref->{_C};
    $self->{_config} =  $self->{_context}->get_object('MdpConfig');
    $self->{_granularity} = $param_hashref->{_granularity};
    $self->{_mets} = $param_hashref->{_mets};
    $self->{_extractor} = $param_hashref->{_extractor};

    $self->__instantiate_algorithms;

    $self->__initialize;

    return $self;
}

# ---------------------------------------------------------------------

=item __instantiate_algorithms

Description

=cut

# ---------------------------------------------------------------------
sub __instantiate_algorithms {
    my $self = shift;

    my $algorithms = [];

    my $config = $self->T_get_config;
    if ( $config->has('document_data_algorithm_classes') ) {
        my @classes = $config->get('document_data_algorithm_classes');
        if (scalar @classes) {
            my $C = $self->T_get_context;

            foreach my $c (@classes) {
                my $class = $Algorithm_Root . $c;
                my $of_attrs = {
                                'class_name' => $class,
                                'parameters' => {
                                                 '_C'  => $C,
                                                },
                               };
                my $alg = ObjFactory->create_instance($C, $of_attrs);
                push(@$algorithms, $alg);
            }
        }
    }

    $self->{_algorithms} = $algorithms;
}

# ---------------------------------------------------------------------

=item Accessors

Description

=cut

# ---------------------------------------------------------------------
sub T_get_algorithms {
    my $self = shift;
    return $self->{_algorithms};
}

sub T_get_context {
    my $self = shift;
    return $self->{_context};
}

sub T_get_config {
    my $self = shift;
    return $self->{_config};
}

sub T_get_METS {
    my $self = shift;
    return $self->{_mets};
}

sub T_get_extractor {
    my $self = shift;
    return $self->{_extractor};
}

# ---------------------------------------------------------------------

=item PUBLIC: T_num_chunks

Description

=cut

# ---------------------------------------------------------------------
sub T_num_chunks {
    my $self = shift;
    ASSERT(0, qq{T_num_chunks not defined in Document::Tokenizer::<subclass>});
}

# ---------------------------------------------------------------------

=item PUBLIC: T_granularity

Description

=cut

# ---------------------------------------------------------------------
sub T_granularity {
    my $self = shift;

    return $self->__T_granularity;
}

# ---------------------------------------------------------------------

=item PUBLIC: T_tokenization_type

Description

=cut

# ---------------------------------------------------------------------
sub T_tokenization_type {
    my $self = shift;

    return $self->__T_tokenization_type;
}

# ---------------------------------------------------------------------

=item PUBLIC: T_get_chunk

Description

=cut

# ---------------------------------------------------------------------
sub T_get_chunk {
    my $self = shift;
    my $N = shift;

    ASSERT(0, qq{T_get_chunk not defined in Document::Tokenizer::<subclass>});
}

# ---------------------------------------------------------------------

=item PUBLIC: T_process_buffer

Description

=cut

# ---------------------------------------------------------------------
sub T_process_buffer {
    my $self = shift;
    my $buf_ref = shift;

    SLIP_Utils::Common::clean_xml($buf_ref);

    my $algorithms = $self->T_get_algorithms;
    foreach my $alg (@$algorithms) {
        my $ref = {
                   _C   => $self->T_get_context,
                   _buf => $buf_ref,
                  };
        $alg->execute($ref);
    }
}

# ---------------------------------------------------------------------

=item PUBLIC: T_read_file

Description

=cut

# ---------------------------------------------------------------------
sub T_read_file {
    my $self = shift;
    my $filename = shift;

    my $ref;
    my $path = $self->T_get_extractor->E_extraction_dir . '/' . $filename;

    if ($path =~ m,.+\.xml$,) {
        require XML::LibXML;
        my $buf = XML::LibXML->load_xml(location => $path, load_ext_dtd => 0)->textContent;
        $ref = \$buf;
    }
    else {
        $ref = read_file($path);
    }

    return $ref;
}


# ---------------------------------------------------------------------

=item T_get_empty_data_token

A buffer containing the contents of the "empty file"

=cut

# ---------------------------------------------------------------------
sub T_get_empty_data_token {
    my $self = shift;

    my $no_text_string = $self->T_get_config->get('ix_index_empty_string');
    return \$no_text_string;
}


# ---------------------------------------------------------------------

=item PUBLIC: T_main_buffer

Description

=cut

# ---------------------------------------------------------------------
sub T_main_buffer {
    my $self = shift;
    my $ref = shift;

    if (defined $self->{_main_buffer}) {
        if (defined $ref) {
            ASSERT(0, qq{__main_buffer attempt to overwrite buffer in Document::Tokenizer});
        }
        else {
            return $self->{_main_buffer};
        }
    }
    else {
        ASSERT(0, qq{__main_buffer not defined in Document::Tokenizer}) unless (defined $ref);
    }

    return $self->{_main_buffer} = $ref;
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
