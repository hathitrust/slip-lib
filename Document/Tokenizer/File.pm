package Document::Tokenizer::File;

=head1 NAME

Document::Tokenizer::File

=head1 DESCRIPTION

This class implements division the textual content of a repository
object into a number of chunks each chunk consists of one or more,
or possibly all, files in the object.

=head1 SYNOPSIS

see Tokenizer.pm

=head1 METHODS

=over 8

=cut


use strict;
use warnings;

use base qw(Document::Tokenizer);

use Utils;


# ---------------------------------------------------------------------

=item PRIVATE: __initialize

Description

=cut

# ---------------------------------------------------------------------
sub __initialize {
    my $self = shift;
    
    my $buf = [];

    my $granularity = $self->__T_granularity;
    my $mets = $self->T_get_METS;
    my $num_files = $mets->num_files;
    my $file_arr_ref = $mets->filelist; 
            
    my $chunk = 0;
    my $file_num = 0;
    my $file_ct = 0;
        
    # insert N files into each chunk to implement granularity = N
    while (1) {
        my $filename_arr_ref = [];
        while ( $file_num < $num_files && $file_ct < $granularity ) {
            my $filename = $file_arr_ref->[$file_num++];
            push(@$filename_arr_ref, $filename);
            $file_ct++;
        }
        $buf->[$chunk] = $filename_arr_ref;
            
        last if ($file_num >= $num_files);
            
        $file_ct = 0;
        $chunk++;
    }
    
    $self->T_main_buffer($buf);
}

# ---------------------------------------------------------------------

=item PRIVATE: __T_granularity

Description

=cut

# ---------------------------------------------------------------------
sub __T_granularity {
    my $self = shift;

    if ($self->{_granularity} == 0) {
        $self->{_granularity} = $self->T_get_METS->num_files;
    }
    return $self->{_granularity};
}

# ---------------------------------------------------------------------

=item PRIVATE: __T_tokenization_type

Description

=cut

# ---------------------------------------------------------------------
sub __T_tokenization_type {
    my $self = shift;
    return 'file';
}

# ---------------------------------------------------------------------

=item PUBLIC: T_num_chunks

Description

=cut

# ---------------------------------------------------------------------
sub T_num_chunks {
    my $self = shift;
    return scalar @{ $self->T_main_buffer };
}


# ---------------------------------------------------------------------

=item PUBLIC: T_get_chunk

Description

=cut

# ---------------------------------------------------------------------
sub T_get_chunk {
    my $self = shift;
    my $N = shift;

    my $num_chunks = $self->T_num_chunks;
    ASSERT($N <= $num_chunks, qq{chunk number="$N" is out of range});    

    my $buf;
    my $filename_arr_ref = $self->T_main_buffer->[$N-1];
    foreach my $filename (@$filename_arr_ref) {
        my $ref = $self->T_read_file($filename);
        unless (length $$ref) {
            $ref = $self->T_get_empty_data_token;
        }
        $self->T_process_buffer($ref);

        $$buf .= $$ref;
    }

    # no files at all
    unless ($buf) {
        $buf = $self->T_get_empty_data_token;
    }
    
    return $buf;
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
