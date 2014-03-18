package Document::Tokenizer::Token;

=head1 NAME

Document::Tokenizer::Token

=head1 DESCRIPTION

This class implements division the textual content of a repository
object into a number of chunks of "words" where each chunk is as close
as possible to N where N is a desired number of tokens.

=head1 SYNOPSIS

see Tokenizer.pm

=head1 METHODS

=over 8

=cut


use strict;
use warnings;

use base qw(Document::Tokenizer);

use Utils;
use Debug::DUtils;

# ---------------------------------------------------------------------

=item PRIVATE: __initialize

Description

=cut

# ---------------------------------------------------------------------
sub __initialize {
    my $self = shift;

    my $mets = $self->T_get_METS;
    my $num_files = $mets->num_files;
    my $file_arr_ref = $mets->filelist;

    my $tokens = [];

    foreach my $filename (@$file_arr_ref) {
        my $ref = $self->T_read_file($filename);
        unless (length $$ref) {
            $ref = $self->T_get_empty_data_token;
        }
        $self->T_process_buffer($ref);

        push( @$tokens, split(/[ ]+|\t+/, $$ref) );
    }

    my $num_tokens = scalar @$tokens;
    my $granularity = $self->__T_granularity($num_tokens);

    my ($A_chunk_size, $num_A_chunks, $B_chunk_size, $num_B_chunks) =
      $self->__get_chunk_data(
                              $num_tokens,
                              $granularity
                             );

    my $token_offsets = [];

    my $offset = 0;
    for (1 .. $num_A_chunks) {
        push(@$token_offsets, { offset => $offset, length => $A_chunk_size });
        $offset += $A_chunk_size;
    }
    for ( 1 .. $num_B_chunks) {
        push(@$token_offsets, { offset => $offset, length => $B_chunk_size });
        $offset += $B_chunk_size;
    }
    ASSERT($offset == $num_tokens, qq{error finding token offsets});

    my $buf = {
               _tokens => $tokens,
               _offsets => $token_offsets,
              };

    $self->T_main_buffer($buf);
}

# ---------------------------------------------------------------------

=item PRIVATE: __T_granularity

Description

=cut

# ---------------------------------------------------------------------
sub __T_granularity {
    my $self = shift;
    my $num_tokens = shift;

    if ($self->{_granularity} == 0) {
        ASSERT(defined $num_tokens, qq{num_tokens not defined for granularity==0});
        $self->{_granularity} = $num_tokens;
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
    return 'token';
}

# ---------------------------------------------------------------------

=item PUBLIC: T_num_chunks

Description

=cut

# ---------------------------------------------------------------------
sub T_num_chunks {
    my $self = shift;

    return scalar @{ $self->T_main_buffer->{_offsets} };
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

    my $offset_ref = $self->T_main_buffer->{_offsets}->[$N-1];

    my $tokens = $self->T_main_buffer->{_tokens};

    my $chunk_length = $offset_ref->{length};
    my $start_index = $offset_ref->{offset};
    my $end_index = $start_index + $chunk_length - 1;

    my $buf = join( ' ', @$tokens[ $start_index ..  $end_index ] );
    my $ref = \$buf;

    unless ($$ref) {
        $ref = $self->T_get_empty_data_token;
    }

    return $ref;
}

# ---------------------------------------------------------------------

=item __get_chunk_data

Description

=cut

# ---------------------------------------------------------------------
sub __get_chunk_data {
    my $self = shift;
    my ($num_tokens, $configured_chunk_size) = @_;

    my $chunk_size = $configured_chunk_size;
    my $num_chunks = int($num_tokens / $chunk_size);
    my $remainder = $num_tokens % $chunk_size;

    my $sub = 'foo';

    my $left = 0;
    my $num_A_chunks = 0;
    my $A_chunk_size = 0;
    my $num_B_chunks = 0;
    my $B_chunk_size = 0;

    if ($num_chunks > 1) {
        ($sub, $left, $A_chunk_size, $num_A_chunks, $B_chunk_size, $num_B_chunks) =
          __num_chunks_greater_than_1($num_tokens, $num_chunks, $chunk_size, $remainder);
    }
    elsif ($num_chunks == 1) {
        ($sub, $left, $A_chunk_size, $num_A_chunks, $B_chunk_size, $num_B_chunks) =
          __num_chunks_equal_1($num_tokens);
    }
    elsif ($num_chunks == 0) {
        ($sub, $left, $A_chunk_size, $num_A_chunks, $B_chunk_size, $num_B_chunks) =
          __num_chunks_equal_0($num_tokens);
    }
    else {
        ASSERT(0, qq{chunk failure in trailing else. num_chunks="$num_chunks"});
    }

    DEBUG('doc',
          sub {
              my $s = sprintf("sub=%s num_chunks=%-3d num_tokens=%-3d chunk_size=%-3d A_chunk_size=%-3d num_A_chunks=%-3d B_chunk_size=%-3d num_B_chunks=%-3d left_over=%-3d remainder=%-3d\n",
                              $sub, $num_chunks,  $num_tokens,    $chunk_size,    $A_chunk_size,    $num_A_chunks,    $B_chunk_size,    $num_B_chunks,    $left,         $remainder);
              return $s;
          });
    ASSERT($left == 0, qq{Left-over tokens="$left" subroutine="$sub"});

    return ($A_chunk_size, $num_A_chunks, $B_chunk_size, $num_B_chunks);
}


# ---------------------------------------------------------------------

=item __num_chunks_equal_0

Chunk size is less than number of tokens in document.

=cut

# ---------------------------------------------------------------------
sub __num_chunks_equal_0 {
    my $num_tokens = shift;

    my $num_B_chunks = 0;
    my $B_chunk_size = 0;

    my $num_A_chunks = 1;
    my $A_chunk_size = $num_tokens;

    my $left_over = $num_tokens - ($A_chunk_size * $num_A_chunks) - ($B_chunk_size * $num_B_chunks);

    return ('num_chunks_equal_0', $left_over, $A_chunk_size, $num_A_chunks, $B_chunk_size, $num_B_chunks);

}

# ---------------------------------------------------------------------

=item __num_chunks_equal_1

Chunk size is

=cut

# ---------------------------------------------------------------------
sub __num_chunks_equal_1 {
    my $num_tokens = shift;

    my $num_B_chunks = 1;
    my $num_A_chunks = 1;
    my $A_chunk_size = int($num_tokens / 2);
    my $B_chunk_size = $num_tokens - $A_chunk_size;

    my $left_over = $num_tokens - ($A_chunk_size * $num_A_chunks) - ($B_chunk_size * $num_B_chunks);

    return ('num_chunks_equal_1', $left_over, $A_chunk_size, $num_A_chunks, $B_chunk_size, $num_B_chunks);
}

# ---------------------------------------------------------------------

=item __num_chunks_greater_than_1

Description

=cut

# ---------------------------------------------------------------------
sub __num_chunks_greater_than_1 {
    my ($num_tokens, $num_chunks, $chunk_size, $remainder) = @_;

    if ($remainder < $num_chunks) {
        return __num_chunks_greater_than_1_small_remainder($num_tokens, $num_chunks, $chunk_size, $remainder);
    }
    else {
        if ($remainder % $num_chunks) {
            return __num_chunks_greater_than_1_large_odd_remainder($num_tokens, $num_chunks, $chunk_size, $remainder);
        }
        else {
            return __num_chunks_greater_than_1_large_even_remainder($num_tokens, $num_chunks, $chunk_size, $remainder);
        }
    }
    return ('num_chunks_greater_than_1', -1, -1, -1, -1, -1);
}

# ---------------------------------------------------------------------

=item __num_chunks_greater_than_1_small_remainder

Description

=cut

# ---------------------------------------------------------------------
sub __num_chunks_greater_than_1_small_remainder {
    my ($num_tokens, $num_chunks, $chunk_size, $remainder) = @_;

    # fewer items than chunks into which to place them,
    # partition num_chunks into A: those that have chunk_size
    # items and B: those that will each get one of the items
    # from the remainder
    my $num_B_chunks = $remainder;
    my $num_A_chunks = $num_chunks - $num_B_chunks;
    my $A_chunk_size = $chunk_size;
    my $B_chunk_size = $chunk_size + 1;

    my $left_over = $num_tokens - ($A_chunk_size * $num_A_chunks) - ($B_chunk_size * $num_B_chunks);

    return ('num_chunks_greater_than_1_small_remainder', $left_over, $A_chunk_size, $num_A_chunks, $B_chunk_size, $num_B_chunks);
}

# ---------------------------------------------------------------------

=item __num_chunks_greater_than_1_large_odd_remainder

Description

=cut

# ---------------------------------------------------------------------
sub __num_chunks_greater_than_1_large_odd_remainder {
    my ($num_tokens, $num_chunks, $chunk_size, $remainder) = @_;

    # increase chunk size uniformly leaving remainder less than num_chunks and call
    $chunk_size = $chunk_size + int($remainder / $num_chunks);
    $remainder = $num_tokens - ($chunk_size * $num_chunks);

    return __num_chunks_greater_than_1_small_remainder($num_tokens, $num_chunks, $chunk_size, $remainder);
}

# ---------------------------------------------------------------------

=item __num_chunks_greater_than_1_large_even_remainder

Description

=cut

# ---------------------------------------------------------------------
sub __num_chunks_greater_than_1_large_even_remainder {
    my ($num_tokens, $num_chunks, $chunk_size, $remainder) = @_;

    my $num_A_chunks = $num_chunks;
    my $A_chunk_size = $chunk_size + int($remainder / $num_chunks);

    my $left_over = $num_tokens - ($A_chunk_size * $num_A_chunks);

    return ('num_chunks_greater_than_1_large_even_remainder', $left_over, $A_chunk_size, $num_A_chunks, 0, 0);
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
