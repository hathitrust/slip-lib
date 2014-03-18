=head1 NAME

Plugin_data_ratios

=head1 DESCRIPTION

This is a plugin to the Document::Doc::Data class.

Refer to Document/Plugins.txt for documentation on Plugins.

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;
use warnings;

use POSIX qw(ceil floor);

use Search::Constants;


# ---------------------------------------------------------------------

=item PLG_add_data_ratio_fields

Adds these fields to the Solr document

 <field name="numPages"     type="tint" indexed="true" stored="true">
 <field name="numChars"     type="tint" indexed="true" stored="true">
 <field name="charsPerPage" type="tint" indexed="true" stored="true">

=cut

# ---------------------------------------------------------------------
sub PLG_add_data_ratio_fields {
    my $self = shift;

    my $fields_ref;

    my $numPages = $self->d_my_facade->D_get_doc_tokenizer->T_granularity;
    $$fields_ref .= wrap_string_in_tag($numPages, 'field',
                                       [
                                        ['name', 'numPages'],
                                        ['type', 'tint'],
                                        ['stored', 'true'],
                                       ]);
    my $numChars = $self->d_my_num_chars;
    $$fields_ref .= wrap_string_in_tag($numChars, 'field',
                                       [
                                        ['name', 'numChars'],
                                        ['type', 'tint'],
                                        ['stored', 'true'],
                                       ]);
    my $ratio;
    if ($numPages == 0) {
        $ratio = 0;
    }
    else {
        if ($numChars == 0) {
            $ratio = 0;
        }
        else {
            $ratio = $numChars/$numPages;
            $ratio = ($ratio < 1.0) ? ceil($ratio) : floor($ratio);
        }
    }
    $$fields_ref .= wrap_string_in_tag($ratio, 'field',
                                       [
                                        ['name', 'charsPerPage'],
                                        ['type', 'tint'],
                                        ['stored', 'true'],
                                       ]);
    $self->data_fields($fields_ref);

    return IX_NO_ERROR;
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2013 ©, The Regents of The University of Michigan, All Rights Reserved

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

1;


