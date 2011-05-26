package Document;


=head1 NAME

Document (doc)

=head1 DESCRIPTION

This class returns a document object structured corectly for
submission to an Indexer.  The Indexer subclasses are currently XPAT
and Solr. The XPAT Indexer types are basically defunct.

=head1 SYNOPSIS



=head1 METHODS

=over 8

=cut

use strict;

# Perl
use Encode;

# Local
use Context;
use Utils;
use Debug::DUtils;
use ObjFactory;


sub new {
    my $class = shift;

    my $self = {};
    bless $self, $class;
    $self->_initialize(@_);

    return $self;
}


# =====================================================================
# ==
# ==                       Abstract Public Interface
# ==
# =====================================================================


# ---------------------------------------------------------------------

=item _initialize

Initialize DocumentFactory object.

=cut

# ---------------------------------------------------------------------
sub create_document {
    ASSERT(0, qq{Pure virtual method create_document() not implemented in a subclass of Document});
}

# ---------------------------------------------------------------------

=item PUBLIC PURE VIRTUAL: build_document

Description

=cut

# ---------------------------------------------------------------------
sub build_document {
    ASSERT(0, qq{Pure virtual method build_document() not implemented in a subclass of Document});
}

# ---------------------------------------------------------------------

=item PUBLIC PURE VIRTUAL: finish_document

Description

=cut

# ---------------------------------------------------------------------
sub finish_document {
    ASSERT(0, qq{Pure virtual method finish_document() not implemented in a subclass of Document});
}

# ---------------------------------------------------------------------

=item PUBLIC PURE VIRTUAL: get_document_content

Description

=cut

# ---------------------------------------------------------------------
sub get_document_content {
    ASSERT(0, qq{Pure virtual method get_document_content() not implemented in a subclass of Document});
}

# ---------------------------------------------------------------------

=item PUBLIC PURE VIRTUAL: get_state_variable

Description

=cut

# ---------------------------------------------------------------------
sub get_state_variable {
    ASSERT(0, qq{Pure virtual method get_state_variable() not implemented in a subclass of Document});
}

# ---------------------------------------------------------------------

=item PUBLIC PURE VIRTUAL: get_metadata_fields

For title, author, date etc. to use Lucene for search/facet

=cut

# ---------------------------------------------------------------------
sub get_metadata_fields {
    ASSERT(0, qq{Pure virtual method get_metadata_fields() not implemented in subclass of Document});
}

# ---------------------------------------------------------------------

=item PUBLIC PURE VIRTUAL: get_data_fields

For fields like ocr, rights, etc that do not come directly from a metadata source like the catalog.

=cut

# ---------------------------------------------------------------------
sub get_data_fields {
    ASSERT(0, qq{Pure virtual method get_data_fields() not implemented in subclass of Document});
}


# =====================================================================
# ==
# ==                       Private Interface
# ==
# =====================================================================

# ---------------------------------------------------------------------

=item PRIVATE CLASS METHOD: ___num2utf8

Description

=cut

# ---------------------------------------------------------------------
sub ___num2utf8
{
    my ( $t ) = @_;
    my ( $trail, $firstbits, @result );

    if    ($t<0x00000080) { $firstbits=0x00; $trail=0; }
    elsif ($t<0x00000800) { $firstbits=0xC0; $trail=1; }
    elsif ($t<0x00010000) { $firstbits=0xE0; $trail=2; }
    elsif ($t<0x00200000) { $firstbits=0xF0; $trail=3; }
    elsif ($t<0x04000000) { $firstbits=0xF8; $trail=4; }
    elsif ($t<0x80000000) { $firstbits=0xFC; $trail=5; }
    else {
        ASSERT(0, qq{Too large scalar value="$t": cannot be converted to UTF-8.});
    }
    for (1 .. $trail)
    {
        unshift (@result, ($t & 0x3F) | 0x80);
        $t >>= 6;         # slight danger of non-portability
    }
    unshift (@result, $t | $firstbits);
    pack ("C*", @result);
}

# ---------------------------------------------------------------------

=item PRIVATE CLASS METHOD: ___Google_NCR_to_UTF8

Description

=cut

# ---------------------------------------------------------------------
sub ___Google_NCR_to_UTF8 {
    my $sRef = shift;
    $$sRef =~ s,\#{([0-9]+)},___num2utf8($1),ges;
}



# =====================================================================
# ==
# ==                       Public Interface
# ==
# =====================================================================

# ---------------------------------------------------------------------

=item PUBLIC CLASS METHOD: clean_xml

The input ref may be invalid UTF-8 because of the forgiving read.  Try
to fix it

As of this date Fri Oct 5 14:36:30 2007 there are 2 problems with the
Google OCR:

1) Single byte control characters like \x01 and \x03 which are legal
UTF-8 but illegal in XML

2) Invalid UTF-8 encoding sequences like \xFF

The following eliminates ranges of invalid control characters (1)
while preserving TAB=U+0009, NEWLINE=U+000A and CARRIAGE
RETURN=U+000D. To handle (2) we eliminate all byte values with high
bit set.  We try to test for this so we do not destroy valid UTF-8
sequences.

=cut

# ---------------------------------------------------------------------
sub clean_xml {
    my $s_ref = shift;

    $$s_ref = Encode::encode_utf8($$s_ref);
    ___Google_NCR_to_UTF8($s_ref);
    $$s_ref = Encode::decode_utf8($$s_ref);

    if (! Encode::is_utf8($$s_ref, 1))
    {
        $$s_ref = Encode::encode_utf8($$s_ref);
        $$s_ref =~ s,[\200-\377]+,,gs;
        $$s_ref = Encode::decode_utf8($$s_ref);
    }
    # Decoding changes invalid UTF-8 bytes to the Unicode REPLACEMENT
    # CHARACTER U+FFFD.  Replace that char with a SPACE for nicer
    # viewing.
    $$s_ref =~ s,[\x{FFFD}]+, ,gs;

    # At some time after Wed Aug 5 16:32:34 2009, Google will begin
    # CJK segmenting using 0x200B ZERO WIDTH SPACE instead of 0x0020
    # SPACE.  To maintain compatibility change ZERO WIDTH SPACE to
    # SPACE until we have a Solr query segmenter.
    $$s_ref =~ s,[\x{200B}]+, ,gs;

    # Kill characters that are invalid in XML data. Valid XML
    # characters and ranges are:

    #  (c == 0x9) || (c == 0xA) || (c == 0xD)
    #             || ((c >= 0x20) && (c <= 0xD7FF))
    #             || ((c >= 0xE000) && (c <= 0xFFFD))
    #             || ((c >= 0x10000) && (c <= 0x10FFFF))

    # Note that since we have valid Unicode UTF-8 encoded at this
    # point we don't need to remove any other code
    # points. \x{D800}-\x{DFFF} compose surrogate pairs in UTF-16
    # and the rest are not valid Unicode code points.
    $$s_ref =~ s,[\000-\010\013-\014\016-\037]+, ,gs;

    # Protect against non-XML character data like "<"
    Utils::map_chars_to_cers($s_ref, [q{"}, q{'}], 1);
}


# ---------------------------------------------------------------------

=item PUBLIC: maybe_preserve_doc

Description

=cut

# ---------------------------------------------------------------------
sub maybe_preserve_doc {
    my $text_ref = shift;
    my $filename = shift;
    
    if (DEBUG('docfulldebug')) {
        my $clean_filename = $filename . '-clean';
        my $logdir = Utils::get_tmp_logdir();
        $clean_filename =~ s,^/ram/,$logdir/,;
        Utils::write_data_to_file($text_ref, $clean_filename);
        chmod(0666, $clean_filename) if (-o $clean_filename);
        DEBUG('docfulldebug', qq{DOC: CLEANED file=$clean_filename});
    }
}

# ---------------------------------------------------------------------

=item apply_algorithms

Description

=cut

# ---------------------------------------------------------------------
sub apply_algorithms {
    my $self = shift;
    my $C = shift;
    my $text_ref = shift;
    my $class = shift;

    my $garbage_class = $C->get_object('MdpConfig')->get($class);
    if ($garbage_class) {
        my $of = new ObjFactory;
        my %of_attrs = (
                        'class_name' => $garbage_class,
                        'parameters' => {
                                         'C'  => $C,
                                        },
                       );
        my $goc = $of->create_instance($C, \%of_attrs);

        DEBUG('doc', qq{ALG: apply Garbage_1 algorithm});
        $goc->remove_garbage($C, $text_ref);
    }
}

# ---------------------------------------------------------------------

=item PUBLIC CLASS METHOD: normalize_solr_date

From mysql we expect e.g. 1999-01-20.  The format Solr needs is of the
form 1995-12-31T23:59:59Z, and is a more restricted form of the
canonical representation of dateTime
http://www.w3.org/TR/xmlschema-2/#dateTime The trailing "Z" designates
UTC time and is mandatory.  Optional fractional seconds are allowed:
1995-12-31T23:59:59.999Z All other components are mandatory.

=cut

# ---------------------------------------------------------------------
sub normalize_solr_date {
    my $date_in = shift;
    return $date_in . 'T00:00:00Z';
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2007-11 ©, The Regents of The University of Michigan, All Rights Reserved

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
