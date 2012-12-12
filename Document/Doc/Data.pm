package Document::Doc::Data;


=head1 NAME

Document::Doc::Data

=head1 DESCRIPTION

This class is the base class of index/Document/Doc/Data/<<data types>>
index/Document.  It is an intermediate class to provide methods to
access metadata common to these classes.

It is empty currently just to maintain the class hierarchy.

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;
use warnings;

use SLIP_Utils::Common;
use Debug::DUtils;

# App
use base qw(Document::Doc);


# ---------------------------------------------------------------------

=item PUBLIC: clean_ocr

Description

=cut

# ---------------------------------------------------------------------
sub clean_ocr {
    my $self = shift;
    my $ocr_text_ref = shift;

    my $ck = Time::HiRes::time();
    SLIP_Utils::Common::clean_xml($ocr_text_ref);
    my $cke = Time::HiRes::time() - $ck;
    DEBUG('doc', qq{OCR: cleaned in sec=$cke});
}


1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2012 ©, The Regents of The University of Michigan, All Rights Reserved

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
