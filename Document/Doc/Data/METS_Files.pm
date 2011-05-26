package Document::Doc::Data::METS_Files;


=head1 NAME

Document::Data::METS_Files

=head1 DESCRIPTION

This class provides a lightweight METS parsing service to return the
list of text files from a METS object.  Presumably we will be able to
retrieve the contents of these files from the zip archive.

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;

use XML::LibXML;

use MdpGlobals;
use Utils;
use Identifier;

# ---------------------------------------------------------------------

=item get_filelist_for

Description

=cut

# ---------------------------------------------------------------------
sub get_filelist_for {
    my $id = shift;
    my $use_attribute = shift;

    my $mets_xml_filename = get_METS_xml_filename($id);
    my $mets_xml_ref = Utils::read_file($mets_xml_filename);

    my $parser = XML::LibXML->new();
    my $tree = $parser->parse_string($$mets_xml_ref);
    my $root = $tree->getDocumentElement();

    my %file_grp_hash = ();

    # OCR fileGrp - some objects lack this group
    my $xpath = qq{/METS:mets/METS:fileSec/METS:fileGrp[@USE='$use_attribute']/METS:file};
    my $text_file_grp = $root->findnodes($xpath);
    my $has_files = scalar(@$text_file_grp);

    if ($has_files) {
        # Test for all zero-length files.
        my $total_file_size = 0;
        foreach my $node ($text_file_grp->get_nodelist) {
            my $seq = $node->getAttribute('SEQ');
            $seq =~ s,^0*,,go;
            my $filesize = $node->getAttribute('SIZE');
            my $filename = ($node->childNodes)[1]->getAttribute('xlink:href');
            $file_grp_hash->{$seq}{filename} = $filename;
            $total_file_size += $filesize;
        }
        $has_files = 0 if ($total_file_size == 0);
    }

    return (\%file_grp_hash, $has_files);
}

# ---------------------------------------------------------------------

=item get_METS_xml_filename

Description

=cut

# ---------------------------------------------------------------------
sub get_METS_xml_filename {
    my $id = shift;

    my $itemFileSystemLocation = Identifier::get_item_location($id);
    my $stripped_id = Identifier::get_pairtree_id_wo_namespace($id);

    return $itemFileSystemLocation . qq{/$stripped_id} . $MdpGlobals::gMetsFileExtension;
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2011 ©, The Regents of The University of Michigan, All Rights Reserved

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
