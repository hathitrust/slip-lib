package Document::METS;


=head1 NAME

Document::METS (mo)

=head1 DESCRIPTION

This class provides a lightweight METS parsing service to return
various bit from a METS object fileSec/fileGrp | structMap.

=head1 SYNOPSIS

my $mets = new Document::METS($item_id, $USE_attr_arr_ref);

=head1 METHODS

=over 8

=cut

use strict;
use warnings;

use XML::LibXML;

use Context;
use Utils;
use Identifier;
use DataTypes;
use MetsReadingOrder;


my %selector =
  (
   volume =>
   {
    volume => {
               _structmap => {
                              xpath   => q{/METS:mets/METS:structMap[@TYPE='physical']/METS:div[@TYPE='volume']/METS:div[@ORDER]},
                              ordered => 1,
                             },
               _filegrp   => {
                              mimetypes => [ 'text/plain' ],
                             },
              },
    TEI    => {
               _structmap => {
                              xpath   => q{/METS:mets/METS:structMap[@TYPE='physical']/METS:div[@TYPE='volume']/METS:div[@ORDER]},
                              ordered => 1,
                             },
               _filegrp   => {
                              mimetypes => [ 'text/plain' ],
                             },
              },
   },
   audio =>
   {
    audio  => {
               _structmap => {
                              xpath   => q{/METS:mets/METS:structMap[@TYPE='physical']/METS:div/METS:div[@TYPE='container']},
                              ordered => 0,
                             },
               _filegrp   => {
                              mimetypes => [ 'text/plain' ],
                             },
              },
   },
   article =>
   {
    JATS   => {
               _structmap => {
                              xpath   => q{/METS:mets/METS:structMap[@TYPE='physical']/METS:div[@TYPE='contents']/METS:div[@TYPE='article']/METS:div[@TYPE='primary']},
                              ordered => 0,
                             },
               _filegrp   => {
                              mimetypes => [ 'text/xml' ],
                             },
              },
   },
  );



sub new {
    my $class = shift;

    my $self = {};
    bless $self, $class;
    $self->__initialize(@_);

    return $self;
}

# ---------------------------------------------------------------------

=item __initialize

Description

=cut

# ---------------------------------------------------------------------
sub __initialize {
    my $self = shift;
    my ($id, $USE_conf) = @_;

    my $root = $self->METS_root($id);

    my $type = DataTypes::getDataType($root) || '__unknown';
    my $subtype = DataTypes::getDataSubType($root) || '__unknown';
    my $USE_attr_arr_ref = $USE_conf->{$type}{$subtype} || [];

    $self->USEs($USE_attr_arr_ref);
    $self->__zero_USE_member_data;

    $self->__mets_map_init($type, $subtype);

    eval {
        $self->__build_METS_dataset($root);
    };
    if ($@) {
        ASSERT(0,qq{error in __initialize: $@} );
    }
}


# ---------------------------------------------------------------------
#
#                                Public
#
# ---------------------------------------------------------------------


# ---------------------------------------------------------------------

=item PUBLIC: METS_root

Ensure root or throw

=cut

# ---------------------------------------------------------------------
sub METS_root {
    my $self = shift;
    my $id = shift;

    return $self->{_METS_ROOT} if (exists $self->{_METS_ROOT});

    my $itemFileSystemLocation = Identifier::get_item_location($id);
    my $stripped_id = Identifier::get_pairtree_id_wo_namespace($id);

    my $mets_xml_filename = $itemFileSystemLocation . qq{/$stripped_id} . '.mets.xml';

    # Not all IDs have data in the repository
    my $mets_xml_ref = Utils::read_file($mets_xml_filename, 'optional');
    ASSERT($$mets_xml_ref, qq{cannot read METS file="$mets_xml_filename"});

    my $METS_ROOT;
    eval {
        my $parser = XML::LibXML->new();

        my $tree = $parser->parse_string($$mets_xml_ref);
        die qq{failure to parse METS file="$mets_xml_filename"} unless (defined $tree);

        $METS_ROOT = $tree->getDocumentElement();
        die qq{getDocumentElement failure parsing METS file="$mets_xml_filename"} unless (defined $METS_ROOT);
    };
    if ($@) {
        ASSERT(0,qq{error in METS_root method: $@} );
    }

    return $self->{_METS_ROOT} = $METS_ROOT;
}

# ---------------------------------------------------------------------

=item dataset_is_valid

Description

=cut

# ---------------------------------------------------------------------
sub dataset_is_valid {
    my $self = shift;
    return $self->{dataset}{is_valid};
}

# ---------------------------------------------------------------------

=item seq2pgnum_map

Description

=cut

# ---------------------------------------------------------------------
sub seq2pgnum_map {
    my $self = shift;

    ASSERT(0, qq{Invalid METS dataset}) unless ($self->dataset_is_valid);
    my $map = $self->{dataset}{METS_maps}->{seq2pgnum};
    return $map;
}


# ---------------------------------------------------------------------

=item page_features

Description

=cut

# ---------------------------------------------------------------------
sub page_features {
    my $self = shift;

    ASSERT(0, qq{Invalid METS dataset}) unless ($self->dataset_is_valid);
    my $ref = $self->{dataset}{METS_maps}->{features};
    my $features_ref = [];
    push( @$features_ref, keys %$ref );

    return $features_ref;
}

# ---------------------------------------------------------------------

=item reading_orders

Description

=cut

# ---------------------------------------------------------------------
sub reading_orders {
    my $self = shift;

    ASSERT(0, qq{Invalid METS dataset}) unless ($self->dataset_is_valid);
    my $reading = $self->{dataset}{METS_maps}->{reading_orders}->{read};
    my $scanning = $self->{dataset}{METS_maps}->{reading_orders}->{scan};
    my $cover = $self->{dataset}{METS_maps}->{reading_orders}->{cover};

    return ($reading, $scanning, $cover);
}


# ---------------------------------------------------------------------

=item filelist

Description

=cut

# ---------------------------------------------------------------------
sub filelist {
    my $self = shift;

    ASSERT(0, qq{Invalid METS dataset}) unless ($self->dataset_is_valid);
    my $files_arr_ref = $self->{dataset}{METS_filelist} || [];
    return $files_arr_ref;
}

# ---------------------------------------------------------------------

=item has_files

Description

=cut

# ---------------------------------------------------------------------
sub has_files {
    my $self = shift;

    ASSERT(0, qq{Invalid METS dataset}) unless ($self->dataset_is_valid);
    my $has_files =  $self->{dataset}{METS_has_files};
    return $has_files;
}

# ---------------------------------------------------------------------

=item num_files

Description

=cut

# ---------------------------------------------------------------------
sub num_files {
    my $self = shift;

    ASSERT(0, qq{Invalid METS dataset}) unless ($self->dataset_is_valid);
    my $num_files =  $self->{dataset}{METS_has_files};
    return $num_files;
}


# ---------------------------------------------------------------------
#
#                                Private
#
# ---------------------------------------------------------------------

# ---------------------------------------------------------------------

=item METS map accessors

Description

=cut

# ---------------------------------------------------------------------
sub __structmap_xpath {
    my $self = shift;
    return $self->{_structmap_xpath};
}

sub __structmap_is_ordered {
    my $self = shift;
    return $self->{_structmap_is_ordered};
}

sub __filegrp_mimetypes {
    my $self = shift;
    return $self->{_filegrp_mimetypes};
}

sub __mets_map_init {
    my $self = shift;
    my ($type, $subtype) = @_;

    $self->{_structmap_xpath} = $selector{$type}->{$subtype}->{_structmap}->{xpath};
    $self->{_structmap_is_ordered} = $selector{$type}->{$subtype}->{_structmap}->{ordered};
    $self->{_filegrp_mimetypes} = $selector{$type}->{$subtype}->{_filegrp}->{mimetypes};
}

# ---------------------------------------------------------------------

=item USEs

Description

=cut

# ---------------------------------------------------------------------
sub USEs {
    my $self = shift;
    my $USE_attr_arr_ref = shift;

    if (defined $USE_attr_arr_ref) {
        $self->{_uses} = $USE_attr_arr_ref;
    }

    return @{ $self->{_uses} };
}

# ---------------------------------------------------------------------

=item __{set,zero}_USE_member_data

Description

=cut

# ---------------------------------------------------------------------
sub __set_USE_member_data {
    my $self = shift;
    my ($is_valid, $file_grp_hashref, $files, $maps) = @_;

    $self->{dataset}{is_valid} = $is_valid;
    $self->{dataset}{METS_filelist} = $files;
    $self->{dataset}{METS_maps} = $maps;
    $self->{dataset}{METS_has_files} = $file_grp_hashref->{summary}{has_files};

    # structMap may lack fptr for fileGrp fileid.
    unless (scalar @$files) {
        $self->{dataset}{METS_has_files} = 0;
    }
}

sub __zero_USE_member_data {
    my $self = shift;

    $self->{dataset}{is_valid} = 0;
    $self->{dataset}{METS_filelist} = [];
    $self->{dataset}{METS_has_files} = 0;
    $self->{dataset}{METS_maps} = $self->__get_zeroed_map;
}

# ---------------------------------------------------------------------

=item __get_zeroed_map

Description

=cut

# ---------------------------------------------------------------------
sub __get_zeroed_map {
    my $self = shift;

    return {
            seq2pgnum => {},
            features => {},
            reading_orders => {
                   read => 'unknown',
                   scan => 'unknown',
                  },
           };
}

# ---------------------------------------------------------------------

=item __set_reading_orders

Description

=cut

# ---------------------------------------------------------------------
sub __set_reading_orders {
    my $self = shift;
    my $map = shift;

    my ($read, $scan, $cover) = MetsReadingOrder::parse( $self->METS_root );
    $map->{reading_orders}->{read} = $read;
    $map->{reading_orders}->{scan} = $scan;
    $map->{reading_orders}->{cover} = $cover;
}

# ---------------------------------------------------------------------

=item __process_unordered_structMap

Merge filenames ordered by the structMap over requested USEs and build
a sequence to page number map

=cut

# ---------------------------------------------------------------------
sub __process_ordered_structMap {
    my $self = shift;
    my $root = shift;
    my $file_grp_hashref = shift;

    my $files_arr_ref = [];
    my $maps = $self->__get_zeroed_map;

    return ($files_arr_ref, $maps)
      unless( $file_grp_hashref->{summary}{has_files} );

    my %METS_hash = ();
    my $seq2pgnum_hashref = {};

    my @structMap_divs = $root->findnodes( $self->__structmap_xpath );

    foreach my $ORDER_div (@structMap_divs) {
        my $order = $ORDER_div->getAttribute('ORDER');
        my $pgnum = $ORDER_div->getAttribute('ORDERLABEL');
        my $features = $ORDER_div->getAttribute('LABEL');

        my @metsFptrs = $ORDER_div->findnodes('METS:fptr[@FILEID]');

        foreach my $fptr (@metsFptrs) {
            my $fileid = $fptr->getAttribute('FILEID');

            if (exists $file_grp_hashref->{filelist}{$fileid}) {
                $METS_hash{$order} = {
                                      filename => $file_grp_hashref->{filelist}{$fileid},
                                      pgnum    => $pgnum,
                                      features => ($features || ''),
                                     };
                last;
            }
        }
    }

    foreach my $ord ( sort {$a <=> $b} keys %METS_hash ) {
        push(@$files_arr_ref, $METS_hash{$ord}->{filename});
        $maps->{seq2pgnum}->{$ord} = $METS_hash{$ord}->{pgnum};

        my @page_features = split( /,\s*/, $METS_hash{$ord}->{features} );
        foreach my $feature (@page_features) {
            $maps->{features}->{$feature} = 1;
        }
    }

    return ($files_arr_ref, $maps);
}

# ---------------------------------------------------------------------

=item __process_unordered_structMap

Collect filenames over requested USEs

=cut

# ---------------------------------------------------------------------
sub __process_unordered_structMap {
    my $self = shift;
    my $root = shift;
    my $file_grp_hashref = shift;

    my @structMap_divs = $root->findnodes( $self->__structmap_xpath );

    my $files_arr_ref = [];
    my $maps = $self->__get_zeroed_map;

    foreach my $div (@structMap_divs) {

        my @metsFptrs = $div->findnodes('METS:fptr[@FILEID]');

        foreach my $fptr (@metsFptrs) {
            my $fileid = $fptr->getAttribute('FILEID');

            if (exists $file_grp_hashref->{filelist}{$fileid}) {
                push(@$files_arr_ref, $file_grp_hashref->{filelist}{$fileid});
                last;
            }
        }
    }

    return ($files_arr_ref, $maps);
}


# ---------------------------------------------------------------------

=item __build_METS_dataset

Capture all the files for indexing. Order them by structMap[@ORDER] if
available otherwise use the order they occur in the fileGrp by USE.

Currently, structMap[@ORDER] exists only fileids with
fileGrp[@USE='ocr']. Build the maps from that USE case.

=cut

# ---------------------------------------------------------------------
sub __build_METS_dataset {
    my $self = shift;
    my $root = shift;

    my $file_grp_hashref = $self->__parse_fileGrp($root);

    my ($files, $maps);
    if ($self->__structmap_is_ordered) {
        ($files, $maps) = $self->__process_ordered_structMap($root, $file_grp_hashref);
    }
    else {
        ($files, $maps) = $self->__process_unordered_structMap($root, $file_grp_hashref);
    }
    $self->__set_reading_orders($maps);
    $self->__set_USE_member_data(1, $file_grp_hashref, $files, $maps);
}


# ---------------------------------------------------------------------

=item __parse_fileGrp

Summarize file data over all requested USEs

=cut

# ---------------------------------------------------------------------
sub __parse_fileGrp {
    my $self = shift;
    my $root = shift;

    my $file_grp_hashref = {};

    my $total_non_zero_file_size = 0;
    my $has_files = 0;

    foreach my $USE ($self->USEs) {
        my $xpath = q{/METS:mets/METS:fileSec/METS:fileGrp[@USE='} . $USE. q{']/METS:file};
        my $_file_grp = $root->findnodes($xpath);
        my $has = scalar @$_file_grp;

        if ($has) {
            # Test for all zero-length files.
            foreach my $node ($_file_grp->get_nodelist) {
                my $fileid = $node->getAttribute('ID');
                my $filesize = $node->getAttribute('SIZE');
                my $mimetype = $node->getAttribute('MIMETYPE');

                foreach my $type (@{ $self->__filegrp_mimetypes }) {
                    if ($mimetype eq $type) {
                        my $filename = ($node->childNodes)[1]->getAttribute('xlink:href');
                        $file_grp_hashref->{filelist}{$fileid} = $filename;
                        $total_non_zero_file_size += $filesize;
                    }
                    else {
                        $has--;
                    }
                }
            }
        }
        $has_files += $has;
    }

    $has_files = 0 if ($total_non_zero_file_size == 0);

    $file_grp_hashref->{summary}{has_files} = $has_files;

    return $file_grp_hashref
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2011-2014 ©, The Regents of The University of Michigan, All Rights Reserved

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
