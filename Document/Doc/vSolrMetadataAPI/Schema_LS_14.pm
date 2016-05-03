package Document::Doc::vSolrMetadataAPI::Schema_LS_14;


=head1 NAME

Document::vSolrMetadataAPI::Schema_LS_14

=head1 DESCRIPTION

Adds the following:
language008_full  (language field from the 008 tranlated into English)
# hlb3Delimited allready in schema, just wasn't in VuFind




----
added  support for four new fields from the ht_id_display
dates from the enumcron if present:
   enumPubDate
   enumPubDateRange
dates containing either the date from the enumcron or if it is not there, the bib pubDate
   bothPubDate
   bothPubDateRange

Institution name for Original Location facet is derived from a new
mapping from collection code in ht_collections table into
ht_institutions. Requires ht_json field from VuFind in the metadata
query.

Type 11 schema

Emit new fields: vol_id, ht_reading_order, ht_scanning_order,
ht_cover_tag, seq and pgnum.  Support for nested and chunked Solr
documents.

Type 10 schema

Delete ht_heldby field.  Now obtained from PHDB not from VuFind
Solr. Add ht_heldby_brlm (held + brittle, lost, missing) obtained from
PHDB.

Wed Mar 13 12:11:20 2013 'title' no longer used as a proxy for
metadata validity. Previously, if absent we did not index the
item. Now we index the item using 245b,c.

Type 9 Schema

Adding "mainauthor" stored field.  This class creates an VuFind Solr
type 9 schema document for indexing using the VuFind API and the
VuFind Solr schema for facets and includes holdings data and
additional marc fields

Several fields that relate to rights are removed because they make
sense for a bib record but not for item records:

	"availability",
	"ht_availability",
	"ht_availability_intl",

Availability is useless.  It's to distinguish HT items and unscanned
items that are in Mirlyn.  All HT items have the same avail value
here.  See emails of 3/21/2012 from Bill Deuber The other two values
are based on the most liberal rights of any item on the bib and so are
totally inappropriate for item records.

htsource is not taken from the VuFind bib data instead we map the
namespace from the item id to the htsource display name.

Maps VuFind id to "record_no"

Maps the publishDate field to the stored date field for display

Processes the Vufind FullText field (MARC21 MARCXML for record) to
create the "allfields" field which concatentates all MARC fields over
99

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;

use JSON::XS;

# App
use Utils;
use Search::Constants;
use SharedQueue;
use CollectionCodes;
use Institutions;
use Access::Holdings;

# SLIP
use Db;
use base qw(Document::Doc::vSolrMetadataAPI);

# ------------------------  Field List  -------------------------------
#
# So far all are multi-valued (arr)
#

my @g_FIELD_LIST =
  (
   'author',
   'author2',
   'author_rest',
   'authorSort',
   'author_top',
   'callnumber',
   'callnosort',
   'countryOfPubStr',
   'ctrlnum',
   'date',
   'edition',
   'era',
   'format',
   'fullgenre',
   'fullgeographic',
   'fullrecord',
   'genre',
   'geographicStr',
   'hlb3Str',
   'hlb3Delimited',
   'ht_id',
   'ht_id_display',
   'ht_json',
   'id',
   'isbn',
   'isn_related',
   'issn',
   'language',
   'language008_full',
   'lccn',
   'lcshID',
   'mainauthor',
   'oclc',
   'publishDate',
   'publishDateRange',
   'publisher',
   'rptnum',
   'sdrnum',
   'serialTitle',
   'serialTitle_a',
   'serialTitle_ab',
   'serialTitle_rest',
   'series',
   'series2',
   'sudoc',
   'title',
   'title_a',
   'title_ab',
   'title_c',
   'title_rest',
   'titleSort',
   'title_top',
   'topicStr',
   'vtitle',
  );

# ---------------------------------------------------------------------

=item get_field_list

Description

=cut

# ---------------------------------------------------------------------
sub get_field_list {
    return \@g_FIELD_LIST;
}


# ---------------------------------------------------------------------

=item __add_large_coll_id_field

Get the list of coll_ids for the given id that are large so those
coll_ids can be added as <coll_id> fields of the Solr doc.

So, if sync-i found an id to have, erroneously, a *small* coll_id
field in its Solr doc and queued it for re-indexing, this routine
would create a Solr doc not containing that coll_id among its
<coll_id> fields.

=cut

# ---------------------------------------------------------------------
sub __add_large_coll_id_field {
    my $self = shift;
    my ($C, $dbh, $item_id, $primary_metadata_hashref) = @_;

    my $status = IX_NO_ERROR;

    my ($ok, $coll_id_arr_ref) = SharedQueue::get_coll_ids_for_id($C, $dbh, $item_id);
    if ($ok) {
        # If item is in one or more collections see if any of those collections are "large"
        my ($ok, $large_coll_id_arr_ref) = SharedQueue::get_large_coll_coll_ids($C, $dbh);
        if ($ok) {
            my $valid_coll_id_arr_ref = [];
            foreach my $coll_id (@$coll_id_arr_ref) {
                if (grep(/^$coll_id$/, @$large_coll_id_arr_ref)) {
                    push(@$valid_coll_id_arr_ref, $coll_id);
                }
            }

            if (scalar(@$valid_coll_id_arr_ref)) {
                $primary_metadata_hashref->{coll_id} = $valid_coll_id_arr_ref;
            }
            else {
                # O reserved for coll_id field of item not in any collection
                $primary_metadata_hashref->{coll_id} = [0];
            }
        }
        else {
            $status = IX_METADATA_FAILURE;
        }
    }
    else {
        $status = IX_METADATA_FAILURE;
    }

    return $status;
}

# ---------------------------------------------------------------------

=item __add_rights_field

<rights> attribute field

=cut

# ---------------------------------------------------------------------
sub __add_rights_field {
    my $self = shift;
    my ($C, $item_id, $primary_metadata_hashref) = @_;

    my $status = IX_NO_ERROR;

    my $rights_attribute = Document::Doc::get_rights_f_id($C, $item_id);
    if ($rights_attribute) {
        $primary_metadata_hashref->{rights} = [$rights_attribute];
    }
    else {
        $status = IX_METADATA_FAILURE;
    }

    return $status;
}

# ---------------------------------------------------------------------

=item __add_heldby_field

<rights> attribute field

=cut

# ---------------------------------------------------------------------
sub __add_heldby_field {
    my $self = shift;
    my ($C, $item_id, $primary_metadata_hashref) = @_;

    my $status = IX_NO_ERROR;

    my $holding_inst_arr_ref = Access::Holdings::holding_institutions($C, $item_id);
    if (scalar @$holding_inst_arr_ref) {
        $primary_metadata_hashref->{ht_heldby} = $holding_inst_arr_ref;
    }

    return $status;
}

# ---------------------------------------------------------------------

=item __add_heldby_brlm_field

<rights> attribute field

=cut

# ---------------------------------------------------------------------
sub __add_heldby_brlm_field {
    my $self = shift;
    my ($C, $item_id, $primary_metadata_hashref) = @_;

    my $status = IX_NO_ERROR;

    my $holding_inst_arr_ref = Access::Holdings::holding_BRLM_institutions($C, $item_id);
    if (scalar @$holding_inst_arr_ref) {
        $primary_metadata_hashref->{ht_heldby_brlm} = $holding_inst_arr_ref;
    }

    return $status;
}

# ---------------------------------------------------------------------

=item __add_page_features

Description

=cut

# ---------------------------------------------------------------------
sub __add_page_features {
    my $self = shift;
    my ($C, $item_id, $primary_metadata_hashref) = @_;

    my $status = IX_NO_ERROR;

    my $features_ref = $self->M_my_facade->D_get_doc_METS->page_features;
    $primary_metadata_hashref->{ht_page_feature} = $features_ref;

    return $status;
}

# ---------------------------------------------------------------------

=item __add_reading_order

Description

=cut

# ---------------------------------------------------------------------
sub __add_reading_order {
    my $self = shift;
    my ($C, $item_id, $primary_metadata_hashref) = @_;

    my $status = IX_NO_ERROR;

    my ($reading, $scanning, $cover_tag) = $self->M_my_facade->D_get_doc_METS->reading_orders;
    $primary_metadata_hashref->{ht_reading_order} = [ $reading ];
    $primary_metadata_hashref->{ht_scanning_order} = [ $scanning ];
    $primary_metadata_hashref->{ht_cover_tag} = [ $cover_tag ];

    return $status;
}

# ---------------------------------------------------------------------

=item get_auxiliary_field_data

Over-rides base class method, which see.

=cut

# ---------------------------------------------------------------------
sub get_auxiliary_field_data {
    my $self = shift;
    my ($C, $dbh, $item_id, $primary_metadata_hashref) = @_;

    my $status = IX_NO_ERROR;

    # Solr doc <coll_id> field(s)
    if ($status == IX_NO_ERROR) {
        $status = $self->__add_large_coll_id_field($C, $dbh, $item_id, $primary_metadata_hashref);
        $self->D_check_event($status, qq{error adding large collid field});
    }

    # Solr doc <rights> field
    if ($status == IX_NO_ERROR) {
        $status = $self->__add_rights_field($C, $item_id, $primary_metadata_hashref);
        $self->D_check_event($status, qq{error adding rights field});
    }

    # Solr doc <ht_heldby>
    if ($status == IX_NO_ERROR) {
        $status = $self->__add_heldby_field($C, $item_id, $primary_metadata_hashref);
        $self->D_check_event($status, qq{error adding ht_heldby field});
    }

    # Solr doc <ht_heldby_brlm> (brittle, lost, missing) field(s)
    if ($status == IX_NO_ERROR) {
        $status = $self->__add_heldby_brlm_field($C, $item_id, $primary_metadata_hashref);
        $self->D_check_event($status, qq{error adding ht_heldby_brlm field});
    }

    # Solr doc <ht_{reading,scanning}_order> (unknown, right-to-left, left-to-right) fields
    if ($status == IX_NO_ERROR) {
        $status = $self->__add_reading_order($C, $item_id, $primary_metadata_hashref);
        $self->D_check_event($status, qq{error adding ht_reading_order, ht_scanning_order fields});
    }

    # Solr doc <ht_page_feature> (e.g. IMAGE_ON_PAGE, TABLE_OF_CONTENTS) field(s)
    if ($status == IX_NO_ERROR) {
        $status = $self->__add_page_features($C, $item_id, $primary_metadata_hashref);
        $self->D_check_event($status, qq{error adding ht_page_feature fields});
    }

    return ($primary_metadata_hashref, $status);
}


# ---------------------------------------------------------------------

=item PUBLIC: post_process_metadata

Description: Massage field values that come back from VuFind specific
to the schema in question for this subclass.

This mapping adheres to the LS Schema above.

=cut

# ---------------------------------------------------------------------
sub post_process_metadata {
    my $self = shift;
    my ($C, $item_id, $metadata_hashref) = @_;

    # Get MARC XML and concatenate text contents of all fields > 99
    # i.e. no control 0xx fields!
    unless ( exists $metadata_hashref->{allfields} ) {
        $metadata_hashref->{allfields} = $self->get_all_fields($metadata_hashref->{fullrecord});
        delete $metadata_hashref->{fullrecord};
    }

    # VuFind id becomes bib_id for PIFiller/ListSearchResults uses.
    unless ( exists $metadata_hashref->{record_no} ) {
        $metadata_hashref->{record_no} = $metadata_hashref->{id};
        delete $metadata_hashref->{id};
    }

    # Save title as Vtitle before Mbooks specific processing reserved
    # for "title" field
    $metadata_hashref->{Vtitle} = $metadata_hashref->{title};

    # Save author to Vauthor for vufind processed field
    if (defined($metadata_hashref->{author})) {
        $metadata_hashref->{Vauthor} = $metadata_hashref->{author}
    }

    my @hathiTrust_str = grep(/^\Q$item_id\E\|.*/, @{$metadata_hashref->{ht_id_display}});
    # 0      1            2           3           4
    # htid | ingestDate | enumcron |enumPublishDate|enumPublishDateRange

    my @ht_id_display = split(/\|/, $hathiTrust_str[0]);

    # Store enumcron as separate Solr field and concatenate at a later
    # stage prior to display.  See emails on October 18th, 2011 re:
    # 245$c and wierd punctuation when there is an enumcron.
    my $volume_enumcron = $ht_id_display[2];
    if ($volume_enumcron) {
        $metadata_hashref->{volume_enumcron} = [$volume_enumcron];
    }

    # Add 4 fields to the schema so we can keep these separate and
    # then have two combined fields that will contain either the item
    # data or the bib data while the other set will be empty if no
    # item data

    # Add enumPublishDate and enumPublishDateRange. These will be
    # empty if not populated in the ht_id_display.  Add also a
    # bothPublishDate and bothPublishDateRange. These will contain
    # either the enum values if they exist for this item from the
    # ht_id_display or if not in ht_id_display will contain the values
    # from the regular bib publishDate and publishDateRange.

    my $enum_date = $ht_id_display[3];

    if (defined $enum_date) {
	if ( is_number($enum_date) ) {
	    $metadata_hashref->{enumPublishDate} = [$enum_date];
	    $metadata_hashref->{bothPublishDate} = [$enum_date];
	}
    }
    elsif (defined $metadata_hashref->{publishDate}) {
	# stick regular pub date in a separate field if we couldn't
	# find one in the enum
        $metadata_hashref->{bothPublishDate} = $metadata_hashref->{publishDate};
    }

    my $enum_range = $ht_id_display[4];

    if ( defined($enum_range) && $enum_range =~ /\d+\-*\d*/ ) {
        $metadata_hashref->{enumPublishDateRange} = [$enum_range];
        $metadata_hashref->{bothPublishDateRange} = [$enum_range];
    }
    elsif ( defined($metadata_hashref->{publishDateRange}) && $metadata_hashref->{publishDateRange} =~ /\d+\-*\d*/ ) {
        $metadata_hashref->{bothPublishDateRange}= $metadata_hashref->{publishDateRange};
    }
    delete $metadata_hashref->{ht_id_display};

    # copy publishDate into date field
    if (defined $metadata_hashref->{publishDate}) {
        $metadata_hashref->{date}[0] = $metadata_hashref->{publishDate}[0];
    }

    # "Original Location" facet
    my $htsource_display_name = get_htsource_display_name($C, $item_id, $metadata_hashref);
    if (defined $htsource_display_name) {
        $metadata_hashref->{htsource} = [$htsource_display_name];
    }
    delete $metadata_hashref->{ht_id};
    delete $metadata_hashref->{ht_json};
}

# ---------------------------------------------------------------------

=item get_htsource_display_name

  Derive htsource per item by this mapping

  VUFIND.ht_json.collection_code -->
    ht_collections.collection(code) -->
      ht_collections.original_from_inst_id -->
        ht_institutions.inst_id -->
          ht_institutions.name

  i.e. collection code=MIU -->
         original_from_inst_id=umich -->
           ht_institutions.inst_id=umich -->
             ht_institutions.name=University of Michigan

=cut

# ---------------------------------------------------------------------
sub get_htsource_display_name {
    my ($C, $item_id, $metadata_hashref) = @_;

    my $display_name = 'Unknown';
    my $json_data = $metadata_hashref->{ht_json}[0];

    my $decoder = JSON::XS->new->ascii;
    my $ref_to_arr_of_hashref = $decoder->decode($json_data);

    my $collection_code = '';
    foreach my $hashref (@$ref_to_arr_of_hashref) {
        if ($hashref->{htid} eq $item_id) {
            # collection codes are uppercase in ht_collections.collection
            $collection_code = uc $hashref->{collection_code};
            last;
        }
    }

    if ($collection_code) {
        my $inst_id = CollectionCodes::get_inst_id_by_collection_code($C, $collection_code);
        $display_name = Institutions::get_institution_inst_id_field_val($C, $inst_id, 'name', 'mapped');
    }

    return $display_name;
}

# ---------------------------------------------------------------------

=item is_number

Description

=cut

# ---------------------------------------------------------------------
sub is_number {
    my $n = shift;
    return ($n =~ /^\d+$/);
}

# ---------------------------------------------------------------------

=item get_all_fields

Input is the VuFind "FullText" field which is the MARC21 MARCXML for
the record, output is concatenation of all the MARC fields above 99.
Note that an earlier process escaped the xml so we reverse the process

=cut

# ---------------------------------------------------------------------
sub get_all_fields {
    my $self = shift;
    my $xml_ref = shift;

    my $xml = $xml_ref->[0];

    # clean up escaped xml until we find out where its escaped
    $xml =~ s/\&lt\;/\</g;
    $xml =~ s/\&gt\;/\>/g;

    my $doc = $self->M_parser->parse_string($xml);
    my @nodelist = $doc->getElementsByTagName('datafield');
    my $bigstring;
    my $content;

    foreach my $node (@nodelist) {
        my $tag = $node->getAttribute('tag');

        if ($tag > 99) {
            if ($node->hasChildNodes()) {
                my @childnodes = $node->childNodes();
                foreach my $child (@childnodes) {
                    $content = $child->textContent;
                    $bigstring .= $content . " ";
                }
            }
            else {
                $content = $node->textContent;
                $bigstring .= $content . " ";
            }
        }
    }

    my $aryref = [];
    $aryref->[0] = $bigstring;

    return $aryref;
}

1;
