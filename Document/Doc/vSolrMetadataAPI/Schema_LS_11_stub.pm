package Document::Doc::vSolrMetadataAPI::Schema_LS_11_stub;

=head1 NAME

Document::vSolrMetadataAPI::Schema_LS_11

=head1 DESCRIPTION

Type 11 schema stub for mdp.99999999999999


=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;


# App
use Utils;
use Search::Constants;
use SharedQueue;
use Namespaces;
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
	"author",
	"author2",
	"author_rest",
	"authorSort",
	"author_top",
        "callnumber",
        "countryOfPubStr",
	"ctrlnum",
	"date",
        "edition",
	"era",
	"format",
	"fullgenre",
	"fullgeographic",
	"fullrecord",
	"genre",
       	"geographicStr",
	"hlb3Str",
	"hlb3Delimited",
	"ht_id_display",
	"id",
	"isbn",
	"isn_related",
	"issn",
	"language",
	"lccn",
        "lcshID",
        "mainauthor",
	"oclc",
	"publishDate",
	"publishDateRange",
	"publisher",
	"rptnum",
	"sdrnum",
	"serialTitle",
	"serialTitle_a",
	"serialTitle_ab",
	"serialTitle_rest",
	"series",
	"series2",
	"sudoc",
	"title",
	"title_a",
	"title_ab",
	"title_c",
	"title_rest",
	"titleSort",
	"title_top",
        "topicStr",
	"vtitle",
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
    $primary_metadata_hashref->{coll_id} = [0];

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
    $primary_metadata_hashref->{rights} = [1];

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
    $primary_metadata_hashref->{ht_heldby} = [ 'uom' ];

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
    $primary_metadata_hashref->{ht_heldby} = [ 'uom' ];

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
    $primary_metadata_hashref->{ht_page_feature} = [ 'TITLE' ];

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

    $primary_metadata_hashref->{ht_reading_order} = [ 'unknown' ];
    $primary_metadata_hashref->{ht_scanning_order} = [ 'unknown' ];
    $primary_metadata_hashref->{ht_cover_tag} = [ 'unknown' ];

    return $status;
}

# ---------------------------------------------------------------------

=item get_auxiliary_field_data

Over-rides base class method, which see.

=cut

# ---------------------------------------------------------------------
sub get_auxiliary_field_data {
    my $self = shift;
    my ($C, $dbh, $item_id, $primary_metadata_hashref, $state, $cached) = @_;

    my $status = IX_NO_ERROR;

    return ($primary_metadata_hashref, $status) if ($cached);

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

=item PRIVATE: __get_metadata_from_vufind_f_item_id

Description

=cut

# ---------------------------------------------------------------------
sub __get_metadata_from_vufind_f_item_id {
    my $self = shift;
    my ($C, $dbh, $item_id, $field_list_arr_ref) = @_;

    my $status = IX_NO_ERROR;
    
    use Document::Doc::vSolrMetadataAPI::11_stub;
    my $response = $Document::Doc::vSolrMetadataAPI::11_stub::response;

    return (\$response, $status);
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
    my ($C, $item_id, $metadata_hashref, $state) = @_;

    # Get MARC XML and concatenate text contents of all fields > 99
    # i.e. no control 0xx fields!
    unless ( exists $metadata_hashref->{allfields} ) {
        $metadata_hashref->{allfields} = $self->get_all_fields($metadata_hashref->{fullrecord});
        delete $metadata_hashref->{fullrecord};
    }

    # VuFind id becomes bib_id for PIFiller/ListSearchResults uses.
    unless ( exists $metadata_hashref->{'record_no'} ) {
        $metadata_hashref->{'record_no'} = $metadata_hashref->{'id'};
        delete $metadata_hashref->{'id'};
    }

    # Save title as Vtitle before Mbooks specific processing reserved
    # for "title" field
    $metadata_hashref->{'Vtitle'} = $metadata_hashref->{'title'};

    # Save author to Vauthor for vufind processed field
    if (defined($metadata_hashref->{'author'})) {
        $metadata_hashref->{'Vauthor'} = $metadata_hashref->{'author'}
    }

    my @hathiTrust_str = grep(/^$item_id\|.*/, @{$metadata_hashref->{'ht_id_display'}});
    # 0      1            2          3
    # htid | ingestDate | enumcron | rightsCodeForThisItem
    my @ht_id_display = split(/\|/, $hathiTrust_str[0]);

    # Store enumcron as separate Solr field and concatenate at a later
    # stage prior to display.  See emails on October 18th, 2011 re:
    # 245$c and wierd punctuation when there is an enumcron.
    my $volume_enumcron = $ht_id_display[2];
    if ($volume_enumcron) {
        $metadata_hashref->{'volume_enumcron'} = [$volume_enumcron];
    }
    delete $metadata_hashref->{'ht_id_display'};

    # copy publishDate into date field
    if (defined($metadata_hashref->{'publishDate'})) {
        $metadata_hashref->{'date'}[0] = $metadata_hashref->{'publishDate'}[0];
    }

    # Derive htsource per item by mapping namespace to institution
    # name, i.e.  if namespace=mdp htsource="University of Michigan"
    my $htsource_display_name = Namespaces::get_institution_by_namespace($C, $item_id);
    if (defined($htsource_display_name)) {
        $metadata_hashref->{htsource} = [$htsource_display_name];
    }
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
