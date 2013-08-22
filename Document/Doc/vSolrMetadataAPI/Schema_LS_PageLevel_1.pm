package Document::Doc::vSolrMetadataAPI::Schema_LS_PageLevel_1;

=head1 NAME

Document::Doc::vSolrMetadataAPI::Schema_LS_PageLevel_1

=head1 DESCRIPTION

This class creates Page Level  Solr document metadata.
Currently using all MARC fields used in LSS volume level search
Should we also include heldby_brlm/heldby which we use for rights stuff?
(See subs in Schema_LS_10)
Skip for now because it could make indexing slower, but note that if we did page-level in 
production we would need to do it for rights

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;

# App
use Utils;
use Search::Constants;
#use SharedQueue;
#use Namespaces;
#use Access::Holdings;


# SLIP
use Db;
use base qw(Document::Doc::vSolrMetadataAPI);

# ------------------------  Field List  -------------------------------
#
# So far all are multi-valued (arr)
#
#   MARC fields for testing faceting performance
#   Should we also include fields used for ranking on MARC data?

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

=item get_auxiliary_field_data

Over-rides base class method, which see.

=cut

# ---------------------------------------------------------------------
sub get_auxiliary_field_data {
    my $self = shift;
    my ($C, $dbh, $item_id, $primary_metadata_hashref, $state, $cached) = @_;

    my $status = IX_NO_ERROR;
    return ($primary_metadata_hashref, $status)
      if ($cached);

    # Aux metadata: Volume id + rights fields
    $primary_metadata_hashref->{vol_id} = [$item_id];

    my $rights_attribute = Document::get_rights_f_id($C, $item_id);
    if ($rights_attribute) {
        $primary_metadata_hashref->{rights} = [$rights_attribute];
    }
    else {
        $status = IX_METADATA_FAILURE;
    }

    return ($primary_metadata_hashref, $status);
}


# ---------------------------------------------------------------------

=item PUBLIC: post_process_metadata

Description: Massage field values that come back from VuFind specific
to the schema in question for this subclass.

This mapping adheres to the Schema above.

=cut

# ---------------------------------------------------------------------
sub post_process_metadata {
    my $self = shift;
    my ($C, $item_id, $metadata_hashref, $state, $cached) = @_;

    # The vufind id is called record_no in our code and our id for a
    # page-level document for one page of an item is the volume id
    # e.g. mdp.39015015823563 concatenated with the state
    # variable. This is called 'hid' and is <uniqueKey>hid</uniqueKey>
    # in schema.xml. Everything else will have been cached.
    #
    if (defined($metadata_hashref->{'id'})) {
        $metadata_hashref->{'record_no'} = $metadata_hashref->{'id'};
        delete $metadata_hashref->{'id'};
    }
    $metadata_hashref->{hid} = [$item_id . qq{_$state}];

    # Nothing else to do after the first call.
    return if ($cached);


    $metadata_hashref->{'allfields'} = getAllFields($metadata_hashref->{'fullrecord'});
    delete $metadata_hashref->{'fullrecord'};



    # Title is used as a proxy for metadata validity
    my @titles = @{$metadata_hashref->{'title'}};
    return unless (scalar(@titles) > 0);

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

    delete $metadata_hashref->{'ht_id_display'};
    # copy publishDate into date field
    if (defined($metadata_hashref->{'publishDate'})) {
        $metadata_hashref->{'date'}[0] = $metadata_hashref->{'publishDate'}[0];
        delete $metadata_hashref->{'publishDate'}
    }
}
#----------------------------------------------------------------------

# ---------------------------------------------------------------------

=item getAllFields

Input is the VuFind "FullText" field which is the MARC21 MARCXML for
the record, output is concatenation of all the MARC fields above 99.
Note that an earlier process escaped the xml so we reverse the process

=cut

# ---------------------------------------------------------------------
sub getAllFields {
    my $xmlref = shift;
    my $xml = $xmlref->[0];

    # clean up escaped xml until we find out where its escaped
    $xml =~s/\&lt\;/\</g;
    $xml =~s/\&gt\;/\>/g;

    my $g_PARSER = XML::LibXML->new();
    my $doc = $g_PARSER->parse_string($xml);
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
