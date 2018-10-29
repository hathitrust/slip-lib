package Search::Searcher::ExportSearcher;

=head1 NAME

Search::Searcher::ExportSearcher

=head1 DESCRIPTION

Uses the export handler and docvalues to provide safe querying for large result sets
WARNING: currently configured to be called on one shard at a time
Caller is responsible for looping through shards and consolidating results
XXX Consider adding aggretation function here for apps other than sync-i

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;

#XXX fixme
use base qw(Search::Searcher);

use strict;
use warnings;

use Encode;
use LWP::UserAgent;
use List::Util qw( first );


#use App;
use Context;
use Utils;
use Utils::Time;
use Utils::Logger;
use Debug::DUtils;
use Search::Query;
use Search::Result;


# ---------------------------------------------------------------------

=item get_Solr_raw_internal_query_result

Overide base class so we can use __Solr_export_result instead of __Solr_result

=cut

# ---------------------------------------------------------------------
sub get_Solr_raw_internal_query_result {
    my $self = shift;
    my ($C, $query_string, $rs) = @_;

    return $self->__Solr_export_result($C, $query_string, $rs);
}


# ---------------------------------------------------------------------
# HACK XXX overide Search::Searcher::__Solr_result so we can return an export url
# instead of a select url
# consider changing bas class to take argument export/select
#XXX Consider where and how to do 1 shard at a time vs all shards!!

# ---------------------------------------------------------------------
sub __Solr_export_result {
    my $self = shift;
    my ($C, $query_string, $rs) = @_;

    my $url = $self->__get_Solr_export_url($C, $query_string);
    my $req = $self->__get_request_object($url);
    my $ua = $self->__create_user_agent();

    if (DEBUG('query')) {
        my $d = $url;
        Utils::map_chars_to_cers(\$d, [q{"}, q{'}]) if Debug::DUtils::under_server();;
        DEBUG('query', qq{Query URL: $d});
    }
    my ($code, $response, $status_line, $failed_HTTP_dump) = $self->__get_query_response($C, $ua, $req);

    $rs->ingest_Solr_search_response($code, \$response, $status_line, $failed_HTTP_dump);

    return $rs;
}

#
#XXX  consider modifying base class
# to take an argument (export|select)

# ---------------------------------------------------------------------
sub __get_Solr_export_url {
    my $self = shift;
    my ($C, $query_string) = @_;

    my $primary_engine_uri = $self->get_engine_uri();
    
    # XXX why is the string "select" a config param?
    # is this used anywhere else?

    #my $script = $C->get_object('MdpConfig')->get('solr_select_script');

    # XXX we are doing this on a per shard basis so don't need the shards param!
    
    # my $url = 
    #     $primary_engine_uri 
    #         . $script 
    #             . '?' 
    #               . (defined($shards_param) ? "${shards_param}&" : '')
    #                 . $query_string;

    my $url = $primary_engine_uri . '/export?' .$query_string;
    
    return $url;
}



# ---------------------------------------------------------------------


1;

__END__

=head1 AUTHOR

Tom Burton-West,University of Michigan, tburtonw@umich.edu

=head1 COPYRIGHT

Copyright 2018 ©, The Regents of The University of Michigan, All Rights Reserved

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
