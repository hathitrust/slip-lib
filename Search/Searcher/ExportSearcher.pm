package Search::Searcher::ExportSearcher;

=head1 NAME

Search::Searcher::ExportSearcher

=head1 DESCRIPTION

Uses the export handler and docvalues to provide safe querying for large result sets

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;

#XXX fixme
use base qw(Search::Searcher);

use LS::Result::JSON::Facets;
use LS::Query::Facets;
use Debug::DUtils;

#XXX

# what methods do we need to overide?
SLIP_Utils::Solr::create_prod_shard_Searcher_by_alias($C, $shard);
 $rs = $searcher->get_Solr_raw_internal_query_result($C, $query, $rs);
    if (! $rs->http_status_ok()) {
        my $status = $rs->get_status_line();
        my $dump = $rs->get_failed_HTTP_dump();

	1 Do we really want to switch to json results rather than accepting the default xml?  maybe benchmark export handler and parser of
	json vs xml?
	



sub create_prod_shard_Searcher_by_alias {
    my $C = shift;
    my $shard = shift;
    my $timeout = shift;

    my $config = $C->get_object('MdpConfig');
    my $engine_uri = $config->get('prod_engine_for_shard_' . $shard);

XXX fixme    my $searcher = new Search::Searcher($engine_uri, $timeout);

    return $searcher;
}

	
# ---------------------------------------------------------------------
# HACK XXX overide Search::Searcher::__Solr_result
# copied from base class
#XXX   look up how to delegate to super in perl OOP
# don't really need to do this explicitly, but seems better.

sub __Solr_result {
    my $self = shift;
    my ($C, $query_string, $rs, $AB) = @_;

    # get export url
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

# ---------------------------------------------------------------------
sub __get_Solr_export_url {
    my $self = shift;
    my ($C, $query_string) = @_;

    my $primary_engine_uri = $self->get_engine_uri();
    
    my $script = $C->get_object('MdpConfig')->get('solr_select_script');
    my $url = 
        $primary_engine_uri 
            . $script 
                . '?' 
                  . (defined($shards_param) ? "${shards_param}&" : '')
                    . $query_string;

    return $url;
}



# ---------------------------------------------------------------------

# ---------------------------------------------------------------------

=item __get_Solr_select_url

Description

=cut

# ---------------------------------------------------------------------

# ---------------------------------------------------------------------

=item get_Solr_internal_query_result

Description

=cut

# ---------------------------------------------------------------------
sub get_Solr_internal_query_result {
    my $self = shift;
    my ($C, $Q, $rs) = @_;    

    my $query_string = $Q->get_Solr_internal_query_string();
    return $self->__Solr_result($C, $query_string, $rs);
}

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
