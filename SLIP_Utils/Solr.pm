package SLIP_Utils::Solr;


=head1 NAME

 SLIP_Utils::Solr

=head1 DESCRIPTION

Some useful subs to create objects to interact with Solr

=head1 VERSION

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;

# App
use Debug::DUtils;
use Context;
use MdpConfig;
use Search::Indexer;
use Search::Searcher;
use Search::Searcher::ExportSearcher;
# ---------------------------------------------------------------------

=item create_shard_Indexer_by_alias

Description

=cut

# ---------------------------------------------------------------------
sub create_shard_Indexer_by_alias {
    my $C = shift;
    my $shard = shift;
    my $timeout = shift;

    my $config = $C->get_object('MdpConfig');
    my $engine_uri = $config->get('engine_for_shard_' . $shard);

    my $indexer = new Search::Indexer($engine_uri, $timeout);

    return $indexer;
}

# ---------------------------------------------------------------------

=item create_prod_shard_Indexer_by_alias

Description

=cut

# ---------------------------------------------------------------------
sub create_prod_shard_Indexer_by_alias {
    my $C = shift;
    my $shard = shift;
    my $timeout = shift;

    my $config = $C->get_object('MdpConfig');
    my $engine_uri = $config->get('prod_engine_for_shard_' . $shard);

    my $indexer = new Search::Indexer($engine_uri, $timeout);

    return $indexer;
}
# ---------------------------------------------------------------------

=item create_export prod_shard_Searcher_by_alias

Same as create_prod_shard_Searcher_by_alias but returns a ExportSearcher instead of generic Search::Searcher

=cut

# ---------------------------------------------------------------------
sub create_export prod_shard_Searcher_by_alias {
    my $C = shift;
    my $shard = shift;
    my $timeout = shift;

    my $config = $C->get_object('MdpConfig');
    my $engine_uri = $config->get('prod_engine_for_shard_' . $shard);

    my $searcher = new Search::Searcher::ExportSearcher($engine_uri, $timeout);

    return $searcher;
}

# ---------------------------------------------------------------------

=item create_shard_Searcher_by_alias

Description

=cut

# ---------------------------------------------------------------------
sub create_shard_Searcher_by_alias {
    my $C = shift;
    my $shard = shift;
    my $timeout = shift;

    my $config = $C->get_object('MdpConfig');
    my $engine_uri = $config->get('engine_for_shard_' . $shard);

    my $searcher = new Search::Searcher($engine_uri, $timeout);

    return $searcher;
}

# ---------------------------------------------------------------------

=item create_prod_shard_Searcher_by_alias

Description

=cut

# ---------------------------------------------------------------------
sub create_prod_shard_Searcher_by_alias {
    my $C = shift;
    my $shard = shift;
    my $timeout = shift;

    my $config = $C->get_object('MdpConfig');
    my $engine_uri = $config->get('prod_engine_for_shard_' . $shard);

    my $searcher = new Search::Searcher($engine_uri, $timeout);

    return $searcher;
}

# ---------------------------------------------------------------------

=item create_VuFind_Solr_Searcher_by_alias

Description

=cut

# ---------------------------------------------------------------------
sub create_VuFind_Solr_Searcher_by_alias {
    my $C = shift;

    my $config = $C->get_object('MdpConfig');
    my $engine_uri = $config->get('engine_for_vSolr');

    my $searcher = new Search::Searcher($engine_uri, 30);

    return $searcher;
}


1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2009 ©, The Regents of The University of Michigan, All Rights Reserved

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
