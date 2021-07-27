package Search::Searcher;

=head1 NAME

Search::Searcher (searcher)

=head1 DESCRIPTION

This class encapsulates the search interface to Solr/Lucene. It
provides two interfaces.  One to handle user entered queries and one
to handle queries generated internally by the application.

=head1 SYNOPSIS

my $searcher = new Search::Searcher(30, <<solr_engine URI>>');
my $rs = new Search::Result();

my $query_string = qq{q=*:*&start=0&rows=10&fl=id&indent=on};
$rs = $searcher->get_Solr_raw_internal_query_result($C, $query_string, $rs);

my $id_arr_ref = $rs->get_result_ids();

=head1 METHODS

=over 8

=cut

use strict;
use warnings;

   use Data::Dumper;

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

use constant DEFAULT_TIMEOUT => 30; # LWP default

my $UA_VERSION = 1.1;

sub new {
    my $class = shift;

    my $self = {};
    bless $self, $class;
    $self->_initialize(@_);

    return $self;
}


# ---------------------------------------------------------------------

=item _initialize

Initialize Search::Searcher object.

=cut

# ---------------------------------------------------------------------
sub _initialize {
    my $self = shift;
    my $engine_uri = shift;
    my $timeout = shift;
    my $use_ls_shards = shift;

    ASSERT(defined($engine_uri), qq{Missing Solr engine URI});
    $self->{'Solr_engine_uri'} = $engine_uri;
    $self->{'timeout'} = defined($timeout) ? $timeout : DEFAULT_TIMEOUT;
    $self->{use_ls_shards} = $use_ls_shards;
}

# ---------------------------------------------------------------------

=item PRIVATE: __get_timeout

Description

=cut

# ---------------------------------------------------------------------
sub __get_timeout {
    my $self = shift;
    return $self->{'timeout'};
}


# ---------------------------------------------------------------------

=item PRIVATE: __Solr_result

Helper

=cut

# ---------------------------------------------------------------------
sub __Solr_result {
    my $self = shift;
    my ($C, $query_string, $rs) = @_;

    my $config = $C->get_object('MdpConfig');

    my $url = $self->__get_Solr_select_url($C, $query_string);

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

=item get_Solr_raw_internal_query_result

Description

=cut

# ---------------------------------------------------------------------
sub get_Solr_raw_internal_query_result {
    my $self = shift;
    my ($C, $query_string, $rs) = @_;

    my $result = $self->__Solr_result($C, $query_string, $rs);
    return $result;
}


# ---------------------------------------------------------------------

=item PUBLIC: get_populated_Solr_query_result

Description

=cut

# ---------------------------------------------------------------------
sub get_populated_Solr_query_result {
    my $self = shift;
    my ($C, $Q, $rs) = @_;

    ASSERT(0, qq{get_populated_Solr_query_result() in __PACKAGE__ is pure virtual});
}


# ---------------------------------------------------------------------

=item PRIVATE: __create_user_agent

Description

=cut

# ---------------------------------------------------------------------
sub __create_user_agent {
    my $self = shift;

    my $timeout = $self->__get_timeout();

    # Create a user agent object
    my $ua = LWP::UserAgent->new;
    $ua->agent(qq{HathiTrust/$UA_VERSION});
    $ua->timeout($timeout)
        if (defined($timeout));

    return $ua;
}


# ---------------------------------------------------------------------

=item get_collid

Description

=cut

# ---------------------------------------------------------------------
sub get_collid {
    my $self = shift;
    return $self->{'collid'};
}


# ---------------------------------------------------------------------

=item __get_request_object

Description

=cut

# ---------------------------------------------------------------------
sub __get_request_object {
    my $self = shift;
    my $uri = shift;

    my ($url, $query_string) = (split(/\?/, $uri));  

    # If this is a string of characters, translate from Perl's
    # internal representation to bytes to make HTTP::Request happy.
    # If it came from a terminal, it will probably be a sequence of
    # bytes already (utf8 flag not set).
    if (Encode::is_utf8($query_string)) {
        $query_string = Encode::encode_utf8($query_string);
    }

    my $req = HTTP::Request->new('POST', $url, undef, $query_string);

    $req->header( 'Content-Type' => 'application/x-www-form-urlencoded; charset=utf8'  );

    # Put in some basic auth for testing against k8s cluster
    my $basic_auth_token = 'c29scjpXdTM5Q2E5eHZaNU5GQjNweTdVRA==';
    $req->header('Authorization' => "Basic $basic_auth_token");

    return $req;
}


# ---------------------------------------------------------------------

=item get_engine_uri

Description

=cut

# ---------------------------------------------------------------------
sub get_engine_uri {
    my $self = shift;
    return $self->{'Solr_engine_uri'};
 }


# ---------------------------------------------------------------------

=item get_random_shard_solr_engine_uri CLASS METHOD

Randomize the primary Solr instance for multishard queries to
distribute the result merge load. Want index of the shard in the array
of mbooks_solr_engines URIs.

=cut

# ---------------------------------------------------------------------
sub get_random_shard_solr_engine_uri {
    my $C = shift;

    my $config = $C->get_object('MdpConfig');
    
    my @engine_uris = $config->get('mbooks_solr_engines');
    my @num_shards_list = $config->get('num_shards_list');

    my $random_shard = $num_shards_list[ int rand @num_shards_list ];
    my ($index_of_random_shard) = grep { $num_shards_list[$_] eq $random_shard } 0..$#num_shards_list;
    
    return $engine_uris[ $index_of_random_shard ];
}


# ---------------------------------------------------------------------

=item use_ls_shards

Add shards param to search LS index

=cut

# ---------------------------------------------------------------------
sub use_ls_shards {
    my $self = shift;

    return $self->{use_ls_shards};
}

# ---------------------------------------------------------------------

=item __get_LS_Solr_shards_param

Description

=cut

# ---------------------------------------------------------------------
sub __get_LS_Solr_shards_param {
    my $self = shift;
    my $C = shift;
    
    my $shards_param;
    my $config = $C->get_object('MdpConfig');

    my @shard_engine_uris = $config->get('mbooks_solr_engines');
    my @num_shards_list = $config->get('num_shards_list');
    if (scalar(@num_shards_list) > 1) {
        my @active_shard_engine_uris;
        foreach my $shard (@num_shards_list) {
            push(@active_shard_engine_uris, $shard_engine_uris[$shard-1]);
        }
        map {$_ =~ s,^http://,,} @active_shard_engine_uris;
        
        $shards_param = 'shards=' . join(',', @active_shard_engine_uris);
    }

    return $shards_param;
}


# ---------------------------------------------------------------------

=item __get_Solr_select_url

Description

=cut

# ---------------------------------------------------------------------
sub __get_Solr_select_url {
    my $self = shift;
    my ($C, $query_string) = @_;

    my $shards_param = $self->use_ls_shards() ? $self->__get_LS_Solr_shards_param($C) : undef;
    
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

=item PRIVATE: __get_query_response

Description

=cut

# ---------------------------------------------------------------------
sub __get_query_response {
    my $self = shift;
    my ($C, $ua, $req) = @_;
    
    my $res = $ua->request($req);

    my $code = $res->code();
    my $status_line = $res->status_line;
    my $http_status_fail = (! $res->is_success());
    
    # Debug / fail logging
    my $responseDebug = DEBUG('response,idx,all');
    my $otherDebug = DEBUG('idx,all');
    my $Debug = $responseDebug || $otherDebug;
    
    my $failed_HTTP_dump = '';
        
    if ($Debug || $http_status_fail) {

        if ($otherDebug) {
            my $u = $req->url();
            Utils::map_chars_to_cers(\$u);
            my $s = qq{__get_query_response: request="$u": status="$code" status_line=} . $status_line;
            DEBUG('idx,all', $s);
        }
        
        if ($responseDebug || $http_status_fail) {
            require Data::Dumper;
            my $d = Data::Dumper::Dumper($res);
            
            if ($http_status_fail) {
                my $sesion_id = 0;
                if ($C->has_object('Session')) {
                    $sesion_id = $C->get_object('Session')->get_session_id();
                }
                my $remote_addr = ($ENV{REMOTE_ADDR} ? $ENV{REMOTE_ADDR} : '0.0.0.0');
                my $lg = qq{$remote_addr $sesion_id $$ } . Utils::Time::iso_Time('time') . qq{ $d};
                my $app_name = $C->has_object('App') ? $C->get_object('App')->get_app_name($C) : 'ls';
		Utils::Logger::__Log_string($C, $lg, 'query_error_logfile', '___QUERY___', $app_name);
                $failed_HTTP_dump = $d;
            }
            
            Utils::map_chars_to_cers(\$d, [q{"}, q{'}]) if Debug::DUtils::under_server();
            DEBUG('response', $d);
        }
    }

    if (! $http_status_fail) {
        return ($code, $res->content(), $res->status_line(), '');
    }
    else {
        return ($code, '',  $res->status_line(), $failed_HTTP_dump);
    }
}



1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2007-9 ©, The Regents of The University of Michigan, All Rights Reserved

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
