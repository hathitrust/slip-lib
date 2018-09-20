package Result::JSON; 

use strict;

use JSON::XS;
use Utils;
use base qw(Search::Result);

# ---------------------------------------------------------------------
# Overide base class because we need to parse json to get these things
# maybe base class should use methods to parse these things and call a parser object that
# would know how to parse the response instead of subclassing


# ---------------------------------------------------------------------
sub ingest_Solr_search_response
{
    my $self = shift;
    my ($code, $Solr_response_ref, $status_line, $failed_HTTP_dump) = @_;

    my $http_status_ok = ($code eq '200');

    my ($max_score, $num_found, $query_time) = (0, 0, 0.0);


    my $parsed; #parsed json Solr response object
    
    if ($http_status_ok)
    {
         $parsed = $self->parse_JSON_results($Solr_response_ref);

    
       # QTime (query time in milliseconds)
        $query_time = $parsed->{responseHeader}->{QTime};
        $query_time = sprintf("%.3f", $query_time/1000);

        # Max score
        ($max_score) = ($parsed->{response}->{maxScore});
        $max_score = $max_score ? $max_score : 0.0;

        # Hits
        ($num_found) = ($parsed->{response}->{numFound});
        $num_found = $num_found ? $num_found : 0;
    }

    $self->{'http_status_ok'} = $http_status_ok;
    $self->{'response_code'} = $code;
    $self->{'status_line'} = $status_line;
    $self->{'query_time'} = $query_time;
    $self->{'max_score'} = $max_score;
    $self->{'num_found'} = $num_found;
    # May be overridden for queries that limit by rows
    $self->{'rows_returned'} = $num_found;
    $self->{'failed_HTTP_dump'} = $failed_HTTP_dump;

    # In Subclass:
    if ($http_status_ok)
    {
        $self->AFTER_ingest_Solr_search_response($parsed);
    }
}

# ---------------------------------------------------------------------
# ---------------------------------------------------------------------
sub parse_JSON_results
{   
    my $self = shift;
    my $Solr_response_ref = shift;

    # Warning json won't escape xml entities such as "&" ">" etc.
    my $coder = JSON::XS->new->utf8->pretty->allow_nonref;
    my $parsed = $coder->decode ($$Solr_response_ref);
    
    return $parsed;
    
}

# ---------------------------------------------------------------------
#
1;

