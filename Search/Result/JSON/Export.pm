package Result::JSON::Export;


=head1 NAME

Result::JSON::Export

=head1 DESCRIPTION

This class does encapsulates the Solr search response data in json format

=head1 VERSION

$Id:$

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;

use Utils;
use Debug::DUtils;
use JSON::XS;
use URI::Escape;
use base qw(Result::JSON);

# ---------------------------------------------------------------------

=item AFTER_Result_initialize

Do we need anything here?

=cut

# ---------------------------------------------------------------------
sub AFTER_Result_initialize
{
    my $self = shift;
    
}

# ---------------------------------------------------------------------


# ---------------------------------------------------------------------

=item AFTER_ingest_Solr_search_response

Example Solr result is:


=cut

# ---------------------------------------------------------------------
sub AFTER_ingest_Solr_search_response
{
    my $self = shift;
    # since this is a subclass of LS::Result::JSON we expect a parsed json object rather than
    # a Solr XML response string
    my $Parsed_Solr_response_ref = shift;

    my $docs = $Parsed_Solr_response_ref->{'response'}->{'docs'};
    
    # check to see if there is at least one doc
    my $count=0;
    
    if (defined($docs->[0]))
    {
	my @result_ids;
	my @complete_result;

        foreach my $doc (@{$docs})
        {
	    $count++;
	    $doc->{'result_number'} = $count;
	    my $id = $doc->{'id_dv'};
            push (@result_ids,$id);
	    my @coll_ids = ();
	    my $col_ary_ref= ($doc->{'coll_id'});#XXX coll_id_dv ??
	    foreach my $coll_id (@{$col_ary_ref})
	    {
		push(@coll_ids,$coll_id);
	    }
	    my $hash_ref = {
			    'id' => $id,
			    'coll_ids' => \@coll_ids,
			   };
        push(@complete_result, $hash_ref);
        push(@result_ids, $id);
        }
	$self->{'rows_returned'} = scalar(@result_ids);
	$self->set_complete_result(\@complete_result);
	$self->__set_result_ids(\@result_ids);
        $self->__set_result_ids(\@result_ids);
    }
}



# ---------------------------------------------------------------------

=item PRIVATE:__set_result_solr_debug

Description: results of a solr debug/explain query parsed from the json response

=cut

# ---------------------------------------------------------------------
sub  __set_result_solr_debug
{
    my $self = shift;
    my $solr_debug = shift;
    $self->{'result_solr_debug'} = $solr_debug;
}

# ---------------------------------------------------------------------

=item :get_result_solr_debug

Description

=cut

# ---------------------------------------------------------------------
sub get_result_solr_debug
{
    my $self = shift;
    return     $self->{'result_solr_debug'} ;
}



# ---------------------------------------------------------------------



=item PRIVATE: __set_result_docs

Description

=cut

# ---------------------------------------------------------------------
sub __set_result_docs
{
    my $self = shift;
    my $arr_ref = shift;

    $self->{'result_response_docs_arr_ref'} = $arr_ref;
}


# ---------------------------------------------------------------------

=item PRIVATE: __set_result_ids

Description

=cut

# ---------------------------------------------------------------------
sub __set_result_ids
{
    my $self = shift;
    my $arr_ref = shift;
    $self->{'result_ids'} = $arr_ref;
}



# ---------------------------------------------------------------------

=item get_result_ids

Description

=cut

# ---------------------------------------------------------------------
sub get_result_ids
{
    my $self = shift;
    return $self->{'result_ids'};
}


# ---------------------------------------------------------------------

=item get_result_docs

Description

=cut

# ---------------------------------------------------------------------
sub get_result_docs
{
    my $self = shift;
    return $self->{'result_response_docs_arr_ref'};
}
# ---------------------------------------------------------------------

#  these set per query start number and number of rows for taking a slice
# of the $i_rs doc array ref
#		$i_rs->set_start($user_solr_start_row);
#		$i_rs->set_num_rows($user_solr_num_rows);
# ---------------------------------------------------------------------
# ---------------------------------------------------------------------
sub set_start
{
    my $self = shift;
    my $start = shift;
    $self->{'start'} = $start;
}
# ---------------------------------------------------------------------
sub set_num_rows
{
    my $self = shift;
    my $num_rows = shift;
    $self->{'num_rows'} = $num_rows;
}

# ---------------------------------------------------------------------
sub get_slice_result_docs
{
    my $self = shift;
    #XXX do we need to replace by getter functions?
    my $start = $self->{'start'};
    my $num = $self->{'num_rows'};
    my $end = $start + $num;
    my $ary_ref =[];
    #XXX figure out 0 based array off by 1 stuff


    # FOR debugging
    # number il ref
    
    if (DEBUG('AB'))
    {
	my $i =1;
	my $arr_ref = $self->{'result_response_docs_arr_ref'};
	foreach my $hash (@{$arr_ref})
	{
	    $hash->{'il_num'} = $i;
	    $i++;
	}
    }
        
    my $i = $start;
    for ( $i = $start; $i < $end; $i++)
    {
	push(@{$ary_ref}, $self->{'result_response_docs_arr_ref'}->[$i]);
    }
    return ($ary_ref);
}


# ---------------------------------------------------------------------

=item __set_result_type

Description

=cut

# ---------------------------------------------------------------------
sub __set_result_type
{
    my $self = shift;
    my $type = shift;
    $self->{'result_type'} = $type;
}

# ---------------------------------------------------------------------

=item get_result_type

Description

=cut

# ---------------------------------------------------------------------
sub get_result_type
{
    my $self = shift;
    return $self->{'result_type'};
}



1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2008 ©, The Regents of The University of Michigan, All Rights Reserved

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

