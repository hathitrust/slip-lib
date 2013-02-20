package Document::Doc;

=head1 NAME

Document

=head1 DESCRIPTION

This class implements teh abstract interface defined by the Document
supreclass and is the Composition parent of the Data and Metadata
objects that support construction of Solr documents for large-scale
full-text indexing and item-level indexing.

=head1 VERSION

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;

# App
use base qw(Document);
use Class::MOP;

use Context;
use Utils;
use Debug::DUtils;
use Identifier;
use Search::Constants;

use Db;

# ---------------------------------------------------------------------

=item PUBLIC API: create_document

Initialize Document object by composition of subclasses of
Document::Doc::Data and Document::Doc::vSolrMetadataAPI

=cut

# ---------------------------------------------------------------------
sub create_document {
    my $self = shift;
    my $C = shift;
    my $document_id = shift;
    
    my $config = $C->get_object('MdpConfig');
    my $document_metadata_class = $config->get('document_metadata_class');

    my %of_attrs;
    my $of = new ObjFactory;
    %of_attrs = (
                 'class_name' => $document_metadata_class,
                 'parameters' => {
                                  'C'  => $C,
                                  'id' => $document_id,
                                 },
                );
    my $metadata_obj = $of->create_instance($C, \%of_attrs);
    
    my $document_data_class = $config->get('document_data_class');
    %of_attrs = (
                 'class_name' => $document_data_class,
                 'parameters' => {
                                  'C'  => $C,
                                  'id' => $document_id,
                                 },
                );
    my $data_obj = $of->create_instance($C, \%of_attrs);

    $self->{D_metadata} = $metadata_obj;
    $self->{D_data} = $data_obj;

    $self->{D_doc_id} = $document_id;
    $self->{D_doc_context} = $C;
}



sub D_get_metadata_obj {
    my $self = shift;
    return $self->{D_metadata};
}

sub D_get_data_obj {
    my $self = shift;
    return $self->{D_data};
}

sub D_get_doc_id {
    my $self = shift;
    return $self->{D_doc_id};
}

sub D_get_doc_context {
    my $self = shift;
    return $self->{D_doc_context};
}

sub D_add_event {
    my $self = shift;
    my $event = shift;

    $self->{D_events} .= qq{$event\n};
    return $self->{D_events};
}

sub D_get_events {
    my $self = shift;

    my $s = '';

    my ($m_e, $d_e) = ($self->{D_metadata}->{D_events}, $self->{D_data}->{D_events});

    $s .= qq{\nMETADATA: $m_e} if ($m_e);
    $s .= qq{\nDATA: $d_e} if ($d_e);

    return $s;
}

sub D_check_event {
    my $self = shift;
    my ($status, $event) = @_;

    return if ($status == IX_NO_ERROR);
    $self->D_add_event($event);
}



# ---------------------------------------------------------------------

=item __call_plugins

Use Class::MOP (perl 5 Meta Object Protocol) to introspect the methods
on this object created by its plugin(s), in order by plugin name, then
by sequence convention and execute them.

=cut

# ---------------------------------------------------------------------
sub __call_plugins {
    my $self = shift;
    
    my $status = IX_NO_ERROR;

    my $plugin_method_names = $self->__get_plugin_method_names;
    if ($plugin_method_names) {
        foreach my $method (@$plugin_method_names) {
            $status = $self->$method;
            return $status unless($status == IX_NO_ERROR);
        }
    }
    return IX_NO_ERROR;
}

# ---------------------------------------------------------------------

=item __get_plugin_method_names

Description

=cut

# ---------------------------------------------------------------------
sub __get_plugin_method_names {
    my $self = shift;
    return $self->{_plugin_method_names};
}

# ---------------------------------------------------------------------

=item __construct_data_fields

Description

=cut

# ---------------------------------------------------------------------
sub __construct_data_fields {
    my $self = shift;
    my ($C, $state) = @_;
    
    my $start = Time::HiRes::time();
    my $item_id = $self->D_get_doc_id();

    my $data_status = IX_NO_ERROR;
    my $data_object = $self->D_get_data_obj();
    
    # Data (like OCR, JATS)
    my ($d_status, $data_elapsed);
    eval {
        ($d_status, $data_elapsed) = $data_object->build_data_fields($C, $item_id, $state);
        $data_status = IX_DATA_FAILURE unless($d_status == IX_NO_ERROR);
    };
    if ($@) {
        $data_status = IX_DATA_FAILURE;
    }
    $self->D_check_event($data_status, qq{data exception: $@});

    if ($data_status == IX_NO_ERROR) {
        $data_status = $data_object->__call_plugins();
    }

    $self->data_status($data_status);
    DEBUG('doc', qq{DATA: read data in sec=} . (Time::HiRes::time() - $start));
}

# ---------------------------------------------------------------------

=item __construct_metadata_fields

Description

=cut

# ---------------------------------------------------------------------
sub __construct_metadata_fields {
    my $self = shift;
    my ($C, $state) = @_;

    my $start = Time::HiRes::time();
    my $dbh = $C->get_object('Database')->get_DBH($C);
    my $item_id = $self->D_get_doc_id();

    my $metadata_status = IX_NO_ERROR;
    my $metadata_object = $self->D_get_metadata_obj();
    
    # Metadata fields
    my ($m_status);
    eval {
        ($m_status) = $metadata_object->build_metadata_fields($C, $dbh, $item_id, $state);
        $metadata_status = IX_METADATA_FAILURE unless($m_status == IX_NO_ERROR);
    };    
    if ($@) {
        $metadata_status = IX_METADATA_FAILURE;
    }   
    $self->D_check_event($metadata_status, qq{metadata exception: $@});

    $self->metadata_status($metadata_status);
    DEBUG('doc', qq{METADATA: read metadata in sec=} . (Time::HiRes::time() - $start));
}

# ---------------------------------------------------------------------

=item __save_stats

Description

=cut

# ---------------------------------------------------------------------
sub __save_stats {
    my $self = shift;
    my $elapsed = shift;
    
    my $metadata_fields_ref = $self->get_metadata_fields;
    my $data_fields_ref = $self->get_data_fields;

    my $stats;
    $stats->{create}{meta_size} = length($$metadata_fields_ref) if defined($metadata_fields_ref);
    $stats->{create}{data_size} = length($$data_fields_ref) if defined($data_fields_ref);
    $stats->{create}{doc_size} = $stats->{create}{meta_size} + $stats->{create}{data_size};

    $stats->{create}{elapsed}  = $elapsed;
    $self->{solr_doc}{stats} = $stats;
}

# ---------------------------------------------------------------------

=item PUBLIC API: build_document

Description

=cut

# ---------------------------------------------------------------------
sub build_document {
    my $self = shift;
    my ($C, $state) = @_;
    
    DEBUG('doc', qq{build_document: start});
    my $start = Time::HiRes::time();

    # Metadata fields
    $self->__construct_metadata_fields($C, $state);
 
    # Data (like OCR, JATS)
    $self->__construct_data_fields($C, $state); 
    
    my $elapsed = (Time::HiRes::time() - $start);
    DEBUG('doc', 
          sprintf("build_document: elapsed=%.3f sec metadata=%s data=%s", 
                  $elapsed, $self->metadata_status, $self->data_status));

    $self->__save_stats($elapsed);
}

# ---------------------------------------------------------------------

=item PUBLIC: finish_document

Clean up resources used to build the document

=cut

# ---------------------------------------------------------------------
sub finish_document {
    my $self = shift;
    my $C = shift;
    
    # Delegate cleanup
    $self->D_get_metadata_obj()->finish_document($C);
    $self->D_get_data_obj()->finish_document($C);
}

# ---------------------------------------------------------------------

=item PUBLIC: get_data_fields, get_metadata_fields

Accessors

=cut

# ---------------------------------------------------------------------
sub get_data_fields {
    my $self = shift;
    return $self->D_get_data_obj()->data_fields;
}

sub get_metadata_fields {
    my $self = shift;
    return $self->D_get_metadata_obj()->metadata_fields;
}


# ---------------------------------------------------------------------

=item PUBLIC: data_status, metadata_status

Mutators

=cut

# ---------------------------------------------------------------------
sub data_status {
    my $self = shift;
    my $ref = shift;
    if (defined $ref) {
        $self->{solr_doc}{data_status} = $ref;
    }
    return $self->{solr_doc}{data_status};
}

sub metadata_status {
    my $self = shift;
    my $ref = shift;
    if (defined $ref) {
        $self->{solr_doc}{metadata_status} = $ref;
    }
    return $self->{solr_doc}{metadata_status};
}

# ---------------------------------------------------------------------

=item PUBLIC: get_document_content

Description: Implements pure virtual method

=cut

# ---------------------------------------------------------------------
sub get_document_content {
    my $self = shift;
    my $C = shift;

    my $data_fields_ref = $self->get_data_fields();
    my $metadata_fields_ref = $self->get_metadata_fields();

    if ($metadata_fields_ref && $$metadata_fields_ref && $data_fields_ref && $$data_fields_ref) {
        my $solr_doc = '<doc>' . $$metadata_fields_ref . $$data_fields_ref . '</doc>';
        return \$solr_doc;
    }
    else {
        return undef;
    }
}

# ---------------------------------------------------------------------

=item PUBLIC: get_state_variable

Maintains the state of traversal over the Data content of a
Document. For example, over the OCR files in a package.

=cut

# ---------------------------------------------------------------------
sub get_state_variable {
    my $self = shift;
    my $C = shift;

    return $self->D_get_data_obj()->get_state_variable($C);
}

# ---------------------------------------------------------------------

=item PUBLIC: get_document_status

Description: 

=cut

# ---------------------------------------------------------------------
sub get_document_status {
    my $self = shift;
    my $C = shift;

    return (
            $self->data_status,
            $self->metadata_status,
           );
}

# ---------------------------------------------------------------------

=item PUBLIC: get_document_stats

Description: 

=cut

# ---------------------------------------------------------------------
sub get_document_stats {
    my $self = shift;
    my $C = shift;

    return $self->{solr_doc}{stats};
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2008-13 ©, The Regents of The University of Michigan, All Rights Reserved

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
