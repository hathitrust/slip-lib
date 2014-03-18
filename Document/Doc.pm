package Document::Doc;

=head1 NAME

Document::Doc

=head1 DESCRIPTION

Document::Doc implements the interface for document creation. The
Solr document is structured for submission to an Indexer.

Document::Doc is modeled as the Composition of a Data Class (such
as text in the form of OCR or XML) and a Metadata Class (consisting of
title, author, rights and so on), i.e. as a HAS-A relationship.

Methods defined in the interface are implemented by delegation to
methods in the Data and Metadata classes.

=head1 VERSION

=head1 SYNOPSIS

my $doc = new Document::Doc($C, $document_id);

=head1 METHODS

=over 8

=cut

use strict;
use warnings;

use Time::HiRes qw( time );

use Context;
use Utils;
use Debug::DUtils;
use Db;

use Search::Constants;
use Document::Reporter;

sub new {
    my $class = shift;

    my $self = {};
    bless $self, $class;
    $self->__initialize(@_);

    return $self;
}

# ---------------------------------------------------------------------

=item __initialize

Initialize Document object by composition of classes:
Document::Doc::Data and Document::Doc::vSolrMetadataAPI

=cut

# ---------------------------------------------------------------------
sub __initialize {
    my $self = shift;
    my ($C, $document_id, $tools, $doclist) = @_;

    my $config = $C->get_object('MdpConfig');

    $self->{D_doc_context}   = $C;
    $self->{D_doc_config}    = $config;
    $self->{D_doc_id}        = $document_id;
    $self->{D_doc_mets}      = $tools->{_METS};
    $self->{D_doc_extractor} = $tools->{_extractor};
    $self->{D_doc_tokenizer} = $tools->{_tokenizer};
    $self->{D_doc_doclist}   = $doclist;
    $self->{D_events}        = '';

    my ($metadata_obj, $data_obj);

    my $has_metadata = $tools->{_has_metadata};
    my $has_data = $tools->{_has_data};

    if ($has_metadata) {
        my $document_metadata_class = $config->get('document_metadata_class');
        my $of_attrs = {
                        class_name => $document_metadata_class,
                        parameters => {
                                       _facade => $self,
                                      },
                       };
        $metadata_obj = ObjFactory->create_instance($C, $of_attrs);
    }

    if ($has_data) {
        my $document_data_class = $config->get('document_data_class');
        my $of_attrs = {
                        class_name => $document_data_class,
                        parameters => {
                                       _facade => $self,
                                      },
                       };
        $data_obj = ObjFactory->create_instance($C, $of_attrs);
    }

    my $document_data_class_type = $config->get('document_data_class_type');
    my $document_extension_base_class = $config->get('document_extension_base_class');
    my $document_extension_class = $document_extension_base_class . "::$document_data_class_type";
    my $of_attrs = {
                    class_name => $document_extension_class,
                    parameters => {
                                   _facade => $self,
                                  },
                   };
    my $extension_obj = ObjFactory->create_instance($C, $of_attrs);


    $self->{D_metadata} = $metadata_obj;
    $self->{D_data} = $data_obj;
    $self->{D_extension} = $extension_obj;
}


# =====================================================================
# ==
# ==                       Public Interface
# ==
# =====================================================================

sub D_get_metadata_obj {
    my $self = shift;
    return $self->{D_metadata};
}

sub D_get_data_obj {
    my $self = shift;
    return $self->{D_data};
}

sub D_get_extension_obj {
    my $self = shift;
    return $self->{D_extension};
}

sub D_get_doc_id {
    my $self = shift;
    return $self->{D_doc_id};
}

sub D_get_doc_context {
    my $self = shift;
    return $self->{D_doc_context};
}

sub D_get_doc_config {
    my $self = shift;
    return $self->{D_doc_config};
}

sub D_get_doc_METS {
    my $self = shift;
    return $self->{D_doc_mets};
}

sub D_get_doc_extractor {
    my $self = shift;
    return $self->{D_doc_extractor};
}

sub D_get_doc_tokenizer {
    my $self = shift;
    return $self->{D_doc_tokenizer};
}

sub D_get_doc_doclist {
    my $self = shift;
    return $self->{D_doc_doclist};
}

sub D_add_event {
    my $self = shift;
    my $event = shift;

    $self->{D_events} .= qq{$event\n};
    return $self->{D_events};
}

sub D_get_events {
    my $self = shift;
    return $self->{D_events};
}

sub D_check_event {
    my $self = shift;
    my ($status, $event) = @_;

    return if ($status == IX_NO_ERROR);
    $self->D_add_event($event);
}

# ---------------------------------------------------------------------

=item PUBLIC: build_document

Description

=cut

# ---------------------------------------------------------------------
sub build_document {
    my $self = shift;
    my ($C, $state, $level, $granularity, $type) = @_;

    report(qq{build_document: start}, 1, 'doc');
    my $start = time;

    # Metadata fields
    $self->__construct_metadata_fields($C, $state);

    # Data (like OCR, JATS)
    $self->__construct_data_fields($C, $state);

    # Data (like OCR, JATS)
    $self->__construct_extension_fields($C, $state, $level, $granularity, $type);

    my $elapsed = (time - $start);
    report( sprintf("build_document[$level]: elapsed=%.6f sec metadata=%s data=%s", $elapsed, $self->metadata_status, $self->data_status), 1, 'doc');

    $self->__save_stats($level, $elapsed);
}

# ---------------------------------------------------------------------

=item PUBLIC: get_data_fields, get_metadata_fields, get_extension_fields

Accessors

=cut

# ---------------------------------------------------------------------
sub get_data_fields {
    my $self = shift;
    my $data_obj = $self->D_get_data_obj;
    if (defined $data_obj) {
        return $data_obj->data_fields;
    }
    return undef;
}

sub get_metadata_fields {
    my $self = shift;
    my $metadata_obj = $self->D_get_metadata_obj;
    if (defined $metadata_obj) {
        return $metadata_obj->metadata_fields;
    }
    return undef;
}

sub get_extension_fields {
    my $self = shift;
    my $extension_obj = $self->D_get_extension_obj;
    if (defined $extension_obj) {
        return $extension_obj->extension_fields;
    }
    return undef;
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

sub extension_status {
    my $self = shift;
    my $ref = shift;
    if (defined $ref) {
        $self->{solr_doc}{extension_status} = $ref;
    }
    return $self->{solr_doc}{extension_status};
}

# ---------------------------------------------------------------------

=item PUBLIC: get_document_content

Description

=cut

# ---------------------------------------------------------------------
sub get_document_content {
    my $self = shift;
    my $C = shift;

    my $data_fields_ref = $self->get_data_fields;
    my $metadata_fields_ref = $self->get_metadata_fields;
    my $extension_fields_ref = $self->get_extension_fields;

    my $ok = 0;

    my $solr_doc = '<doc>';
    if ($metadata_fields_ref) {
        $solr_doc .=  $$metadata_fields_ref;
        $ok = 1;
    }
    if ($extension_fields_ref) {
        $solr_doc .=  $$extension_fields_ref;
        $ok = 1;
    }
    if ($data_fields_ref) {
        $solr_doc .=  $$data_fields_ref;
        $ok = 1;
    }
    $solr_doc .= '</doc>';

    return $ok ? \$solr_doc : undef;
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

# ---------------------------------------------------------------------

=item PUBLIC: debug_save_doc

Description

=cut

# ---------------------------------------------------------------------
sub debug_save_doc {
    my $self = shift;
    my ($C, $state, $level) = @_;

    if (DEBUG('doconly')) {
        my $item_id = $self->D_get_doc_id;
        my $pairtree_item_id = Identifier::get_pairtree_id_wo_namespace($item_id);

        my $logdir = Utils::get_tmp_logdir;
        my $temporary_dir = $ENV{'SOLR_DOC_DIR'} ? $ENV{'SOLR_DOC_DIR'} : $logdir;
        my $complete_solr_doc_filename = "$temporary_dir/" . $pairtree_item_id . "-$$-$state-$level" . '.solr.xml';

        my $ref = $self->get_document_content($C);
        if ($ref) {
            Utils::write_data_to_file($ref, $complete_solr_doc_filename);
            chmod(0666, $complete_solr_doc_filename) if (-o $complete_solr_doc_filename);

            DEBUG('doconly', qq{build_document: save solr doc: "$complete_solr_doc_filename"});
        }
    }
}

# ---------------------------------------------------------------------

=item PUBLIC: get_rights_f_id

Description

=cut

# ---------------------------------------------------------------------
sub get_rights_f_id {
    my ($C, $id) = @_;

    my $dbh = $C->get_object('Database')->get_DBH($C);
    my $attr = Db::Select_j_rights_id_attr($C, $dbh, $id);

    unless ($attr) {
        report(qq{METADATA: $id MISSING from slip_rights}, 1, 'doc');
    }
    return $attr;
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

    my $start = time;
    my $item_id = $self->D_get_doc_id;

    my $data_status = IX_NO_ERROR;
    my $data_object = $self->D_get_data_obj;

    if (defined $data_object) {
        eval {
            $data_status = $data_object->build_data_fields($C, $state);
        };
        if ($@) {
            $data_status = IX_DATA_FAILURE;
            $self->D_add_event(qq{data exception: $@});
        }

        if ($data_status == IX_NO_ERROR) {
            eval {
                $data_status = $data_object->__call_plugins;
            };
            if ($@) {
                $self->D_add_event(qq{plugin exception});
            }

        }
    }

    $self->D_check_event($data_status, qq{data failure});
    $self->data_status($data_status);
    report(sprintf("DATA: fields constructed in sec=%.6f", time - $start), 0, 'doc');
}

# ---------------------------------------------------------------------

=item __construct_metadata_fields

Description

=cut

# ---------------------------------------------------------------------
sub __construct_metadata_fields {
    my $self = shift;
    my ($C, $state) = @_;

    my $start = time;
    my $dbh = $C->get_object('Database')->get_DBH($C);

    my $metadata_status = IX_NO_ERROR;
    my $metadata_object = $self->D_get_metadata_obj;

    if (defined $metadata_object) {
        eval {
            $metadata_status = $metadata_object->build_metadata_fields($C, $dbh, $state);
        };
        if ($@) {
            $metadata_status = IX_METADATA_FAILURE;
            $self->D_add_event(qq{metadata exception: $@});
        }
    }

    $self->D_check_event($metadata_status, qq{metadata failure});
    $self->metadata_status($metadata_status);
    report(sprintf("METADATA: fields constructed in sec=%.6f", (time - $start)), 0, 'doc');
}


# ---------------------------------------------------------------------

=item __construct_extension_fields

Description

=cut

# ---------------------------------------------------------------------
sub __construct_extension_fields {
    my $self = shift;
    my ($C, $state, $level, $granularity, $type) = @_;

    my $start = time;
    my $item_id = $self->D_get_doc_id;
    
    my $extension_status = IX_NO_ERROR;
    my $extension_object = $self->D_get_extension_obj;

    if (defined $extension_object) {
        eval {
            $extension_status = $extension_object->build_extension_fields($C, $state, $level, $granularity, $type);
        };
        if ($@) {
            $extension_status = IX_EXTENSION_FAILURE;
            $self->D_add_event(qq{extension exception: $@});
        }
    }

    $self->D_check_event($extension_status, qq{extension failure});
    $self->extension_status($extension_status);
    report(qq{EXTENSION: fields constructed in sec=} . (time - $start), 0, 'doc');
}

# ---------------------------------------------------------------------

=item __save_stats

Description

=cut

# ---------------------------------------------------------------------
sub __save_stats {
    my $self = shift;
    my ($level, $elapsed) = @_;

    my $metadata_fields_ref = $self->get_metadata_fields;
    my $data_fields_ref = $self->get_data_fields;
    my $extension_fields_ref = $self->get_extension_fields;

    my $stats;
    $stats->{create}{elapsed} = $elapsed;

    if ($level eq 'child') {
        $stats->{create}{meta_size} = 0;
        $stats->{create}{data_size} = 0;
        $stats->{create}{exte_size} = 0;
        $stats->{create}{doc_size} = 0;
    }
    else {
        if ($level eq 'parent') {
            $stats->{create}{meta_size} = (defined $metadata_fields_ref) ? length($$metadata_fields_ref) : 0;
            $stats->{create}{data_size} = 0;
            $stats->{create}{exte_size} = (defined $extension_fields_ref) ? length($$extension_fields_ref) : 0; # includes children
            $stats->{create}{doc_size} = $stats->{create}{meta_size} + $stats->{create}{data_size} + $stats->{create}{exte_size};
        }
        else {
            # flat
            $stats->{create}{meta_size} = (defined $metadata_fields_ref) ? length($$metadata_fields_ref) : 0;
            $stats->{create}{data_size} = (defined $data_fields_ref) ? length($$data_fields_ref) : 0;
            $stats->{create}{exte_size} = (defined $extension_fields_ref) ? length($$extension_fields_ref) : 0;
            $stats->{create}{doc_size} = $stats->{create}{meta_size} + $stats->{create}{data_size} + $stats->{create}{exte_size};
        }
    }
    $self->{solr_doc}{stats} = $stats;
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2008-14 ©, The Regents of The University of Michigan, All Rights Reserved

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
