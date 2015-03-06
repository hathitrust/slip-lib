package Document::Generator;

=head1 NAME

Document::Generator;

=head1 DESCRIPTION

This class defines a generator for Document::Doc.

It expects a Context and a item id. The constituents of Document::Doc
that are generated depends on config data.

Document::Generator HAS-A Document::Doc, Extractor and METS.

The generate() method will create the document content that expresses
the schema of the Document::Doc metadata and data constituents.  A
given item_id may equate to one or more instances of Solr documents.

A state variable is maintained so that the generator can tell when all
the Solr document content instances have been generated.

=head1 SYNOPSIS

 my $RUN = 1;
 my $C = new Context;

 my $config = SLIP_Utils::Common::gen_run_config($app, $RUN);
 $C->set_object('MdpConfig', $config);

 my $db = new Database('ht_maintenance');
 $C->set_object('Database', $db);
 my $id_arr_ref = ['mdp.39015015823563'];

 my $generator = new Document::Generator($C, $id );
 $generator->G_generate($C);
 my $solr_doc = $generator->G_get_generated_document;

 unless ($solr_doc) {
   my ($data_status, $metadata_status) = $generator->G_status;
   report_error($data_status, $metadata_status);
 }


=head1 METHODS

=over 8

=cut

use strict;
use warnings;

use Time::HiRes qw( time );

use Context;
use Utils;
use MdpConfig;
use DataTypes;
use ObjFactory;

use Search::Constants;

use Document::Reporter;
use Document::METS;
use Document::Extractor;
use Document::Doc;

my $Generator_Singleton;

sub new {
    my $class = shift;
    my ($C, $item_id) = @_;

    if (defined $Generator_Singleton) {
        unless ($Generator_Singleton->__G_item_id eq $item_id) {
            undef $Generator_Singleton;
        }
    }

    unless (defined $Generator_Singleton) {
        my $this = {};
        $Generator_Singleton = bless $this, $class;
        $Generator_Singleton->__initialize($C, $item_id);
    }

    $Generator_Singleton->__G_state_init;

    return $Generator_Singleton;
}

# ---------------------------------------------------------------------

=item __initialize

Only for singleton initialization.

=cut

# ---------------------------------------------------------------------
sub __initialize {
    my $self = shift;
    my ($C, $item_id) = @_;

    my $start = time;

    $self->{_context} = $C;
    $self->{_item_id} = $item_id;
    $self->{_config} = $C->get_object('MdpConfig');
    $self->{_doclist} = [];
    $self->{_configured_granularity} = $self->__G_config->get('document_data_tokenizer_granulatity');

    $self->{_data_status} = IX_NO_ERROR;
    $self->{_metadata_status} = IX_NO_ERROR;

    my $uses = $self->__G_get_USEs($item_id);
    my $mets = $self->__G_instantiate_mets($item_id, $uses);
    my $extractor = $self->__G_instantiate_extractor($item_id, $mets);
    my $granularity = $self->__G_granularity;
    $self->__G_instantiate_tokenizer($mets, $extractor, $granularity);

    my $document_data_class_type = $self->__G_config->get('document_data_class_type');
    my $type = $self->__G_generator_type($document_data_class_type);

    # level transitions: 'child' --> 'parent' --> 'terminal' or 'flat' --> 'terminal'
    my $doc_level = ($type eq 'nested') ? 'child' : 'flat';
    $self->__G_doc_level($doc_level);

    my $elapsed = time - $start;
    $self->{_stats} = {
                       create => { elapsed => $elapsed }
                      };
    report( sprintf("Generator[initialization]: elapsed=%.5f sec", $elapsed), 0, 'doc');
}

# ---------------------------------------------------------------------

=item G_generate

Description

=cut

# ---------------------------------------------------------------------
sub G_generate {
    my $self = shift;
    my $C = shift;

    my $doc;

    while ( $doc = $self->__G_generate_next ) {

        my ($data_status, $metadata_status) = $doc->get_document_status;

        unless ( ($data_status == IX_NO_ERROR) && ($metadata_status == IX_NO_ERROR) ) {
            $self->G_events( $doc->D_get_events() );
            $self->G_status($data_status, $metadata_status);
        }

        # flat   = [one or more chunked docs]
        # nested = [a parent doc containing chunk children]
        $self->__G_doc_doclist_enqueue($doc);
        $self->__G_process_statistics($C, $doc);
    }
}

# ---------------------------------------------------------------------

=item PUBLIC: G_release

Document::Doc has circular-references that have to be broken to
garbage-collect memory.

=cut

# ---------------------------------------------------------------------
sub G_release {
    my $self = shift;

    my $doc_ct = 0;
    my $start = time;

    while ( my $doc = pop @{ $self->__G_doc_list } ) {
        $doc->D_release;
        $doc_ct++;
    }

    # release extraction directory
    $self->__G_extractor->E_unlink_extraction_dir;

    my $elapsed = time - $start;
    report( sprintf("Generator[release]: num_docs=$doc_ct elapsed=%.5f sec", $elapsed), 0, 'doc');
}

# ---------------------------------------------------------------------

=item PUBLIC: G_get_generated_document

Description

=cut

# ---------------------------------------------------------------------
sub G_get_generated_document {
    my $self = shift;
    my $C = $self->__G_context;

    my $Solr_document;

    foreach my $doc ( @{ $self->__G_doc_list } ) {
        my $ref = $doc->get_document_content($C);
        $$Solr_document .= $$ref if ($ref);
    }

    return $Solr_document;
}

# ---------------------------------------------------------------------

=item PUBLIC: G_events

Event accumulator

=cut

# ---------------------------------------------------------------------
sub G_events {
    my $self = shift;
    my $events = shift;
    if (defined $events) {
        $self->{_events} .= $events;
    }
    return $self->{_events};
}

# ---------------------------------------------------------------------

=item PUBLIC: G_status

Mutator.  Persists a non-zero data_status or metadata_status.

NOTE: Member statuses are initialized to IX_NO_ERROR

=cut

# ---------------------------------------------------------------------
sub G_status {
    my $self = shift;
    my ($data_status, $metadata_status) = @_;

    if (defined $data_status) {
        unless ($data_status == IX_NO_ERROR) {
            # ... bad incoming. set _data_status unless _data_status is already bad
            if ($self->{_data_status} == IX_NO_ERROR) {
                $self->{_data_status} = $data_status;
            }
        }
    }

    if (defined $metadata_status) {
        unless ($metadata_status == IX_NO_ERROR) {
            # ... bad incoming. set _metadata_status unless _metadata_status is already bad
            if ($self->{_metadata_status} == IX_NO_ERROR) {
                $self->{_metadata_status} = $metadata_status;
            }
        }
    }

    return ( $self->{_data_status}, $self->{_metadata_status} );
}

# ---------------------------------------------------------------------

=item G_stats

Description

=cut

# ---------------------------------------------------------------------
sub G_stats {
    my $self = shift;
    return $self->{_stats};
}

# ---------------------------------------------------------------------

=item __G_state_init

Re-initialize for each recursive call.

=cut

# ---------------------------------------------------------------------
sub __G_state_init {
    my $self = shift;
    $self->{S_state} = 0;
}

# ---------------------------------------------------------------------

=item __G_process_statistics

Are we building the parent or a flat doc?

=cut

# ---------------------------------------------------------------------
sub __G_process_statistics {
    my $self = shift;
    my ($C, $doc) = @_;

    SLIP_Utils::Common::merge_stats( $C, $self->G_stats, $doc->get_document_stats($C) );
}

# ---------------------------------------------------------------------

=item __G_get_state

Maintains the state of traversal over the Tokenizer service to build
documents.

In the case of Document::Doc::Data::File, we are building a Solr
document that contains the text of one page of an item.

In the case of Document::Doc::Data::Token, we are building a Solr
document that contains a chunk of N tokens ("words") from an item.

Supports hierarchically structured Solr documents built by recursion
on the Generator.

=cut

# ---------------------------------------------------------------------
sub __G_get_state {
    my $self = shift;

    my $state = ++$self->{S_state};
    my $level = $self->__G_doc_level;

    if ($level eq 'parent') {
        if ($state > 1) {
            undef $state;
        }
    }
    elsif ($state > $self->__G_tokenizer->T_num_chunks) {
        undef $state;
    }

    return $state;
}

# ---------------------------------------------------------------------

=item __G_generate_next

Description

=cut

# ---------------------------------------------------------------------
sub __G_generate_next {
    my $self = shift;

    my $C = $self->__G_context;
    my $state = $self->__G_get_state;

    if (defined $state) {
        my $level = $self->__G_doc_level;
        my $granularity = $self->__G_granularity;
        my $token_type = $self->__G_tokenizer->T_tokenization_type;

        my $tools = {
                     _METS         => $self->__G_METS,
                     _extractor    => $self->__G_extractor,
                     _tokenizer    => $self->__G_tokenizer,
                     _has_metadata => ($level eq 'parent') || ($level eq 'flat'),
                     _has_data     => ($level eq 'child') || ($level eq 'flat'),
                    };

        my $doc = new Document::Doc(
                                    $C,
                                    $self->__G_item_id,
                                    $tools,
                                    $self->__G_doc_list
                                   );

        $doc->build_document($C, $state, $level, $granularity, $token_type);
        $doc->debug_save_doc($C, $state, $level);

        return $doc;
    }
    else {
        $self->__G_finish;
        return undef;
    }
}

# ---------------------------------------------------------------------

=item __G_finish

Handle recursion for nested documents

=cut

# ---------------------------------------------------------------------
sub __G_finish {
    my $self = shift;

    my $level = $self->__G_doc_decrement_level;

    if ($level ne 'terminal') {
        my $C = $self->__G_context;
        my $generator = Document::Generator->new(
                                                 $C,
                                                 $self->__G_item_id,
                                                 $self->__G_doc_list
                                                );
        $generator->G_generate($C);
    }
}

# ---------------------------------------------------------------------

=item __G_doc_doclist_enqueue

Description

=cut

# ---------------------------------------------------------------------
sub __G_doc_doclist_enqueue {
    my $self = shift;
    my $doc = shift;

    push(@{ $self->{_doclist} }, $doc);
}

# ---------------------------------------------------------------------

=item __G_doc_list

Description

=cut

# ---------------------------------------------------------------------
sub __G_doc_list {
    my $self = shift;
    return $self->{_doclist};
}

# ---------------------------------------------------------------------

=item __G_doc_level

Description

=cut

# ---------------------------------------------------------------------
sub __G_doc_level {
    my $self = shift;
    my $level = shift;

    if (defined $level) {
        if (exists $self->{_level}) {
            ASSERT(0, qq{attempt to overwrite document level});
        }
        $self->{_level} = $level;
    }

    return $self->{_level};
}

# ---------------------------------------------------------------------

=item __G_doc_decrement_level

Description

=cut

# ---------------------------------------------------------------------
sub __G_doc_decrement_level {
    my $self = shift;

    unless (exists $self->{_level}) {
        ASSERT(0, qq{document level not initialized});
    }

    my $level = $self->{_level};

    if ($level eq 'child') {
        $level = 'parent';
    }
    elsif ($level eq 'parent') {
        $level = 'terminal';
    }
    elsif ($level eq 'flat') {
        $level = 'terminal';
    }
    else {
        ASSERT(0, qq{level=$level cannot be decremented});
    }

    return $self->{_level} = $level;
}

# ---------------------------------------------------------------------

=item __G_get_USEs

Description

=cut

# ---------------------------------------------------------------------
sub __G_get_USEs {
    my $self = shift;
    my $item_id = shift;

    my $C = $self->__G_context;

    my $document_data_uses_class = $self->__G_config->get('document_data_uses_class');
    my $of_attrs = {
                    class_name => $document_data_uses_class,
                    parameters => {
                                   _C          => $C,
                                   _item_id    => $item_id,
                                  },
                   };
    my $uses = ObjFactory->create_instance($C, $of_attrs);

    return $uses->get_USEs($C, $item_id);
}

# ---------------------------------------------------------------------

=item __G_instantiate_mets

Description

=cut

# ---------------------------------------------------------------------
sub __G_instantiate_mets {
    my $self = shift;
    my ($item_id, $USE_conf) = @_;

    my $mets = new Document::METS($item_id, $USE_conf);

    $self->__G_METS($mets);
    return $mets;
}

# ---------------------------------------------------------------------

=item __G_instantiate_extractor

Description

=cut

# ---------------------------------------------------------------------
sub __G_instantiate_extractor {
    my $self = shift;
    my ($item_id, $METS) = @_;

    my $C = $self->__G_context;

    my $document_data_extractor_class = $self->__G_config->get('document_data_extractor_class');
    my $of_attrs = {
                    class_name => $document_data_extractor_class,
                    parameters => {
                                   _C          => $C,
                                   _item_id    => $item_id,
                                   _mets       => $METS,
                                  },
                   };
    my $extractor = ObjFactory->create_instance($C, $of_attrs);
    $self->__G_extractor($extractor);
    return $extractor;
}


# ---------------------------------------------------------------------

=item __G_instantiate_tokenizer

Description

=cut

# ---------------------------------------------------------------------
sub __G_instantiate_tokenizer {
    my $self = shift;
    my ($METS, $extractor, $granularity) = @_;

    my $C = $self->__G_context;

    my $document_data_tokenizer_class = $self->__G_config->get('document_data_tokenizer_class');
    my $of_attrs = {
                    class_name => $document_data_tokenizer_class,
                    parameters => {
                                   _C           => $C,
                                   _granularity => $granularity,
                                   _mets        => $METS,
                                   _extractor   => $extractor,
                                  },
                   };
    my $tokenizer = ObjFactory->create_instance($C, $of_attrs);
    $self->__G_tokenizer($tokenizer);
    return $tokenizer;
}

# ---------------------------------------------------------------------

=item Accessors

Mutators

=cut

# ---------------------------------------------------------------------
sub __G_context {
    my $self = shift;
    return $self->{_context};
}

sub __G_config {
    my $self = shift;
    return $self->{_config};
}

sub __G_item_id {
    my $self = shift;
    return $self->{_item_id};
}

sub __G_generator_type {
    my $self = shift;
    my $type = shift;
    if (defined $type) {
        $self->{_generator_type} = $type;
    }
    return $self->{_generator_type};
}

sub __G_METS {
    my $self = shift;
    my $METS = shift;
    if (defined $METS) {
        $self->{_METS} = $METS;
    }
    return $self->{_METS};
}

sub __G_extractor {
    my $self = shift;
    my $extractor = shift;
    if (defined $extractor) {
        $self->{_extractor} = $extractor;
    }
    return $self->{_extractor};
}

sub __G_tokenizer {
    my $self = shift;
    my $tokenizer = shift;
    if (defined $tokenizer) {
        $self->{_tokenizer} = $tokenizer;
    }
    return $self->{_tokenizer};
}

sub __G_granularity {
    my $self = shift;
    return $self->{_configured_granularity};
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2011-2014 ©, The Regents of The University of Michigan, All Rights Reserved

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
