package Document::Generator;

=head1 NAME

Document::Generator;

=head1 DESCRIPTION

This class defines a generator for subclasses of Document.

It expects a document id and a Document subclass name. It will
instantiate the subclass, gather data for the given document
id, and generate one or more Solr/Lucene documents as defined by the
subclass.



=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use Context;
use ObjFactory;


sub new {
    my $class = shift;

    my $self = {};
    bless $self, $class;
    $self->_initialize(@_);

    return $self;
}


# ---------------------------------------------------------------------

=item _initialize

Initialize object.

=cut

# ---------------------------------------------------------------------
sub _initialize {
    my $self = shift;
    my $C = shift;
    my $document_id = shift;
    my $document_subclass = shift;


    my $of = new ObjFactory;
    my %of_attrs = (
                    'class_name' => $document_subclass,
                    'parameters' => {
                                     'C'  => $C,
                                     'id' => $document_id,
                                    },
                   );
    my $doc = $of->create_instance($C, \%of_attrs);

    $self->{document_subclass} = $document_subclass;
    $self->{state_variable} = $document_subclass->get_state_variable();
}

sub __get_genv {
    my $self = shift;
    my $key = shift;
    return $self->{$key};
}


# ---------------------------------------------------------------------

=item generate_next

Description

=cut

# ---------------------------------------------------------------------
sub generate_next {
    my $self = shift;
    my $C = shift;

    my $doc = $self->__get_genv('document_subclass');

    my $stateV = $self->__get_genv('state_variable');
    my $state = $stateV->();

    my $Solr_Document;
    if (defined($state)) {
        $doc->build_document($C, $state);
        $Solr_Document = $doc->get_document_content($C);
    }
    else {
        $doc->finish_document($C);
    }

    return $Solr_Document;
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2011 ©, The Regents of The University of Michigan, All Rights Reserved

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


