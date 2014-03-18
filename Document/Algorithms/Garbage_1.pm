package Document::Algorithms::Garbage_1;

use Debug::DUtils;

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
}


# ---------------------------------------------------------------------

=item remove_garbage_ocr

Description

=cut

# ---------------------------------------------------------------------
sub execute {
    my $self;
    my $args_hashref = shift;

    DEBUG('doc', qq{Document::Algorithms::Garbage_1::execute});
}

1;

