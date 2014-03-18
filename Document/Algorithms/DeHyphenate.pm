package Document::Algorithms::DeHyphenate;

use Time::HiRes qw( time );
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

=item execute

 fastest and dumbest
 $$text_ref =~ s/\-\n//g;

 Add basic sanity check.  Word must have at least 2 alpha characters
 at end and second word must have at least 2 alpha characters at
 beginning.  This filters out numbers like foo-33 and stuff like "--"
 This one is about 20 ms at the 99th percentile vs 10ms for alg above

=cut

# ---------------------------------------------------------------------
sub execute {
    my $self = shift;
    my ($C, $text_ref) = @_;

    my $start = time;

    $$text_ref =~ s/([a-zA-z][a-zA-Z])\-\n([a-zA-z][a-zA-Z])/$1$2/g;

    my $elapsed = time - $start;

    DEBUG('doc', sprintf("ALG: dehyphenate: elapsed=%.6f sec ", $elapsed));
}


1;

