package Document::Algorithms::Garbage_2;

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
sub remove_garbage {
    my $self = shift;
    my $C = shift;
    my $text_ref = shift;
    
    my $start = Time::HiRes::time();    
    #
    #     fastest and dumbest
    #     #$$text_ref=~s/\-\n//g;
    #
    #    Add basic sanity check.  Word must have at least 2 alpha 
    #    characters at end and second word must have at least 2 alpha characters at beginning
    #    This filters out numbers like foo-33 and stuff like "--"
    #    This one is about 20 ms at the 99th percentile vs 10ms for alg above

    $$text_ref=~s/([a-zA-z][a-zA-Z])\-\n([a-zA-z][a-zA-Z])/$1$2/g;
    
    my $elapsed = (Time::HiRes::time() - $start);   
    #XXX need to figure out how to turn this on with docs-j

    DEBUG('garbage', 
          sprintf("remove garbage: elapsed=%.3f sec ", $elapsed)
          
   );
    #XXX in meantime just turn it on
    #my $time =sprintf("remove garbage: elapsed=%.3f sec ", $elapsed);
    #print "\n$time\n";
}


1;

