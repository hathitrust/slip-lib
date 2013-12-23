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
    # add timing here

# is there a faster way ?
    my @lines = split(/\n/,$$text_ref);
    my $out = [];
    
    for ( my $i = 0 ; $i <= $#lines ; $i++ ) 
    {
        # would it be faster to pop off the last word and test it?
        #if the line ends with a hyhpen
        if ($lines[$i]=~/\s+([^\s]+\-)$/)
        {
            #if its not the last line dehypenate
            # this will mess up words that should be hypenated
            if ($i <$#lines)
            {
                #pop off last word in this line and first word in next line
                my (@words) = split(/\s+/,$lines[$i]);
                my $last = pop(@words);
                #do we need to clean this up
                #remove hyphen
                $last =~s/-$//;
                
                my (@next_line_words)=split(/\s+/,$lines[$i+1]);
                my $first = shift (@next_line_words);
                my $fixed = $last . $first;
                my $fixed_line = join (' ', @words);
                $fixed_line .= " " . $fixed;
                push (@{$out},$fixed_line);
                # fix next line 
                $lines[$i+1] = join (' ',@next_line_words);
                
            }
        }
        else
        {
            #copy line somewhere or do nothing and at end join @lines with \n
            push (@{$out},$lines[$i]);
        }
        
    }
    my $fixed_text = join ("\n",@{$out});
          
    #print STDERR "removing garbage With new args\n";
    # replace text ref with fixed
    $$text_ref=$fixed_text;
    
    my $elapsed = (Time::HiRes::time() - $start);   
    #XXX need to figure out how to turn this on with docs-j

    DEBUG('garbage', 
          sprintf("remove garbage: elapsed=%.3f sec ", $elapsed)
          
   );
    #XXX in meantime just turn it on
   # my $time =sprintf("remove garbage: elapsed=%.3f sec ", $elapsed);
   # print "\n$time\n";
}


1;

