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
        if ($lines[$i]=~/\-$/)
        {

            #if its not the last line dehypenate
            # Warning this will mess up words that should be hypenated such as "human-computer interaction"
            if ($i <$#lines)
            {
                #pop off last word in this line and shift off first word in next line
                my (@words) = split(/\s+/,$lines[$i]);
                my $last = pop(@words);
                # check that last word is a reasonable word
                # exclude numbers or words containing numbers
                # and require at least 2 letters
                # should exclude punct but unicode issues and legitimate punct in words
                if ($last !~/\d/ && length($last)>1 )
                #    if ($last !~ $no_numbers && length($last)>1 )
                {
                    #remove ending hyphen
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
                else
                {
                    push (@{$out},$lines[$i]);
                }
            }
            else
            {
                #copy line somewhere or do nothing and at end join @lines with \n
                push (@{$out},$lines[$i]);
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
    #my $time =sprintf("remove garbage: elapsed=%.3f sec ", $elapsed);
    #print "\n$time\n";
}


1;

