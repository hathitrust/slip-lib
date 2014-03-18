package Document::Conf::uses_1;

# //METS:fileGrp[@USE] values where we want the METS object provide
# filenames of text to be indexed.

use strict;
use warnings;

my $USE_CONFIGURATION =
  {
   volume   => {
                volume => [ 'ocr' ],
                TEI    => [ 'ocr' ],
               },
   article =>  {
                JATS => [ 'article' ],
               },
   audio   =>  {
                audio => [ 'notes' ],
               },
  };


sub new {
    my $class = shift;

    my $self = {};
    bless $self, $class;

    $self->{_uses} = $USE_CONFIGURATION;
    
    return $self;
}

sub get_USEs {
    my $self = shift;
    my ($C, $item_id) = @_;
    return $self->{_uses};
}

1;



