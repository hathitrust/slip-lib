package SLIP_Utils::YamlConfig;


=head1 NAME

Yaml;

=head1 DESCRIPTION

This class reads yaml files from disk or url

=head1 VERSION

=head1 SYNOPSIS

my $yaml = new SLIP_Utils::YamlConfig()
my $data =$yaml->get_data($key)
XXX currently specialized just for ns2label hash !

=head1 METHODS

=over 8

=cut

use strict;
use YAML::Any;
use LWP;



sub new {
    my $class = shift;

    my $self = {};
    bless $self, $class;
    $self->_initialize(@_);

    return $self;
}
# ---------------------------------------------------------------------
sub _initialize
{
    my $self = shift;
    my ($url,$filename) = @_;
    my $ns2label = $self->__get_ns2label_hash($url);
    if ($ns2label=~/ERROR/)
    {
        die " need better error handling $ns2label $!";
    }
    else{
        $self->set_ns2label($ns2label);    
    }
}

# ---------------------------------------------------------------------
sub set_ns2label
{
    my $self = shift;
    my $ns2label  = shift;
    $self->{'ns2label'}=$ns2label;
    
}

sub get_ns2label
{
    my $self = shift;
    return $self->{'ns2label'};
}


=head

Get mapping of name space to htsource/Original Location  display label
Mapping maintained by LIS at http://mirlyn-aleph.lib.umich.edu/namespacemap.yaml

XXX Do we want to cache a copy of the yaml file in /tmp in case mirlyn is down and use that or
do we die/bail if we can't get it?  Actually, perhaps we should store the file as a blog in mysql since indexers could be anywhere?

RETURNS: "ERROR: "errormessage" or hashref

=cut


# ---------------------------------------------------------------------
sub __get_ns2label_hash
{
    my $self = shift;
    my $url = shift;
    my $filename = shift;
    
    my $ns2label= {};
    my $yaml = $self->__get_yaml_from_url($url);
    #   my $yaml = $self->__get_yaml_from_file($filename);
    if ($yaml =~/ERROR/)
    {
        #if url fetching failed $yaml will have http error message
        return $yaml;
    }

    my $parsed = Load $yaml;

    # create hash key = ns value= display label
    foreach my $key (keys %{$parsed})
    {
        my $value=$parsed->{$key}->{desc};
        $value=~s/^\s+//g;
        $value=~s/\s+$//g;
        $ns2label->{$key}=$value;
    }
    return $ns2label;
}

# ---------------------------------------------------------------------
sub __get_yaml_from_url
{
    my $self = shift;
    my $url = shift;
    my $yaml;
    my $errormsg = "get_yaml_from_url failed";
    
    my $ua = LWP::UserAgent->new;
    $ua->agent("ls yamlgetter");
    my $req = HTTP::Request->new(     
                              GET => "$url"
                             );
    $req->header('Accept' => 'text/html');
    my $res = $ua->request($req);

    
    if ($res->is_success) 
    {
        return $res->content;
    }
    else 
    {
        $errormsg = 'ERROR: ' . $res->status_line ;
    }
    return $errormsg;
}
# ---------------------------------------------------------------------
sub __get_yaml_from_file
{
    my $self = shift;
    my $yamlfile = shift;
    my $yaml ="ERROR $yamlfile file doe not exist";
    
    if ( -e $yamlfile)
    {
        $yaml = do 
        {
            local $/;
            open (my ($fh), '<', $yamlfile) or die $!;
            <$fh>;
        };
    }
    return $yaml;
}
# ---------------------------------------------------------------------
1;

