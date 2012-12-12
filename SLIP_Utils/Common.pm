package SLIP_Utils::Common;


=head1 NAME

Utils

=head1 DESCRIPTION

Some useful subs.

=head1 VERSION

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;

# Perl
use Mail::Mailer;
use Encode;

BEGIN {
    use Exporter;
    our @ISA = qw(Exporter);
    our @EXPORT = qw( __output __output_non_interactive __confirm __non_interactive_err_output );
}

# App
use Debug::DUtils;
use Context;
use MdpConfig;
use Search::Constants;
use Search::Site;
use Utils;
use Utils::Time;

# Local
use SLIP_Utils::Processes;
use SLIP_Utils::States;
use SLIP_Utils::Log;
use Db;

# ---------------------------------------------------------------------

=item get_offset_ISO_timestamp

Increment can be negative to advance.

=cut

# ---------------------------------------------------------------------
sub get_offset_ISO_timestamp {
    my ($timestamp, $increment) = @_;
    
    my $unixTime = Utils::Time::unix_Time($timestamp);
    return '00000000'
      if ($unixTime == 0);
    
    my $offset_timestamp =
      Utils::Time::iso_Time('date', $unixTime - $increment*(24*60*60));
    $offset_timestamp =~ s,-,,g;
    
    return $offset_timestamp;
}

# ---------------------------------------------------------------------

=item get_now_ISO_timestamp



=cut

# ---------------------------------------------------------------------
sub get_now_ISO_timestamp {
    my $timestamp = Utils::Time::iso_Time('date');
    $timestamp =~ s,-,,g;
    
    return $timestamp;
}

# ---------------------------------------------------------------------

=item uniq

Description

=cut

# ---------------------------------------------------------------------
sub uniq {
    my $list_ref = shift;
    
    my %hash;
    foreach my $item ( @$list_ref ) {
        $hash{$item}++;
    }
    
    @$list_ref = keys %hash;
}

# ---------------------------------------------------------------------

=item get_shards_from_host

Description

=cut

# ---------------------------------------------------------------------
sub get_shards_from_host {
    my ($C, $host) = @_;
    
    my @shards = $C->get_object('MdpConfig')->get('shards_of_host_' . $host);
    return @shards;
}


# ---------------------------------------------------------------------

=item Solr_host_from_shard

Given a shard number, return short form of host. The short form is
used as a table key, never as an actual host name, e.g. in a url.

=cut

# ---------------------------------------------------------------------
sub Solr_host_from_shard {
    my ($C, $shard) = @_;
    
    my $host = $C->get_object('MdpConfig')->get('host_of_shard_' . $shard);
    return $host;
}


# ---------------------------------------------------------------------

=item get_producer_host_list

Description

=cut

# ---------------------------------------------------------------------
sub get_producer_host_list {
    my $C = shift;
    
    my @host_list = $C->get_object('MdpConfig')->get('producer_hosts');
    return @host_list;
}

# ---------------------------------------------------------------------

=item get_solr_host_list

Description

=cut

# ---------------------------------------------------------------------
sub get_solr_host_list {
    my $C = shift;
    
    my @host_list = $C->get_object('MdpConfig')->get('solr_hosts');
    return @host_list;
}

# ---------------------------------------------------------------------

=item get_common_config_path

Description

=cut

# ---------------------------------------------------------------------
sub get_common_config_path {
    my $app = shift;
    my $conf_file = shift;
    
    my $path;
    if (DEBUG('local')) {
        $path = $ENV{SDRROOT} . "/slip-lib/Config/$conf_file"
    }
    else {
        $path = $ENV{SDRROOT} . "/$app/vendor/slip-lib/lib/Config/$conf_file"
    }
    ASSERT(-e $path, qq{get_common_config_path: $path does not exist});

    return $path;

}

# ---------------------------------------------------------------------

=item get_app_config_path

Description

=cut

# ---------------------------------------------------------------------
sub get_app_config_path {
    my $app = shift;
    my $conf_file = shift;
    
    my $path = $ENV{SDRROOT} . "/$app/lib/Config/$conf_file";
    ASSERT(-e $path, qq{get_app_config_path: $path does not exist});
           
    return $path;
}

# ---------------------------------------------------------------------

=item merge_run_config

Description

=cut

# ---------------------------------------------------------------------
sub merge_run_config {
    my $app = shift;
    my $config = shift;
   
    my $run_number = get_run_number($config);
    my $run_config = gen_run_config($app, $run_number, 1);
    $config->merge($run_config);

    return $config;
}

# ---------------------------------------------------------------------

=item get_run_number

Description

=cut

# ---------------------------------------------------------------------
sub get_run_number {
    my $config = shift;

    # Different sites can use different run numbers (pt/search)
    my $production_site = Search::Site::get_server_site_name();
    my $production_key = 'production_run_configuration' . "_$production_site"; 
    
    my $run_number = defined($ENV{HT_DEV})
      ? $config->get('development_run_configuration')
        : $config->get($production_key);

    return $run_number;
}

# ---------------------------------------------------------------------

=item gen_run_config

A run configuration consists of:

1) uber.conf from mdp-lib or the app submodule vendor/common-lib/lib
(if debug=local)

plus

2) common.conf from slip-lib or the app submodule vendor/slip-lib/lib
(if debug=local)

plus

3) run-<run_number>.conf from the app lib/Config (always)

=cut

# ---------------------------------------------------------------------
sub gen_run_config {
    my $app = shift;
    my $run = shift;
    my $use_empty_uber_config = shift;
    
    ASSERT(defined($app) && defined($run), qq{app or run_number missing.});

    my $uber_configfile;
    if ($use_empty_uber_config) {
        $uber_configfile = Utils::get_uber_config_path($app, 1)
    }
    else {
        $uber_configfile = Utils::get_uber_config_path($app);
    }
    ASSERT(-e $uber_configfile, qq{get_uber_config_path <- gen_run_config: $uber_configfile does not exist});

    my $common_configfile = get_common_config_path($app, 'common.conf');
    my $app_configfile = get_app_config_path($app, qq{run-$run.conf});

    my $config = new MdpConfig($uber_configfile, $common_configfile, $app_configfile);
    
    return $config;
}


# ---------------------------------------------------------------------

=item merge_stats

Description

=cut

# ---------------------------------------------------------------------
sub merge_stats {
    my ($C, $merged_stats_hashref, $stats_hashref) = @_;

    foreach my $key (keys %$stats_hashref) {
        foreach my $subkey (keys %{ $stats_hashref->{$key} }) {
            $merged_stats_hashref->{$key}{$subkey} += $stats_hashref->{$key}{$subkey};
        }
    }
}

# ---------------------------------------------------------------------

=item stage_rc_to_string

Description

=cut

# ---------------------------------------------------------------------
sub stage_rc_to_string {
    my $rc = shift;
    
    my $s;
    
    if    ($rc == $SLIP_Utils::States::RC_OK)                 { $s = 'all is well!';                     }
    elsif ($rc == $SLIP_Utils::States::RC_DATABASE_CONNECT)   { $s = 'database connect error';           }
    elsif ($rc == $SLIP_Utils::States::RC_MAX_ERRORS)         { $s = 'max errors reached';               }
    elsif ($rc == $SLIP_Utils::States::RC_CRITICAL_ERROR)     { $s = 'critical error';                   }
    elsif ($rc == $SLIP_Utils::States::RC_BAD_ARGS)           { $s = 'bad argument to script';           }
    elsif ($rc == $SLIP_Utils::States::RC_SOLR_ERROR)         { $s = 'solr error';                       }
    elsif ($rc == $SLIP_Utils::States::RC_DRIVER_DISABLED)    { $s = 'driver-j disabled';                }
    elsif ($rc == $SLIP_Utils::States::RC_DRIVER_NO_SEM)      { $s = 'driver-j cannot get semaphore';    }
    elsif ($rc == $SLIP_Utils::States::RC_RIGHTS_NO_SEM)      { $s = 'rights-j cannot get semaphore';    }
    elsif ($rc == $SLIP_Utils::States::RC_DRIVER_WRONG_STAGE) { $s = 'driver-j not at expected stage';   }
    elsif ($rc == $SLIP_Utils::States::RC_DRIVER_BUSY_FILE)   { $s = 'driver-j encountered busy file';   }
    elsif ($rc == $SLIP_Utils::States::RC_DRIVER_FLAGS_DIR)   { $s = 'flags dir unavailable';            }
    elsif ($rc == $SLIP_Utils::States::RC_CHILD_ERROR)        { $s = '(fore|back)ground process error';  }
    elsif ($rc == $SLIP_Utils::States::RC_ERROR_SHARD_STATES) { $s = 'shard error state exists';         }
    elsif ($rc == $SLIP_Utils::States::RC_NO_INDEX_DIR)       { $s = 'bad index directory';              }
    elsif ($rc == $SLIP_Utils::States::RC_BAD_SCHED_FILE)     { $s = 'schedule file i/o error';          }
    elsif ($rc == $SLIP_Utils::States::RC_TOMCAT_STOP_FAIL)   { $s = 'tomcat stop failure';              }
    elsif ($rc == $SLIP_Utils::States::RC_TOMCAT_START_FAIL)  { $s = 'tomcat start failure';             }
    else                                                      { $s = 'unknown rc';                       }
    
    return $s;
}

# ---------------------------------------------------------------------

=item IXconstant2string

Description

=cut

# ---------------------------------------------------------------------
sub IXconstant2string {
    my $const = shift;
    
    my $s;
    
    if    ($const == IX_INDEXED)          { $s = 'IX_INDEXED';          }
    elsif ($const == IX_INDEX_FAILURE)    { $s = 'IX_INDEX_FAILURE';    }
    elsif ($const == IX_INDEX_TIMEOUT)    { $s = 'IX_INDEX_TIMEOUT';    }
    elsif ($const == IX_SERVER_GONE)      { $s = 'IX_SERVER_GONE';      }
    elsif ($const == IX_ALREADY_FAILED)   { $s = 'IX_ALREADY_FAILED';   }
    elsif ($const == IX_DATA_FAILURE)     { $s = 'IX_DATA_FAILURE';     }
    elsif ($const == IX_METADATA_FAILURE) { $s = 'IX_METADATA_FAILURE'; }
    elsif ($const == IX_CRITICAL_FAILURE) { $s = 'IX_CRITICAL_FAILURE'; }
    elsif ($const == IX_NO_INDEXER_AVAIL) { $s = 'IX_NO_INDEXER_AVAIL'; }
    else                                  { $s = 'IX_UNKNOWN_ERROR';    }
    
    return $s;
}

# ---------------------------------------------------------------------

=item __non_interactive_err_output

When scripts are children of cron, print to STDERR.

=cut

# ---------------------------------------------------------------------
sub __non_interactive_err_output {
    my ($rc, $msg) = @_;
    
    return unless ($rc > 0);
    return if ($ENV{'TERM'});
    print STDERR qq{$msg};
}

# ---------------------------------------------------------------------

=item __output

Description

=cut

# ---------------------------------------------------------------------
sub __output {
    my $msg = shift;
    
    return if (! $ENV{'TERM'});
    print STDOUT qq{$msg};
}

# ---------------------------------------------------------------------

=item __output_non_interactive

Description

=cut

# ---------------------------------------------------------------------
sub __output_non_interactive {
    my $msg = shift;
    
    return if ($ENV{'TERM'});
    print STDOUT qq{$msg};
}

# ---------------------------------------------------------------------

=item __confirm

Description

=cut

# ---------------------------------------------------------------------
sub __confirm {
    my $s = shift;
    
    return if (! $ENV{'TERM'});
    
    __output "$s";
    my $pass_1 = <STDIN>;
    exit if ($pass_1 !~ m,y,i);
}

# ---------------------------------------------------------------------

=item Log_database_connection_error

Description

=cut

# ---------------------------------------------------------------------
sub Log_database_connection_error {
    my ($C, $script, $error) = @_;

    my $host = `hostname`;
    my $time = Utils::Time::iso_Time();
    
    my $s = qq{script=$script, host=$host, error="$error" at: } . $time;
    SLIP_Utils::Log::this_string($C, $s, 'connect_logfile', '___RUN___', 'connect');

    Send_email($C, 'report', "[SLIP] connect error: $time $host $script", $s);
}

# ---------------------------------------------------------------------

=item Send_email

Description

=cut

# ---------------------------------------------------------------------
sub Send_email
{
    my ($C, $key, $email_subject, $msg) = @_;
    
    my $config = $C->get_object('MdpConfig');
    my $email_to_addr = $config->get($key . '_to_email_address');
    my $email_from_addr = $config->get($key . '_from_email_address');
    
    my $email_body = $msg;
    
    my $mailer = new Mail::Mailer('sendmail');
    $mailer->open({
                   'To'      => $email_to_addr,
                   'From'    => $email_from_addr,
                   'Subject' => $email_subject,
                  });
    print $mailer($email_body);
    $mailer->close;
}


# ---------------------------------------------------------------------

=item ___num2utf8

Description

=cut

# ---------------------------------------------------------------------
sub ___num2utf8
{
    my ( $t ) = @_;
    my ( $trail, $firstbits, @result );

    if    ($t<0x00000080) { $firstbits=0x00; $trail=0; }
    elsif ($t<0x00000800) { $firstbits=0xC0; $trail=1; }
    elsif ($t<0x00010000) { $firstbits=0xE0; $trail=2; }
    elsif ($t<0x00200000) { $firstbits=0xF0; $trail=3; }
    elsif ($t<0x04000000) { $firstbits=0xF8; $trail=4; }
    elsif ($t<0x80000000) { $firstbits=0xFC; $trail=5; }
    else {
        ASSERT(0, qq{Too large scalar value="$t": cannot be converted to UTF-8.});
    }
    for (1 .. $trail)
    {
        unshift (@result, ($t & 0x3F) | 0x80);
        $t >>= 6;         # slight danger of non-portability
    }
    unshift (@result, $t | $firstbits);
    pack ("C*", @result);
}

# ---------------------------------------------------------------------

=item ___Google_NCR_to_UTF8

Description

=cut

# ---------------------------------------------------------------------
sub ___Google_NCR_to_UTF8 {
    my $sRef = shift;
    $$sRef =~ s,\#{([0-9]+)},___num2utf8($1),ges;
}


# ---------------------------------------------------------------------

=item clean_xml

The input ref may be invalid UTF-8 because of the forgiving read.  Try
to fix it

As of this date Fri Oct 5 14:36:30 2007 there are 2 problems with the
Google OCR:

1) Single byte control characters like \x01 and \x03 which are legal
UTF-8 but illegal in XML

2) Invalid UTF-8 encoding sequences like \xFF

The following eliminates ranges of invalid control characters (1)
while preserving TAB=U+0009, NEWLINE=U+000A and CARRIAGE
RETURN=U+000D. To handle (2) we eliminate all byte values with high
bit set.  We try to test for this so we do not destroy valid UTF-8
sequences.

=cut

# ---------------------------------------------------------------------
sub clean_xml {
    my $s_ref = shift;

    $$s_ref = Encode::encode_utf8($$s_ref);
    ___Google_NCR_to_UTF8($s_ref);
    $$s_ref = Encode::decode_utf8($$s_ref);

    if (! Encode::is_utf8($$s_ref, 1))
    {
        $$s_ref = Encode::encode_utf8($$s_ref);
        $$s_ref =~ s,[\200-\377]+,,gs;
        $$s_ref = Encode::decode_utf8($$s_ref);
    }
    # Decoding changes invalid UTF-8 bytes to the Unicode REPLACEMENT
    # CHARACTER U+FFFD.  Replace that char with a SPACE for nicer
    # viewing.
    $$s_ref =~ s,[\x{FFFD}]+, ,gs;

    # At some time after Wed Aug 5 16:32:34 2009, Google will begin
    # CJK segmenting using 0x200B ZERO WIDTH SPACE instead of 0x0020
    # SPACE.  To maintain compatibility change ZERO WIDTH SPACE to
    # SPACE until we have a Solr query segmenter.
    $$s_ref =~ s,[\x{200B}]+, ,gs;

    # Kill characters that are invalid in XML data. Valid XML
    # characters and ranges are:

    #  (c == 0x9) || (c == 0xA) || (c == 0xD)
    #             || ((c >= 0x20) && (c <= 0xD7FF))
    #             || ((c >= 0xE000) && (c <= 0xFFFD))
    #             || ((c >= 0x10000) && (c <= 0x10FFFF))

    # Note that since we have valid Unicode UTF-8 encoded at this
    # point we don't need to remove any other code
    # points. \x{D800}-\x{DFFF} compose surrogate pairs in UTF-16
    # and the rest are not valid Unicode code points.
    $$s_ref =~ s,[\000-\010\013-\014\016-\037]+, ,gs;

    # Protect against non-XML character data like "<"
    Utils::map_chars_to_cers($s_ref, [q{"}, q{'}], 1);
}

# ---------------------------------------------------------------------

=item normalize_solr_date

From mysql we expect e.g. 1999-01-20.  The format Solr needs is of the
form 1995-12-31T23:59:59Z, and is a more restricted form of the
canonical representation of dateTime
http://www.w3.org/TR/xmlschema-2/#dateTime The trailing "Z" designates
UTC time and is mandatory.  Optional fractional seconds are allowed:
1995-12-31T23:59:59.999Z All other components are mandatory.

=cut

# ---------------------------------------------------------------------
sub normalize_solr_date {
    my $date_in = shift;
    return $date_in . 'T00:00:00Z';
}


1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2009 ©, The Regents of The University of Michigan, All Rights Reserved

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
