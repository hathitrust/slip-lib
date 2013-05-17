package SLIP_Utils::States;


=head1 NAME

States

=head1 DESCRIPTION

This package defines:

1) values that represent the possible states an item in slip_queue table
can be in.

2) error return codes


=head1 VERSION

=head1 SYNOPSIS

use States;

=over 8

=cut

# Queue states
our $Q_AVAILABLE  = 2;
our $Q_PROCESSING = 3;

# Return codes check these
our $RC_OK                 = 0;
our $RC_DATABASE_CONNECT   = 1;
our $RC_MAX_ERRORS         = 2;
our $RC_CRITICAL_ERROR     = 3;
our $RC_BAD_ARGS           = 4;
our $RC_SOLR_ERROR         = 5;
our $RC_DRIVER_DISABLED    = 6;
our $RC_DRIVER_NO_SEM      = 7;
our $RC_DRIVER_WRONG_STAGE = 8;
our $RC_CHILD_ERROR        = 9;
our $RC_ERROR_SHARD_STATES = 10;
our $RC_NO_INDEX_DIR       = 11;
our $RC_DRIVER_BUSY_FILE   = 12;
our $RC_DRIVER_FLAGS_DIR   = 13;
our $RC_RIGHTS_NO_SEM      = 14;
our $RC_BAD_SCHED_FILE     = 15;
our $RC_TOMCAT_STOP_FAIL   = 16;
our $RC_TOMCAT_START_FAIL  = 17;
our $RC_WRONG_NUM_SEGS     = 18;
our $RC_STOPSLIP_DRIVER    = 19;

# Driver stages check these
our $St_Undefined       = 'Undefined';
our $St_Build_Resume    = 'Build_Resume';
our $St_Build_Wait      = 'Build_Wait';
our $St_Building        = 'Building';
our $St_Optimizing      = 'Optimizing';
our $St_Checking        = 'Checking';
our $St_Driver_ERROR    = 'Driver_ERROR';

# Shard states
our $Sht_No_Build_Error = 0;
our $Sht_Build_Error    = 2;

our $Sht_Not_Optimized  = 0;
our $Sht_Optimized      = 1;
our $Sht_Optimize_Error = 2;

our $Sht_Not_Checked    = 0;
our $Sht_Checked        = 1;
our $Sht_Check_Error    = 2;

# Snapshot release states
our $Srl_Rel_Error      = 0;
our $Srl_Rel_Ok         = 1;
our $Srl_Rel_Pending    = 2;
our $Srl_Rel_Wrong_Host = 3;

# ---------------------------------------------------------------------
1;


__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2008-10 ©, The Regents of The University of Michigan, All Rights Reserved

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
