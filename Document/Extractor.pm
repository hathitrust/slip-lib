package Document::Extractor;

=head1 NAME

Document::Extractor

=head1 DESCRIPTION

Extracts files to ramdisk.

This class presents the interface for extraction of text from
repository objects and which process text into a member-data buffer
for iterative access and inclusion in one or more Solr documents for
indexing.

File names come from METS:fileSec/METS:fileGrp[@USE] elements where
USE attribute values are configured in
run-N.conf::document_data_uses_class.


=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;
use warnings;

use Time::HiRes qw( time );

use Utils::Extract;
use Identifier;

use Search::Constants;
use Document::Reporter;


sub new {
    my $class = shift;
    my $param_hashref = shift;

    my $self = {};
    bless $self, $class;

    $self->{_context} = $param_hashref->{_C};
    $self->{_item_id} = $param_hashref->{_item_id};
    $self->{_mets} = $param_hashref->{_mets};

    $self->{_status} = IX_NO_ERROR;

    $self->__initialize;

    return $self;
}

# ---------------------------------------------------------------------

=item PRIVATE: __initialize

Description

=cut

# ---------------------------------------------------------------------
sub __initialize {
    my $self = shift;

    $self->E_extract_files_from_object;
}


# ---------------------------------------------------------------------

=item Accessors

Description

=cut

# ---------------------------------------------------------------------
sub E_get_id {
    my $self = shift;
    return $self->{_item_id};
}

sub E_get_context {
    my $self = shift;
    return $self->{_context};
}

sub E_get_METS {
    my $self = shift;
    return $self->{_mets};
}

# ---------------------------------------------------------------------

=item PUBLIC: E_status

A status other than IX_NO_ERROR is terminal.

=cut

# ---------------------------------------------------------------------
sub E_status {
    my $self = shift;
    my $status = shift;

    if (defined $status) {
        $self->{_status} = $status if ($self->{_status} == IX_NO_ERROR);
    }
    return $self->{_status};
}

# ---------------------------------------------------------------------

=item PUBLIC: E_extraction_dir

Description

=cut

# ---------------------------------------------------------------------
sub E_extraction_dir {
    my $self = shift;
    my $dir = shift;

    if (defined $self->{_extraction_dir}) {
        if (defined $dir) {
            ASSERT(0, qq{extraction_dir attempt to overwrite directory path in Document::Extractor});
        }
        else {
            return $self->{_extraction_dir};
        }
    }
    else {
        ASSERT(0, qq{_extraction_dir not defined in Document::Extractor}) unless (defined $dir);
    }

    return $self->{_extraction_dir} = $dir;
}

# ---------------------------------------------------------------------

=item PUBLIC: E_unlink_extraction_dir

Description

=cut

# ---------------------------------------------------------------------
sub E_unlink_extraction_dir {
    my $self = shift;

    my $dir = $self->E_extraction_dir;

    if (defined $dir) {
        if (-e $dir) {
            system("rm", "-rf", "$dir");
        }
    }
}

# ---------------------------------------------------------------------

=item PUBLIC: E_extract_files_from_object

Extract from the zip an ordered list of files generated by parsing the
USEs requested from the METS.

=cut

# ---------------------------------------------------------------------
sub E_extract_files_from_object {
    my $self = shift;

    my $METS = $self->E_get_METS;
    unless ( $METS->has_files ) {
        return ($self->E_status == IX_NO_ERROR);
    }

    my $file_arr_ref = $METS->filelist;

    my $item_id = $self->E_get_id;
    my $file_sys_location = Identifier::get_item_location($item_id);
    my $stripped_id = Identifier::get_pairtree_id_wo_namespace($item_id);

    # zip file existence
    #
    unless (-e $file_sys_location . qq{/$stripped_id.zip}) {
        report(qq{Extractor: failed: zip missing at $file_sys_location id=$item_id}, 1, 'doc');
        $self->E_status(IX_DATA_FAILURE);
    }

    # unzip
    #
    my $start = time;
    if ($self->E_status == IX_NO_ERROR) {
        my $temp_dir = '';
        eval {
            $temp_dir = Utils::Extract::extract_filelist_to_temp_cache
              (
               $item_id,
               $file_sys_location,
               $file_arr_ref,
               'METSflist',
              );
            chomp($temp_dir);
            $self->E_extraction_dir($temp_dir);
        };
        if ($@) {
            report(qq{Extractor: failed: id=$item_id error:$@}, 1, 'doc');
            $self->E_status(IX_DATA_FAILURE);
             }
        else {
            report( sprintf(qq{Extractor: zipfile extracted to dir="%s" in sec=%.6f}, $temp_dir, (time - $start)), 0, 'doc' );
        }
    }

    # test
    #
    if ($self->E_status == IX_NO_ERROR) {
        my $DIR;
        my $temp_dir = $self->E_extraction_dir;
        if ( opendir($DIR, $temp_dir) ) {
            my $file;
            my $empty = 1;
            while ( defined($file = readdir $DIR) ) {
                next if $file eq '.' || $file eq '..';
                $empty = 0;
                last;
            }
            closedir $DIR;
            # Assumption is that there were non-zero-length files in the zip
            if ($empty) {
                $self->E_status(IX_DATA_FAILURE);
                report(qq{Extractor: No files found in dir="$temp_dir", item_id="$item_id"}, 1, 'doc');
            }
        }
        else {
            my $item_id = $self->E_get_id;
            $self->E_status(IX_DATA_FAILURE);
            report(qq{Extractor: failed to open dir="$temp_dir", item_id="$item_id"}, 1, 'doc');
        }
    }

    return ($self->E_status == IX_NO_ERROR);
}


1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2014 ©, The Regents of The University of Michigan, All Rights Reserved

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
