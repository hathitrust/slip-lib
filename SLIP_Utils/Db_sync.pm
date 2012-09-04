package SLIP_Utils::Db_sync;


=head1 NAME

Db_driver

=head1 DESCRIPTION

This class is a non-OO database interface

=head1 VERSION

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;

# App
use Utils;
use Debug::DUtils;

use Context;
use DbUtils;

my $MYSQL_ZERO_TIMESTAMP = '0000-00-00 00:00:00';
my $vSOLR_ZERO_TIMESTAMP = '00000000';

use constant C_INSERT_SIZE => 100000;

# =====================================================================
# =====================================================================
#
#    Table:   [j_rights] @@
#
# =====================================================================
# =====================================================================

# ---------------------------------------------------------------------

=item Select_j_rights_id



=cut

# ---------------------------------------------------------------------
sub Select_j_rights_id {
    my ($C, $dbh, $nid) = @_;

    my $statement = qq{SELECT count(*) FROM j_rights WHERE nid=?};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $nid);

    my $in_j_rights = $sth->fetchrow_array() || 0;

    return $in_j_rights;
}

# ---------------------------------------------------------------------

=item Select_j_rights_NIN_j_indexed

Description

=cut

# ---------------------------------------------------------------------
sub Select_j_rights_NIN_j_indexed {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT nid FROM j_rights WHERE nid NOT IN (SELECT id FROM j_indexed_temp)};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $ref_to_arr_of_hashref = $sth->fetchall_arrayref({});

    return $ref_to_arr_of_hashref;
}

# ---------------------------------------------------------------------

=item Select_j_indexed_NIN_j_rights

Description

=cut

# ---------------------------------------------------------------------
sub Select_j_indexed_NIN_j_rights {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT id FROM j_indexed_temp WHERE id NOT IN (SELECT nid FROM j_rights)};
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $sth = DbUtils::prep_n_execute($dbh, $statement);

    my $ref_to_arr_of_hashref = $sth->fetchall_arrayref({});

    return $ref_to_arr_of_hashref;
}

# =====================================================================
# =====================================================================
#
#    Table:   [j_indexed] @@
#
# =====================================================================
# =====================================================================


# ---------------------------------------------------------------------

=item init_j_indexed_temp

idempotent

=cut

# ---------------------------------------------------------------------
sub init_j_indexed_temp {
    my ($C, $dbh) = @_;

    my ($statement, $sth);
    
    $statement = qq{DROP TABLE IF EXISTS j_indexed_temp};
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);

    $statement = qq{CREATE TABLE `j_indexed_temp` (`shard` smallint(2) NOT NULL default '0', `id` varchar(32) NOT NULL default '', KEY `id` (`id`)) };
    DEBUG('lsdb', qq{DEBUG: $statement});
    $sth = DbUtils::prep_n_execute($dbh, $statement);
}

# ---------------------------------------------------------------------

=item insert_item_id_j_indexed_temp

idempotent

=cut

# ---------------------------------------------------------------------
sub insert_item_id_j_indexed_temp {
    my ($C, $dbh, $shard, $id_arr_ref) = @_;

    # my $values = join(',', map("($shard, '$_')", @$id_arr_ref));    
    
    my @values = ();
    my @params = ();
    foreach my $id ( @$id_arr_ref ) {
        push @params, $shard, $id;
        push @values, qq{(?, ?)};
    }
    my $values = join(', ', @values);
    
    my $statement = qq{INSERT INTO j_indexed_temp (`shard`, `id`) VALUES $values};

    my $begin = time();
    my $sth = DbUtils::prep_n_execute($dbh, $statement, @params);
    DEBUG('lsdb', qq{DEBUG: $statement});
    my $elapsed = time() - $begin;

    # Let replication catch up
    sleep $elapsed/2;
}

# ---------------------------------------------------------------------

=item Select_error_item_id

idempotent

=cut

# ---------------------------------------------------------------------
sub Select_error_item_id {
    my ($C, $dbh, $run, $id) = @_;

    my $statement = qq{SELECT id FROM j_errors WHERE run=? AND id=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $id);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $error_id = $sth->fetchrow_array() || 0;

    return $error_id;
}

# ---------------------------------------------------------------------

=item Select_duplicate_ids_j_indexed_temp

idempotent

=cut

# ---------------------------------------------------------------------
sub Select_duplicate_ids_j_indexed_temp {
    my ($C, $dbh) = @_;

    my $statement = qq{SELECT id, count(shard) FROM j_indexed_temp GROUP BY id HAVING count(shard) > 1};
    my $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $ref_to_arr_of_hash_ref = $sth->fetchall_arrayref({});

    return $ref_to_arr_of_hash_ref;
}


# ---------------------------------------------------------------------

=item Select_duplicate_ids_j_indexed

idempotent

=cut

# ---------------------------------------------------------------------
sub Select_duplicate_ids_j_indexed {
    my ($C, $dbh, $run) = @_;

    my $statement = qq{SELECT id, count(shard) FROM j_indexed WHERE run=? GROUP BY id HAVING count(shard) > 1};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run);
    DEBUG('lsdb', qq{DEBUG: $statement : run=$run});

    my $ref_to_arr_of_hash_ref = $sth->fetchall_arrayref({});

    return $ref_to_arr_of_hash_ref;
}

# ---------------------------------------------------------------------

=item Select_duplicate_shards_of_id

idempotent

=cut

# ---------------------------------------------------------------------
sub Select_duplicate_shards_of_id {
    my ($C, $dbh, $run, $id) = @_;

    my $statement = qq{SELECT id, shard FROM j_indexed WHERE run=? AND id=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $run, $id);
    DEBUG('lsdb', qq{DEBUG: $statement : run=$run id=$id});

    my $ref_to_arr_of_hashref = $sth->fetchall_arrayref({});

    return $ref_to_arr_of_hashref;
}


# ---------------------------------------------------------------------

=item Select_shards_of_duplicate_id_j_indexed_temp

Description

=cut

# ---------------------------------------------------------------------
sub Select_shards_of_duplicate_id_j_indexed_temp {
    my ($C, $dbh, $id) = @_;

    my $statement = qq{SELECT shard FROM j_indexed_temp WHERE id=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $id);
    DEBUG('lsdb', qq{DEBUG: $statement});

    my $ref_to_arr_of_ary_ref = $sth->fetchall_arrayref([]);

    return $ref_to_arr_of_ary_ref;
}

# ---------------------------------------------------------------------

=item Delete_duplicate_id_j_indexed_temp

Description

=cut

# ---------------------------------------------------------------------
sub Delete_duplicate_id_j_indexed_temp {
    my ($C, $dbh, $id, $shard) = @_;

    my $statement = qq{DELETE FROM j_indexed_temp WHERE id=? AND shard=?};
    my $sth = DbUtils::prep_n_execute($dbh, $statement, $id, $shard);
    DEBUG('lsdb', qq{DEBUG: $statement});
}

# ---------------------------------------------------------------------

=item insert_j_indexed_temp_j_indexed

Description

=cut

# ---------------------------------------------------------------------
sub insert_j_indexed_temp_j_indexed {
    my ($C, $dbh, $run) = @_;

    my ($statement, $sth);

    my $start = 0;
    my $offset = C_INSERT_SIZE;
    my $num_inserted = 0;

    $statement = qq{ALTER TABLE j_indexed DISABLE KEYS};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
    
    do {
        my $begin = time();
        
        $statement = qq{LOCK TABLES j_indexed_temp WRITE, j_indexed WRITE};
        $sth = DbUtils::prep_n_execute($dbh, $statement);
        DEBUG('lsdb', qq{DEBUG: $statement});

        my $SELECT_clause = qq{SELECT $run, `shard`, `id`, '$MYSQL_ZERO_TIMESTAMP', 1 FROM j_indexed_temp LIMIT $start, $offset};
        
        $statement = qq{INSERT INTO j_indexed (`run`, `shard`, `id`, `time`, `indexed_ct`) ($SELECT_clause)};
        $sth = DbUtils::prep_n_execute($dbh, $statement, \$num_inserted);
        DEBUG('lsdb', qq{DEBUG: $statement});

        $statement = qq{UNLOCK TABLES};
        $sth = DbUtils::prep_n_execute($dbh, $statement);
        DEBUG('lsdb', qq{DEBUG: $statement});

        my $elapsed = time() - $begin;
        # Let replication catch up
        sleep $elapsed/2;

        $start += C_INSERT_SIZE;
        
    } until ($num_inserted <= 0);
    
    $statement = qq{ALTER TABLE j_indexed ENABLE KEYS};
    $sth = DbUtils::prep_n_execute($dbh, $statement);
    DEBUG('lsdb', qq{DEBUG: $statement});
}



1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2009-11 ©, The Regents of The University of Michigan, All Rights Reserved

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
