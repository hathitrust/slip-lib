package Search::Query;


=head1 NAME

Search::Query ((Q)

=head1 DESCRIPTION

This class represents the form of the Solr query as based on the
user's query string.

=head1 SYNOPSIS

my $Q = new Search::Query($query_string, [[1,234,4,456,563456,43563,3456345634]]);

$Q->get_Solr_query_string();

=head1 METHODS

=over 8

=cut

use Utils;
use Utils::Time;
use Utils::Logger;
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

Initialize Search::Query object.

=cut

# ---------------------------------------------------------------------
sub _initialize {
    my $self = shift;
    my $C = shift;
    my $query_string = shift;
    my $internal = shift;

    $self->{'query_string'} = $query_string;

    $self->AFTER_Query_initialize($C, $internal, @_);
}


# ---------------------------------------------------------------------

=item get_response_writer_version

Description

=cut

# ---------------------------------------------------------------------
sub get_Solr_XmlResponseWriter_version {
    return '2.2';
}

# ---------------------------------------------------------------------

=item AFTER_Query_initialize

Subclass Initialize Search::Query object.

=cut

# ---------------------------------------------------------------------
sub AFTER_Query_initialize {
    ASSERT(0, qq{AFTER_Query_initialize() in __PACKAGE__ is pure virtual});
}


# ---------------------------------------------------------------------

=item get_Solr_query_string

Description

=cut

# ---------------------------------------------------------------------
sub get_Solr_query_string {
    ASSERT(0, qq{get_Solr_query_string() in __PACKAGE__ is pure virtual});
}



# ---------------------------------------------------------------------

=item get_query_string

Description

=cut

# ---------------------------------------------------------------------
sub get_query_string {
    my $self = shift;
    return $self->{'query_string'};
}


# ---------------------------------------------------------------------

=item get_processed_query_string

Description

=cut

# ---------------------------------------------------------------------
sub get_processed_query_string {
    my $self = shift;
    return $self->{'processedquerystring'};
}

# ---------------------------------------------------------------------

=item set_processed_query_string

Description

=cut

# ---------------------------------------------------------------------
sub set_processed_query_string {
    my $self = shift;
    my $s = shift;
    $self->{'processedquerystring'} = $s;
}

# ---------------------------------------------------------------------

=item get_Solr_no_fulltext_filter_query

Construct a filter query informed by the authentication and holdings
environment for 'search-only'.  This is the negation of
get_Solr_fulltext_filter_query() 

=cut

# ---------------------------------------------------------------------
sub get_Solr_no_fulltext_filter_query {
    my $self = shift;
    my $C = shift;
    
    my $fulltext_FQ_arg = $self->__HELPER_get_Solr_fulltext_filter_query_arg($C);
    my $full_no_fulltext_FQ_string = 'fq=(NOT+' . $fulltext_FQ_arg . ')';

    return $full_no_fulltext_FQ_string;
}

# ---------------------------------------------------------------------

=item get_Solr_fulltext_filter_query

Construct a full filter query (fq) informed by the
authentication and holdings environment.

=cut

# ---------------------------------------------------------------------
sub get_Solr_fulltext_filter_query {
    my $self = shift;
    my $C = shift;

    my $full_fulltext_FQ_string = 'fq=' . $self->__HELPER_get_Solr_fulltext_filter_query_arg($C);

    DEBUG('query', qq{<font color="blue">FQ: </font>$full_fulltext_FQ_string});
    return $full_fulltext_FQ_string;
}


# ---------------------------------------------------------------------

=item __HELPER_get_Solr_fulltext_filter_query_arg

Construct the clause to a filter query (fq) informed by the
authentication, institution and holdings environment. Construction
varies:

There are two cases that turn on attr:3 (OP)

1) The SSD user query only requires holdings because OP implies IC so
their query is e.g. 

fq=((rights:1+OR+rights:7+OR+...)+OR+(ht_heldby:inst+AND+attr:3)+OR+(ht_heldby:inst+AND+attr:4))

2) HT affiliates or in-library users require holdings AND brittle so their query is e.g.

fq=((rights:1+OR+rights:7+OR+...)+OR+(ht_heldby_brlm:inst+AND+attr:3)+OR+(ht_heldby:inst+AND+attr:4))

=cut

# ---------------------------------------------------------------------
sub __HELPER_get_Solr_fulltext_filter_query_arg {
    my $self = shift;
    my $C = shift;
    
    # These are the attrs, for this users authorization type
    # (e.g. SSD, HT affiliate, in-library), geo location and
    # institution that equate to the 'allow' status, i.e. fulltext.
    # This code takes into account whether the attr requires
    # institution to hold the volumes, whether the holding have to be
    # brittle, and qualifies accordingly.
    my $fulltext_attr_list_ref = Access::Rights::get_fulltext_attr_list($C);
    
    my @holdings_qualified_attr_list = ();
    my @unqualified_attr_list = @$fulltext_attr_list_ref;
    
    # Remove any attributes that must be qualified by holdings of the
    # user's institution
    foreach my $fulltext_attr (@$fulltext_attr_list_ref) {
        if (grep(/^$fulltext_attr$/, @RightsGlobals::g_access_requires_holdings_attribute_values)) {
            push(@holdings_qualified_attr_list, $fulltext_attr);
            @unqualified_attr_list = grep(! /^$fulltext_attr$/, @unqualified_attr_list);
        }
    }
    
    my $unqualified_string = '';
    if (scalar @unqualified_attr_list) {
        $unqualified_string = 
          '(' . join('+OR+', map { 'rights:' . $_ } @unqualified_attr_list) . ')';
    }
    
    # Now qualify by holdings.  If there is no institution, there
    # cannot be a clause qualified by institution holdings at all.
    my $holdings_qualified_string = '';

    # @OPB
    my $inst = $C->get_object('Auth')->get_institution_code($C, 'mapped');
    if ($inst) {
        my @qualified_OR_clauses = ();
        my $access_type = Access::Rights::get_access_type_determination($C);

        foreach my $attr (@holdings_qualified_attr_list) {
            if (($access_type ne $RightsGlobals::SSD_USER)
                &&
                ($attr eq $RightsGlobals::g_access_requires_brittle_holdings_attribute_value)) {
                push(@qualified_OR_clauses, qq{(ht_heldby_brlm:$inst+AND+rights:$attr)});
            }
            else {
                push(@qualified_OR_clauses, qq{(ht_heldby:$inst+AND+rights:$attr)});
            }
        }
        $holdings_qualified_string = (scalar @qualified_OR_clauses) ? '(' . join('+OR+', @qualified_OR_clauses) . ')' : '';
    }

    my $fulltext_FQ_string = '(' . $unqualified_string . ($holdings_qualified_string ? '+OR+' . $holdings_qualified_string : '') . ')';
    
    return $fulltext_FQ_string;
}


# ---------------------------------------------------------------------

=item set_well_formed

Description

=cut

# ---------------------------------------------------------------------
sub set_well_formed {
    my $self = shift;
    my $well_formed = shift;
    $self->{'wellformedformula'} = $well_formed;
}

# ---------------------------------------------------------------------

=item well_formed

Description

=cut

# ---------------------------------------------------------------------
sub well_formed {
    my $self = shift;
    return $self->{'wellformedformula'};
}

# ---------------------------------------------------------------------

=item valid_boolean_expression

Description

=cut

# ---------------------------------------------------------------------
sub parse_was_valid_boolean_expression {
    my $self = shift;
    return $self->{'wasvalidbooleanexpression'};
}

# ---------------------------------------------------------------------

=item set_valid_boolean_expression

Description

=cut

# ---------------------------------------------------------------------
sub set_was_valid_boolean_expression {
    my $self = shift;
    $self->{'wasvalidbooleanexpression'} = 1;
}

# ---------------------------------------------------------------------

=item get_processed_user_query_string

Perform some common ops on the user query string to make it work with
Solr.  Support for very simple useer entered queries

1) '"' chars must be balanced or the query is treated as without '"'

2) leading "+" and "-" are supported

3) stemming via "*" is supported

NOTE: as of Tue Nov 17 13:26:52 2009, stemming is causing timeouts in
the 12/4 configuration.  Not supported

NOTE: as of Fri Dec 4 12:21:44 2009 AND|OR and "(", ")" balanced
parentheses are supported. However, if a boolean query si not a
well-formed formula, all operators and parens are removed. This means
the query will devolve to the default AND query.

4) All other punctuation is _removed_

5) added code to allow query string as an argument for advanced search
processing (tbw)

=cut

# ---------------------------------------------------------------------
sub get_processed_user_query_string {
    my $self = shift;
    my $query_string = shift;

    my $user_query_string;

    if (defined ($query_string))
    {
        $user_query_string= $query_string;
    }
    else
    {
        $user_query_string = $self->get_query_string();
    }
    
    $user_query_string = $self->filter_lucene_chars($user_query_string);
    
    # At this point double quotes are balanced. Lower-case AND|OR
    
    # embedded in phrases and replace phrase-embedded parentheses with
    # spaces.
    my @tokens = parse_preprocess($user_query_string);

    # If user is not attempting a boolean query skip the parse here
    my $valid = 1;
    if (grep(/^(AND|OR|\(|\))$/, @tokens)) {
        # Attempt to parse the query as a boolean expression.
        $valid = valid_boolean_expression(@tokens);
        if ($valid) {
            $self->set_was_valid_boolean_expression();
        }
    }

    if (! $valid) {
        $self->set_well_formed(0);

        # The parse fails. remove parentheses and lower case _all_
        # occurrences of AND|OR and compose a default AND query.
        my @final_tokens = ();
        foreach my $t (@tokens) {
            my $f = get_final_token($t);
            push(@final_tokens, $f) if ($f);
        }
        $user_query_string = join(' ', @final_tokens);
    }
    else {
        $self->set_well_formed(1);
    }

    $self->set_processed_query_string($user_query_string);
    DEBUG('parse', sub {return qq{Final processed user query: $user_query_string}});

    return $user_query_string;
}

sub get_final_token {
    my $s = shift;
    if ($s =~ m,([\(\)]|^AND$|^OR$),) {
        return '';
    }
    return $s;
}

# ---------------------------------------------------------------------
sub filter_lucene_chars {
    my $self = shift;
    my $user_query_string = shift;
 
    Utils::remap_cers_to_chars(\$user_query_string);
 
    # Replace sequences of 2 or more double-quotes (") with a single
    # double-quote
    $user_query_string =~ s,["]+,",g;
    # Remove all double-quote (") if any are unbalanced
    my $num_chars = $user_query_string =~ tr/"//;
    $user_query_string =~ s,", ,g
        if ($num_chars % 2);
    
    # stemming
    # stemming    # Asterisk (*) must follow wordchars and be followed by whitespace
    # stemming    # or EOL.  In other words, a free standing, leading or embedded *
    # stemming    # is ignored
    # stemming    while ($user_query_string =~ s,(\w)\*+(\w),$1 $2,){}
    # stemming    while ($user_query_string =~ s,(^|\s)\*+,$1,){}
    # stemming
    # stemming    # If any asterisk (*) remains, we have right stemming. Solr right
    # stemming    # stemmed query strings have to be lowercase
    # stemming    if ($user_query_string =~ m,\*,) {
    # stemming        $user_query_string = lc($user_query_string);
    # stemming    }
    # stemming
    
    # Temporarily disable stemming
    $user_query_string =~ s,\*, ,g;
    
    # Preserve only word-leading plus and minus (+) (-)
    while ($user_query_string =~ s,(^|\W)[-+]+(\W|$),$1 $2,){}
    
    # Note: Lucene special chars are: + - && || ! ( ) { } [ ] ^ " ~ * ? : \
    
    # Note: See "stemming note" above. Except for + - * " ( ) special
    # chars are removed to prevent query parsing errors.  Other
    # punctuation, (e.g. ' . , @) is more likely to appear in normal
    # text) is left in place, (e.g. 1,000) because the
    # PunctFilterFactory will tokenize the punctuated term as a single
    # token whereas if we remove the punctuation, the query parser
    # will see 2 or more operands and perform a boolean AND which is
    # slow.
    $user_query_string =~ s/[!:?\[\]\\^{~}]/ /g;
    $user_query_string =~ s/\|\|/ /g;
    $user_query_string =~ s/\&\&/ /g;
    
    # Remove leading and trailing whitespace
    Utils::trim_spaces(\$user_query_string);
    
    # convert the xml special characters " < > & back to character entities
    Utils::map_chars_to_cers(\$user_query_string, [q{"}, q{'}], 1);
    
    return $user_query_string;
}

# ---------------------------------------------------------------------

=item log_query

Description

=cut

# ---------------------------------------------------------------------
sub log_query {
    my $self = shift;
    my $C = shift;
    my $searcher = shift;
    my $rs = shift;
    my $query_dir_part = shift;

    # Log
    my $ipaddr = $ENV{'REMOTE_ADDR'};
    my $Qtime = $rs->get_query_time();
    my $num_found = $rs->get_num_found();
    my $config = $C->get_object('MdpConfig');
    my $Solr_url = $searcher->get_engine_uri() . '?' . $self->get_Solr_query_string($C);
    $Solr_url =~ s, ,+,g;
    # add cgi params for better tracking
    my $tempcgi = $C->get_object('CGI');
    my $appURL=$tempcgi->url(-query=>1);
    
    my $session_id = $C->get_object('Session')->get_session_id();

    my $log_string = qq{$ipaddr $session_id $$ }
        . Utils::Time::iso_Time('time')
            . qq{ qtime=$Qtime numfound=$num_found url=$Solr_url cgi=$appURL };

    Utils::Logger::__Log_string($C, $log_string,
                                     'query_logfile', '___QUERY___', $query_dir_part);
}


# ---------------------------------------------------------------------

=item Boolean Expression Validation Routines

expression ::= term [ OR term ]
term       ::= factor [ AND factor ]
factor     ::= literal | ( expression )

=cut

# ---------------------------------------------------------------------
my %Reserved =
    (
     'lparen' => '(',
     'rparen' => ')',
     'and'    => 'AND',
     'or'     => 'OR',
    );

my @Tokens = ();
my %ParsedToken = ();

sub suppress_boolean_in_phrase {
    my $s = shift;
    $s =~ s,([\(\)]), ,g;
    $s =~ s,AND,and,;
    $s =~ s,OR,or,;
    return qq{$s };
}

sub parse_preprocess {
    my $query = shift;

    my @token_array = ();

    # Set parens off from operands for parsing ease
    $query =~ s,\(, \( ,g;
    $query =~ s,\), \) ,g;

    Utils::trim_spaces(\$query);
    my @PreTokens = split(/\s+/, $query);

    # Handle AND, OR, RPAREN, LPAREN within double quotes, i.e. within a
    # phrase. We assume balanced quotes at this point in the processing.
    while (1) {
        my $t = shift @PreTokens;
        last if (! $t);
        if ($t =~ m,^",) {
            my $quote;
            $quote .= suppress_boolean_in_phrase($t);
            while (($t !~ m,"$,) && ($t)) {
                $t =  shift @PreTokens;
                $quote .= suppress_boolean_in_phrase($t);
            }
            push(@token_array, $quote);
        }
        else {
            push(@token_array, $t);
        }
    }
    return @token_array;
}

sub IsReserved {
    my $tok = shift;
    if (grep(/^\Q$tok\E$/, values(%Reserved))) {
        DEBUG('parse', sub {return qq{Reserved: $tok\n};});
        return 1;
    }
    return 0;
}


sub HandleReserved {
    my $s = shift;
    my $rc = 1;
    if ($s eq $Reserved{'lparen'}) {
        %ParsedToken = ( 'type'  => 'LPAREN',
                         'token' => 'LPAREN' );
    }
    elsif ($s eq $Reserved{'rparen'}) {
        %ParsedToken = ( 'type'  => 'RPAREN',
                         'token' => 'RPAREN' );
    }
    elsif ($s eq $Reserved{'and'}) {
        %ParsedToken = ( 'type'  => 'AND',
                         'token' => 'AND' );
    }
    elsif ($s eq $Reserved{'or'}) {
        %ParsedToken = ( 'type'  => 'OR',
                         'token' => 'OR' );
    }
    else {
        $rc = 0;
    }
    return $rc;
}

sub IsEmpty() {
    return (scalar @Tokens == 0);
}


sub GetToken {
    my $token = '';

    if (IsEmpty()) {
        %ParsedToken = ( 'type' => 'ENDTOK',
                         'token' => 'ENDTOK' );
        DEBUG('parse', sub {return q{Get: [} . $ParsedToken{'token'} . q{] } . join(' ', @Tokens)});
        return;
    }

    while (1) {
        if (IsEmpty()) {
            return;
        }

        my $tok = shift @Tokens;
        DEBUG('parse', sub {return qq{Get: token="$tok"};});

        if ($token) {
            die unless(IsReserved($tok));
        }

        if (IsReserved($tok)) {
            if ($token) {
                unshift(@Tokens, $tok);
                return;
            }
            else {
                HandleReserved($tok);
                return;
            }
        }

        $token .= $tok;

        %ParsedToken = ( 'type' => 'LITERAL',
                         'token' => $token );
        DEBUG('parse', sub {return q{Get: [} . $ParsedToken{'token'} . q{] } . join(' ', @Tokens)});
    }
}


sub Accept {
    my $s = shift;

    if ($ParsedToken{'type'} eq $s) {
        DEBUG('parse', sub {return qq{Accept: } . $ParsedToken{'token'}});
        GetToken();
        return 1;
    }
    return 0;
}

sub Expect {
    my $s = shift;
    DEBUG('parse', sub {return qq{Expect: } . $ParsedToken{'token'}});
    if (Accept($s)) {
        return 1;
    }
    die "Expect: unexpected symbol=$s\n";
}

sub Term {
    DEBUG('parse', sub {qq{Term}});
    Factor();
    while (Accept('AND')) {
        Factor();
    }
}

sub Factor {
    DEBUG('parse', sub {return qq{Factor}});
    if (Accept('LITERAL')) {
    }
    elsif (Accept('LPAREN')) {
        Expression();
        Expect('RPAREN');
    }
    else {
        die "Factor: syntax error\n";
    }
}


sub Expression {
    DEBUG('parse', sub {return qq{Expression\n}});
    Term();
    while ($ParsedToken{'type'} eq 'OR') {
        GetToken();
        Term();
    }
}

sub valid_boolean_expression {
    my @toks = @_;
    @Tokens = @toks;

    eval {
        GetToken();
        Expression();
        Expect('ENDTOK');
    };
    if ($@) {
        DEBUG('parse', qq{valid_boolean_expression: die: $@});
        return 0;
    }
    DEBUG('parse', sub {return qq{Valid boolean expression}});
    return 1;
}




















1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2007 ©, The Regents of The University of Michigan, All Rights Reserved

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
