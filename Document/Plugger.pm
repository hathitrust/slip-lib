=head1 NAME

Plugger.pm

=head1 DESCRIPTION

This is not a package.  It is use'd into the namespace of a client
package to act as common code over the Document subclasses that have
plugins configured.

=head1 SYNOPSIS

Coding example

=head1 METHODS

=over 8

=cut

use strict;
use warnings;

use MdpConfig;
use Context;

# ---------------------------------------------------------------------

=item initialize_plugins

Use Class::MOP (perl 5 Meta Object Protocol) to introspect the methods
on this object created by its plugin(s).

Refer to Document/Plugins.txt

=cut

# ---------------------------------------------------------------------
sub initialize_plugins {
    my $self = shift;
    my $C = shift;

    $self->{_plugin_method_names} = [];

    my $config = $C->get_object('MdpConfig');
    my $class = ref $self;
    my $plugin_key = 'plugin_for_' . $class;

    if ($config->has($plugin_key)) {
        my @plugin_names = split(/,/, $config->get($plugin_key));
        my @plugins = map { $class . '::' . $_ } @plugin_names;

        if (scalar @plugins) {
            require Class::MOP;

            foreach my $pin (@plugins) {
                eval "require $pin";
                ASSERT(!$@, qq{Error compiling Plugin name="$pin": $@});
            }

            my $metaclass = Class::MOP::Class->initialize($class);
            my @plugin_method_names;
            my @all_methods = ( $metaclass->get_all_methods );
            foreach my $meth (@all_methods) {
                my $method_name = $meth->fully_qualified_name;
                push(@plugin_method_names, $method_name) if (grep(/PLG_/, $method_name));
            }
            $self->{_plugin_method_names} = [ @plugin_method_names ];
        }
    }
}

1;

__END__

=head1 AUTHOR

Phillip Farber, University of Michigan, pfarber@umich.edu

=head1 COPYRIGHT

Copyright 2013-14 ©, The Regents of The University of Michigan, All Rights Reserved

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
