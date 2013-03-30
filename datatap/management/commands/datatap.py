from __future__ import absolute_import

from django.core.management.base import BaseCommand, CommandError, handle_default_options

from datatap.loading import get_datatap_registry, lookup_datatap, autodiscover


class Command(BaseCommand):
    args = '<datataptype> <datatap vargs> [(-- <datataptype> <datatap vargs>), ...]'
    help = 'Chain a series of datataps with the source to the left and the one to write to being the right most. Each datatap invocation is seperated by "--"'
    
    def print_help(self, prog_name, subcommand):
        super(Command, self).print_help(prog_name, subcommand)
        registry = get_datatap_registry()
        print '\nAvailable datataps: %s' % ', '.join(registry.iterkeys())
        for section, datatap in registry.iteritems():
            print '\nSection: %s' % section
            print datatap.__doc__
            for option in datatap.command_option_list:
                print '\t', option
    
    def get_datatap_class(self, name):
        return lookup_datatap(name)
    
    def load_datatap(self, name, arglist, previous=None, for_write=False):
        datatap = self.get_datatap_class(name)
        if for_write:
            return datatap.load_from_command_line_for_write(arglist, instream=previous)
        return datatap.load_from_command_line(arglist, instream=previous)
    
    def run_from_argv(self, argv):
        """
        Set up any environment changes requested (e.g., Python path
        and Django settings), then run this command.

        """
        autodiscover()
        parser = self.create_parser(argv[0], argv[1])
        
        #the first argument without a "-" is the start of source
        #the first argument that is only "--" is the start of destination
        args = argv[2:]
        standard_args = list()
        parts = list()
        
        while args and args[0].startswith('-'):
            standard_args.append(args.pop(0))
        
        current_part = list()
        while args:
            arg = args.pop(0)
            if arg == '--':
                parts.append(current_part)
                current_part = list()
            else:
                current_part.append(arg)
        if current_part:
            parts.append(current_part)
        
        if not parts:
            return self.print_help(argv[0], argv[1])
        
        options, args = parser.parse_args(standard_args)
        handle_default_options(options)
        
        if len(parts) == 1:
            #we defined a source but no where to write
            parts.extend([['JSON'], ['Stream']])
        
        datataps = list()
        datatap = None
        for index, tapentry in enumerate(parts):
            is_last = len(parts) - 1 == index
            datatap = self.load_datatap(tapentry.pop(0), tapentry, previous=datatap, for_write=is_last)
            datataps.append(datatap)
        
        options.__dict__['datataps'] = datataps
        self.execute(*args, **options.__dict__)
    
    def handle(self, *args, **options):
        datataps = options.pop('datataps')
        datataps[-1].commit() #datataps constructed with for_write get a commit method
