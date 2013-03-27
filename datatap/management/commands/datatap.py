from __future__ import absolute_import

from django.core.management.base import BaseCommand, CommandError, handle_default_options

from datatap.loading import lookup_datatap, autodiscover


class Command(BaseCommand):
    args = '<source> <source vargs> [-- <destination> <destination vargs>]'
    help = 'Use datataps to export data'
    
    
    def get_datatap_class(self, name):
        return lookup_datatap(name)
    
    def load_datatap(self, name, arglist):
        datatap = self.get_datatap_class(name)
        return datatap.load_from_command_line(arglist)
    
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
        source = None
        source_args = list()
        destination = None
        destination_args = list()
        
        while args and args[0].startswith('-'):
            standard_args.append(args.pop(0))
        
        source = args.pop(0)
        
        try:
            position = args.index('--')
        except ValueError:
            source_args = args
        else:
            source_args = args[:position]
            destination = args[position+1]
            destination_args = args[position+2:]
        
        if not source:
            return self.print_help(argv[0], argv[1])
        
        options, args = parser.parse_args(standard_args)
        handle_default_options(options)
        
        source_tap = self.load_datatap(source, source_args)
        
        if not destination:
            source_tap.open('r')
            DestinationTap = source_tap.detect_originating_datatap()
            if DestinationTap is not None:
                destination = DestinationTap.get_ident()
            else:
                destination = 'JSONStream'
            destination_args = []
        
        destination_tap = self.load_datatap(destination, destination_args)
        
        options.__dict__['source_tap'] = source_tap
        options.__dict__['destination_tap'] = destination_tap
        
        self.execute(*args, **options.__dict__)
    
    def handle(self, *args, **options):
        source_tap = options.pop('source_tap')
        destination_tap = options.pop('destination_tap')
        source_tap.open('r', for_datatap=destination_tap)
        destination_tap.open('w', for_datatap=source_tap)
        
        source_tap.dump(destination_tap)
        source_tap.close()
        destination_tap.close()
