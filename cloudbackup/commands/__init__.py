# -*- coding: utf-8 -*-

import collections
from importlib import import_module
import json
import logging
from optparse import make_option, OptionParser
import os
import sys
import warnings


class CommandError(Exception):
    pass


class BaseCommand(object):

    # Metadata about this command.
    option_list = (
        make_option('--config',
                    dest='config', action="store", type="string",
                    help='The file path of a config file.',
                    default=os.path.expanduser("~/.cloudbackup.json")
                    ),
    )
    help = ''
    args = ''

    def __init__(self):
        logger = logging.getLogger("zfsglacier")
        handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logger.setLevel(logging.DEBUG)

    def usage(self, subcommand):
        """
        Return a brief description of how to use this command, by
        default from the attribute ``self.help``.

        """
        usage = '%%prog %s [options] %s' % (subcommand, self.args)
        if self.help:
            return '%s\n\n%s' % (usage, self.help)
        else:
            return usage

    def create_parser(self, prog_name, subcommand):
        """
        Create and return the ``OptionParser`` which will be used to
        parse the arguments to this command.

        """
        return OptionParser(prog=prog_name,
                            usage=self.usage(subcommand),
                            option_list=self.option_list)

    def print_help(self, prog_name, subcommand):
        """
        Print the help message for this command, derived from
        ``self.usage()``.

        """
        parser = self.create_parser(prog_name, subcommand)
        parser.print_help()

    def run_from_argv(self, argv):
        parser = self.create_parser(argv[0], argv[1])
        options, args = parser.parse_args(argv[2:])

        try:
            self.execute(*args, **options.__dict__)
        except Exception as e:
            if not isinstance(e, CommandError):
                raise

            # self.stderr is not guaranteed to be set here
            sys.stderr.write('%s: %s\n' % (e.__class__.__name__, e))
            sys.exit(1)

    def execute(self, *args, **options):
        """
        Try to execute this command, performing system checks if needed (as
        controlled by attributes ``self.requires_system_checks`` and
        ``self.requires_model_validation``, except if force-skipped).
        """
        self.stdout = sys.stdout
        self.stderr = sys.stderr

        self.config = {}
        if options["config"]:
            self.config = json.load(open(options["config"]))

        output = self.handle(*args, **options)
        if output:
            self.stdout.write(output)

    def handle(self, *args, **options):
        """
        The actual logic of the command. Subclasses must implement
        this method.

        """
        raise NotImplementedError('subclasses of BaseCommand must provide a handle() method')


def get_commands():
    command_dir = os.path.dirname(os.path.abspath(__file__))
    return [f[:-3] for f in os.listdir(command_dir) if not f.startswith('_') and f.endswith('.py')]

def fetch_command(subcommand):
    module = import_module("cloudbackup.commands.%s" % (subcommand,))
    return module.Command()

def help_text(prog_name):
    usage = [
        "",
        "Type '%s help <subcommand>' for help on a specific subcommand." % prog_name,
        "",
        "Available subcommands:",
    ]

    for subcommand in get_commands():
        command = fetch_command(subcommand)
        usage.append("    %s : %s" % (subcommand, command.help))

    return '\n'.join(usage)

def execute_from_command_line(argv=None):
    if not argv:
        argv = sys.argv
    prog_name = os.path.basename(argv[0])

    try:
        subcommand = argv[1]
    except IndexError:
        subcommand = "help"

    if subcommand == 'help':
        if len(argv) <= 2:
            print(help_text(prog_name))
        else:
            fetch_command(argv[2]).print_help(prog_name, argv[2])
        print("")

    elif subcommand in get_commands():
        fetch_command(subcommand).run_from_argv(argv)


if __name__ == "__main__":
    execute_from_command_line()

