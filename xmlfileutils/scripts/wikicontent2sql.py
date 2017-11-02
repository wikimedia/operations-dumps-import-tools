# -*- coding: utf-8 -*-
import os
import re
import sys
import getopt
import urllib
import json
import time
import select
import shutil
from subprocess import Popen, PIPE
from wikifile import File


class WikiContentErr(Exception):
    pass


class Path(object):
    """Make files or paths which contain the lang code,
    project name and date in the filename."""

    def __init__(self, dirname, project=None, lang=None, date=None):
        """Constructor.  Arguments:
        dirname   --  directory name in which files will be located (abs or rel path)
        project   --  name of wiki project type, i.e. 'wikipedia', 'wiktionary', etc.
        lang      --  language code of wiki, i.e. 'en', 'el', etc.
        date      --  datestring in some nice format"""

        self.dir = dirname
        self.project = project
        self.lang = lang
        self.date = date

    def make_path(self, filename):
        """Create a pathname with the filename in the format
        "langcode-project-date-restofname" in the directory
        given to the object when instantiated
        Returns the pathname"""

        return(os.path.join(self.dir, "-".join(filter(None, [self.lang, self.project,
                                                             self.date, filename]))))

    def make_file(self, filename):
        """Create a filename in the format "langcode-project-date-restofname
        Returns the filename"""

        return("-".join(filter(None, [self.lang, self.project, self.date, filename])))


class Command(object):
    """Run a command capturing stdout and optionally displaying stderr
    as it runs"""

    def __init__(self, verbose=False, dryrun=False):
        """Constructor.  Arguments:
        verbose   -- print messages received on stderr from command,
                     also display messages about the command being run
        dryrun    -- don't run the command, show what would have been run"""

        self.dryrun = dryrun
        self.verbose = verbose

    def run_command(self, command):
        """Run a command, capturing output to stdout and stderr,
        optionally displaying stderr output as it is received
        On nonzero return code from the command, displays an error on stderr.
        Returns:  return code from the command, any output produced to stdout
        the output.
        """

        if type(command).__name__ == "list":
            command_string = " ".join(command)
        else:
            command_string = command
        if (self.dryrun or self.verbose):
            if self.dryrun:
                sys.stderr.write("would run %s\n" % command_string)
                return (None, None)
            if self.verbose:
                sys.stderr.write("about to run %s\n" % command_string)

        self._proc = Popen(command, shell=False, stdout=PIPE, stderr=PIPE)

        self._poller = select.poll()
        self._poller.register(self._proc.stdout, select.POLLIN | select.POLLPRI)
        self._poller.register(self._proc.stderr, select.POLLIN | select.POLLPRI)
        self.polledfds = 2  # keep track of active fds
        self.poll_and_wait()

        if self._proc.returncode:
            sys.stderr.write("command '%s failed with return code %s\n"
                             % (command_string, self._proc.returncode))

        # let the caller decide whether to bail or not
        return (self._proc.returncode, self.output)

    def poll_and_wait(self):
        """Collect output to stdout from a process and optionally
        display messages to stderr from the process, until it
       exits or an error is encountered or its stdout and stderr are closed"""

        self.output = ""
        while self.polledfds:  # if there are active fds
            self.poll_once()
        self._proc.wait()

    def poll_once(self):
        """poll process, collect stdout, optionally display stderr,
        waiting up to a second for an event"""

        fds = self._poller.poll(1000)  # once a second is plenty to poll
        if (fds):
            for (fd, event) in fds:
                if event & (select.POLLIN | select.POLLPRI):
                    out = os.read(fd, 1024)
                    if fd == self._proc.stderr.fileno():
                        if (self.verbose):
                            sys.stderr.write(out)
                    else:
                        self.output = self.output + out
                elif (event & (select.POLLHUP | select.POLLNVAL | select.POLLERR)):
                    self._poller.unregister(fd)
                    self.polledfds = self.polledfds - 1  # lower number of active fds


class Converter(object):
    """Convert MediaWiki stub and content XML to page, revision
    and sql tables"""

    def __init__(self, mwxml2sql, output_dir, verbose):
        """Constructor.  Arguments:
        mwxml2sql   -- path to mwxml2sql program which does the conversion
        output_dir   -- output directory into which to place the sql files
        verbose     -- display progress messages about what is being done"""

        self.mwxml2sql = mwxml2sql
        self.output_dir = output_dir
        self.verbose = verbose
        self.runner = Command(verbose=self.verbose)

    def convert_content(self, content_path, stubs_path, mw_version):
        """Run the command to convert XML to sql. Raises excption
        on error from the command.  Arguments:
        content_path  -- path to XML content file (containing full text of pages)
        stubs_path    -- path to XML stubs file corresponding to content file
        mw_version    -- string eg 1.20 representing the version of MediaWiki for
                        which sql tables will be produced
                        Note that for 1.21 and on, the fields page_content_model,
                        rev_content_format, rev_content_model will always be written,
                        even if the user wishes to install into a wiki with
                        $wgContentHandlerUseDB set to false"""

        command = [self.mwxml2sql, '-s', stubs_path, '-t', content_path,
                   '-f', os.path.join(self.output_dir, "filteredsql.gz"), "-m", mw_version]
        if self.verbose:
            command.append('--verbose')
        (result, junk) = self.runner.run_command(command)
        if (self.verbose):
            sys.stderr.write(junk)
        if result:
            raise WikiContentErr("Error trying to convert page content to sql tables\n")


class Stubber(object):
    """Produce MediaWiki XML stub file and a separate file with a list
    of page ids, from a XML page content file
    Note that the XML page content file must hae the bytes attribute
    in the text tag (as dumps produced by Special:Export do)
    and the sha1 tag."""

    def __init__(self, output_dir, verbose):
        """Constructor. Arguments:
        output_dir  --  directory where files will be written
        verbose    --  display progress messages"""

        self.output_dir = output_dir
        self.verbose = verbose
        self.runner = Command(verbose=self.verbose)

    def write_stub_and_page_ids(self, content_path, stubs_path, page_ids_path):
        """Write an XML stub file (omitting text content) and a
        list of page ids, from a MediaWiki XML page content file.
        Arguments:
        content_path  -- path to the XML page content file to read
        stubs_path    -- path to the stubs file to write
        page_ids_path  -- path to the page ids file to write"""

        page_pattern = "^\s*<page>"
        compiled_page_pattern = re.compile(page_pattern)
        revision_pattern = "^\s*<revision>"
        compiled_revision_pattern = re.compile(revision_pattern)
        id_pattern = "^\s*<id>(?P<i>.+)</id>\s*\n$"
        compiled_id_pattern = re.compile(id_pattern)
        text_pattern = '^(?P<s>\s*)<text\s+[^<>/]*bytes="(?P<b>[0-9]+)"'
        compiled_text_pattern = re.compile(text_pattern)

        in_fd = File.open_input(content_path)
        out_fd = File.open_output(stubs_path)
        outpage_id_fd = File.open_output(page_ids_path)
        current_title = None
        current_text_id = None
        page_id = None

        expect_rev_id = False
        expect_page_id = False

        for line in in_fd:
            # FIXME we could jus calculate text len  if the output is missing
            # the bytes attr. (as in dumps not from Special:Export)
            # format in content file:
            #   <text <text xml:space="preserve" bytes="78">
            # format wanted for stubs file:
            #   <text id="11248" bytes="9" />
            if '<' in line:
                result = compiled_text_pattern.match(line)
                if result:
                    line = result.group("s") + '<text id="%s" bytes="%s" />\n' % (
                        current_text_id, result.group("b"))
                    out_fd.write(line)
                    continue
                elif '</text' in line:
                    continue

                result = compiled_page_pattern.match(line)
                if result:
                    expect_page_id = True
                    out_fd.write(line)
                    continue
                result = compiled_revision_pattern.match(line)
                if result:
                    expect_rev_id = True
                    out_fd.write(line)
                    continue
                if expect_page_id:
                    result = compiled_id_pattern.match(line)
                    if result:
                        outpage_id_fd.write("1:%s\n" % result.group("i"))
                        expect_page_id = False
                    out_fd.write(line)
                    continue
                if expect_rev_id:
                    result = compiled_id_pattern.match(line)
                    if result:
                        current_text_id = result.group("i")
                        expect_rev_id = False
                    out_fd.write(line)
                    continue
                out_fd.write(line)
            else:
                continue  # these are lines of text, we can skip them
        in_fd.close()
        out_fd.close()
        outpage_id_fd.close()


class Retriever(object):
    """Retrieve page titles, page content, or namespace information from a wiki using
    the MediaWiki api"""

    def __init__(self, wcr, output_dir, lang_code, project, verbose):
        """Constructor. Arguments:
        output_dir  --  directory where files will be written
        verbose    --  display progress messages"""
        self.wcr = wcr
        self.output_dir = output_dir
        self.lang_code = lang_code
        self.project = project
        self.verbose = verbose
        self.runner = Command(verbose=self.verbose)

    def get_titles_embedded_in(self, template, output_file, escaped=False):
        """Run command to retrieve all page titles using a given template.
        Returns the name of the output file produced.
        On error, raises an exception.
        Arguments:
        template    -- name of the template, includes the 'Template:' string or
                       its localized equivalent on the wiki
        output_file  -- name of file (not full path) for the list of titles
        escaped     -- whether to sqlescape these titles"""

        command = ['python', self.wcr, '-q', 'embeddedin', '-p', template, '-o',
                   self.output_dir, '-O', output_file, '-w',
                   "%s.%s.org" % (self.lang_code, self.project)]

        if escaped:
            command.append('--sqlescaped')
        if self.verbose:
            command.append('--verbose')
        (result, titles_path) = self.runner.run_command(command)
        if result:
            raise WikiContentErr("Error trying to retrieve page titles with embedding\n")
        else:
            titles_path = titles_path.strip()
            return titles_path

    def get_titles_in_namespace(self, ns, output_file, escaped=False):
        """Run command to retrieve all page titles in a given namespace.
        Returns the name of the output file produced.
        On error, raises an exception.
        Arguments:
        ns          -- number of the namespace.
        output_file  -- name of file (not full path) for the list of titles
        escaped     -- whether to sqlescape these titles"""

        command = ['python', self.wcr, '-q', 'namespace', '-p', ns, '-o', self.output_dir,
                   '-O', output_file, '-w', "%s.%s.org" % (self.lang_code, self.project)]
        if escaped:
            command.append('--sqlescaped')
        if self.verbose:
            command.append('--verbose')
        (result, titles_path) = self.runner.run_command(command)
        if result:
            raise WikiContentErr("Error trying to retrieve page titles in namespace\n")
        else:
            titles_path = titles_path.strip()
            return titles_path

    def get_content(self, titles_path, output_file):
        """Run command to retrieve all page content for a list of page titles.
        Returns the name of the output file produced.
        On error, raises an exception.
        Arguments:
        titles_path   -- full path to the list of page titles
        output_file   -- name of file (not full path) for the page content"""

        command = ['python', self.wcr, '-q', 'content', '-p', titles_path, '-o', self.output_dir,
                   "-O", output_file, '-w', "%s.%s.org" % (self.lang_code, self.project)]
        if self.verbose:
            command.append('--verbose')
        (result, content_path) = self.runner.run_command(command)
        if result:
            raise WikiContentErr("Error trying to retrieve content\n")
        else:
            content_path = content_path.strip()
            return content_path

    def get_ns_dict(self):
        """Retrieve namespace informtion for a wiki via the MediaWiki api
        and store in in dict form.
        On error raises an exception."""

        # http://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=namespaces&format=json
        api_url = ("http://" + self.lang_code + "." + self.project + "." + "org/w/api.php" +
                   "?action=query&meta=siteinfo&siprop=namespaces&format=json")
        ns_dict = {}
        ufd = urllib.urlopen(api_url)
        if str(ufd.getcode()).startswith("2"):
            output = ufd.read()
            ufd.close()
            site_info = json.loads(output)
            if 'query' not in site_info or 'namespaces' not in site_info['query']:
                raise WikiContentErr("Error trying to get namespace information from api\n")
            for k in site_info['query']['namespaces'].keys():
                if '*' in site_info['query']['namespaces'][k]:
                    ns_dict[k] = site_info['query']['namespaces'][k]['*'].encode('utf8')
                else:
                    raise WikiContentErr("Error trying to get parse namespace information\n")
            return ns_dict
        else:
            code = ufd.getcode()
            ufd.close()
            raise WikiContentErr("Error trying to retrieve namespace info: %s\n" % code)

        return ns_dict


class Titles(object):
    """Manipulate lists and dicts of wiki page titles"""

    def __init__(self, ns_dict, ns_dict_by_string):
        """Constructor.  Arguments:
        ns_dict          -- dictionary of namespace entries, {num1: name1, num2: name2...}
        ns_dict_by_string  -- dictionary of namespace entries, {name1: num1, name2: num2...}
        Note that the namespace numbers are strings of digits, not ints"""

        self.ns_dict = ns_dict
        self.ns_dict_by_string = ns_dict_by_string

        self.list = []  # list of all titles but templates, with namespace prefix
        self.list_templates = []  # list of all template titles, with namespace prefix
        self.dict = {}  # dict without namespace prefix but values are {ns1: True, ns2: True} etc

    def add_related_titles_from_file(self, filename, related_ns_list, ns_list):
        """Read list of titles from file, for those in one of the
        specified namespaces, convert the title to one from its related
        namespace (i.e. if it was in Category talk, convert to Category,
        if it was in File talk, convert to File, etc.) and add to title
        list and dict. Arguments:
        filename       -- full path to list of titles
        related_ns_list  -- list of namespaces wanted, e.g. ["4", "6", "12"]
        ns_list         -- list of namespaces to convert from, in the same order as the
                          related NsList, e.g. ["5", "7", "13"]"""

        # don't pass templates in here, we do those separately
        # because it could be a huge list and we want the user
        # to be able to save and reuse it
        fd = File.open_input(filename)
        for line in fd:
            line = line.strip()
            sep = line.find(":")
            if sep != -1:
                prefix = line[:sep]
                if prefix in self.ns_dict_by_string:
                    # main, file, category, project talk namespaces
                    if self.ns_dict_by_string[prefix] in related_ns_list:
                        no_prefix_title = line[sep + 1:]
                        # convert to file, category, project namespace
                        related_ns = str(int(self.ns_dict_by_string[prefix]) - 1)
                        if (self.ns_dict[related_ns]):
                            new_title = self.ns_dict[related_ns] + ":" + no_prefix_title
                        else:
                            new_title = no_prefix_title  # main namespace titles
                        self.list.append(new_title)
                        if no_prefix_title in self.dict:
                            self.dict[no_prefix_title][related_ns] = True
                        else:
                            self.dict[no_prefix_title] = {related_ns: True}
                    # file, category, project talk namespaces
                    elif self.ns_dict_by_string[prefix] in ns_list:
                        ns = self.ns_dict_by_string[prefix]
                        no_prefix_title = line[sep + 1:]
                        self.list.append(no_prefix_title)
                        if no_prefix_title in self.dict:
                            self.dict[no_prefix_title][ns] = True
                        else:
                            self.dict[no_prefix_title] = {ns: True}
            elif "0" in ns_list:
                # main namespace, won't be caught above
                self.list.append(line)
                if line in self.dict:
                    self.dict[line]["0"] = True
                else:
                    self.dict[line] = {"0": True}
        fd.close()

    def add_titles_from_file(self, filename, ns):
        """add titles from a file to the title list and dict.
        Note that template titles get added to a different title list
        than the rest, for separate processing
        Arguments:
        filename   -- full path to file containing page titles
        ns         -- number (string of digits) of namespace of page titles to
                      grab from file"""

        fd = File.open_input(filename)
        prefix = self.ns_dict[ns] + ":"
        prefix_len = len(prefix)
        for line in fd:
            if line.startswith(prefix):
                if ns == "10":  # special case bleah
                    self.list_templates.append(line[:-1])  # lose newline
                else:
                    self.list.append(line[:-1])  # lose newline
                no_prefix_title = line[prefix_len:-1]
                if no_prefix_title in self.dict:
                    self.dict[no_prefix_title][ns] = True
                else:
                    self.dict[no_prefix_title] = {ns: True}

    def uniq(self):
        """Remove duplicates from the lists of titles"""

        self.list = list(set(self.list))
        self.list_templates = list(set(self.list_templates))


class Filter(object):
    """Filter dumps of MediaWiki sql tables against a list f pageids, keeping
    only the rows for pageids in the list"""

    def __init__(self, sql_filter, output_dir, verbose):
        """Constructor. Arguments:
        output_dir  --  directory where files will be written
        verbose    --  display progress messages"""
        self.sql_filter = sql_filter
        self.output_dir = output_dir
        self.verbose = verbose
        self.runner = Command(verbose=self.verbose)

    def filter(self, input, output, filter_path):
        """Run command to filter an sql table dump against certain values,
        optinally writing out only certain columns from each row.
        Arguments:
        input           -- full path to sql file for input
        output          -- filename (not full path) to write filtered sql output
        filter_path      -- full path to file containing filter values in form column:value
                           (starting with column 1)"""

        command = [self.sql_filter, '-s', input, '-o', os.path.join(self.output_dir, output)]
        if (filter_path):
            command.extend(['-f', filter_path])
        if self.verbose:
            command.append('--verbose')
            (result, junk) = self.runner.run_command(command)
        if result:
            raise WikiContentErr("Error trying to filter sql tables\n")
        return


def extended_usage():
    """Show extended usage information, explaining how to
    run just certain steps of this program"""

    usage_message = """This script has several steps:
retrievetitles   -- retrieve titles and content for pages from the wiki
converttitles    -- convert titles to non-talk page titles, discard titles not in the
                    main, file, category, project talk namespaces
retrievecontent  -- retrieve titles and content for pages from the wiki
makestubs        -- write a stub xml file and a pageids file from downloaded content
convertxml       -- convert retrieved content to page, revision and text sql tables
filtersql        -- filter previously downloaded sql table dumps against page ids
             of the page content for import
By default each of these will be done in order; to skip one pass the corresponding
no<stepname> e.g. --nofiltersql, --noconvertxml

By providing some or all of the output files to a step you can skip part or all of it.
All output files from the last skipped step must be provided for the program to run.
"Retrievetitles outputfiles:
--titles        path of file containing main content (possibly talk page) titles with the template
--mwtitles      path of file containing all mediawiki namespace titles for the wiki
--mdltitles     path of file containing all module namespace titles for the wiki
--tmpltitles    path of file containing all template namespace titles for the wiki

Converttitles outputfiles:
--titleswithprefix       path of file containing all titles except for templates for import
--tmpltitleswithprefix   path of file containing all template namespace titles for this wiki
                if already retrieved e.g. during a previous run

Retrievecontent outputfiles:
--maincontent   path of file containing all content except templates for import
--tmplcontent   path of file containing all template namespace content for import
--content       path of file containing all content for import

Makestub outputfles:
--stubs         path to file containing stub XML of all content to be imported
--pageids       path to file containing pageids of all content to be imported
"""
    sys.stderr.write(usage_message)
    return


def usage(message=None, extended=None):
    """Show usage and help information. Arguments:
    message   -- message to be shown (e.g. error message) before the help
    extended  -- show exended help as well"""

    if message:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """Usage: python wikicontent2sql.py --template name --sqlfiles pathformat
          [--lang langcode] [--project name] [--batchsize]
          [--output directory] [--auth username:password]
          [--sqlfilter path] [--mwxml2sql] [--wcr path]
          [--verbose] [--help] [--extendedhelp]
"""
    sys.stderr.write(usage_message)

    if (extended):
        usage_message = ("Additional options for skipping various steps of\n" +
                         "the processing are listed below.\n")
        sys.stderr.write(usage_message + "\n")

    usage_message = """This script uses the MediaWiki api and Special:Export to download pages
with a specific template, including category and Wikipedia (or other wiki)
pages, all templates and all system messages, js and css.
For example, pages in a specific wikiproject on some Wikipedias can be
"retrieved by specifying the name of the template included on the articles'
Talk pages; though the articles themselves do not include the template,
"this script will find the titles via the Talk pages which do have the template.
This content is converted into the appropriate sql tables for import.
It is also used to generate a list of page ids against which sql table
dumps of the wiki are filtered for import.
At the end of the process the user should have a directory of sql files for
import into a new wiki, which will contain all content needed except for media.
The script may be run as an an anonymous user on that wiki, or with
authentication.

Options:

--template      name of template for which to download content; this should be
                the name of a template which is included on all articles or their
                talk pages, e.g. 'Template:WikiProject Lepidoptera'
--sqlfiles      path including file format string, to sql files from the most recent
                dump of the wiki from which you are retrieving content
                the format string must contain a '{t}' which will be replaced with
                the appropriate sql table name\n")
                example: dump/enwiki-20130304-{t}.sql.gz would be expanded to the table
                files dump/enwiki-20130304-category.sql.gz,
                dump/enwiki-20130304-pagelinks.sql.gz, etc.
--mwversion     version of MediaWiki such as '1.20' for which sql files should be
                produced from the content; these should match the version from which
                the downloaded sql table dumps were produced
--lang          language code of wiki to download from (en, fr, el etc.), default: en
--project       name project to download from (wikipedia, wiktionary, etc), default:
                wikipedia
--batchsize     number of pages to download at once, if you don't have a lot of memory
                consider specifying 100 or 50 here, default: 500 (the maximum
--output        directory into which to put all resulting files, default: './new_wiki'
--auth          username, optionally a colon and the password, for connecting to
                the wiki; if no password is specified the user will be prompted for one

--sqlfilter     path to sqlfilter program, default: ./sqlfilter
--mwxml2sql     path to mwxml2sql program, default: ./mwxml2sql
--wcr           path to wikicontentretriever script, default: ./wcr

--verbose       print progress messages to stderr
--help          show this usage message
--extendedhelp  show this usage message plus extended help
"""
    sys.stderr.write(usage_message)

    if (extended):
        extended_usage()

    usage_message = """Example usage:
python wikicontent2sql.py --template 'Template:Wikiproject Lepidoptera' \\
      --sqlfiles '/home/ariel/dumps/en-mar-2013/enwiki-20130304-{t}.sql.gz' --verbose"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def init_steps(opt_dict):
    """Initialize vars for running each step, by default we will run them"""


def process_step_option(step_to_skip, odict):
    """Process options that specify skipping a step.
    Arguments:
    step_to_skip -- name of the option without the leading '--'
                  and without the 'no'"""

    if step_to_skip == "retrievetitles":
        odict['retrieve_titles'] = False
    elif step_to_skip == "converttitles":
        odict['convert_titles'] = False
    elif step_to_skip == "retrievecontent":
        odict['retrieve_content'] = False
    elif step_to_skip == "makestubs":
        odict['make_stubs'] = False
    elif step_to_skip == "convertxml":
        odict['convert_xml'] = False
    elif step_to_skip == "filtersql":
        odict['filter_sql'] = False


def process_file_option(file_opt, value, odict):
    """Process options specifying output files to reuse.
    Raiss exception if the file doesn't exist.
    Arguments:
    file_opt  -- the name of the file option without the leading '--'
    value    -- the file path"""

    if not os.path.exists(value):
        usage("specified file %s for %s does not exist or is not a file" % (value, file_opt))

    if file_opt == "titles":
        odict['titles_path'] = value
    elif file_opt == "mwtitles":
        odict['mediawiki_titles_path'] = value
    elif file_opt == "mdltitles":
        odict['module_titles_path'] = value
    elif file_opt == "tmpltitles":
        odict['template_titles_path'] = value
    elif file_opt == "titleswithprefix":
        odict['main_titles_with_prefix_path'] = value
    elif file_opt == "tmpltitleswithprefix":
        odict['tmpl_titles_with_prefix_path'] = value
    elif file_opt == "maincontent":
        odict['main_ccontent_path'] = value
    elif file_opt == "tmplcontent":
        odict['template_content_path'] = value
    elif file_opt == "content":
        odict['content_path'] = value
    elif file_opt == "stubs":
        odict['stubs_path'] = value
    elif file_opt == "pageids":
        odict['page_ids_path'] = value


def do_main():
    o = {}  # stash all opt vars in here

    # init main opt vars
    for opt in ['template', 'sql_files', 'mw_version', 'output_dir', 'username', 'password']:
        o[opt] = None

    o['project'] = "wikipedia"
    o['lang_code'] = "en"
    o['batch_size'] = 500

    cwd = Path(os.getcwd())
    o['sqlfilter'] = cwd.make_path("sqlfilter")
    o['wcr'] = cwd.make_path("wikiretriever.py")
    o['mwxml2sql'] = cwd.make_path("mwxml2sql")

    # init step opt vars
    for opt in ['retrieve_titles', 'convert_titles', 'retrieve_content', 'make_stubs',
                'convert_xml', 'filter_sql']:
        o[opt] = True

    # init file opt vars
    for opt in ['titles_path', 'mediawiki_titles_path', 'module_titles_path', 'template_titles_path',
                'main_titles_with_prefix_path', 'tmpl_titles_with_prefix_path', 'main_content_path',
                'template_content_path', 'content_path', 'stubs_path', 'page_ids_path']:
        o[opt] = None

    verbose = False

    # option handling
    main_options = ["template=", "sqlfiles=", "mwversion=", "lang=",
                    "project=", "batchsize=", "output=", "auth="]
    cmd_options = ["sqlfilter=", "mwxml2sql=", "wcr="]

    steps = ["retrievetitles", "converttitles", "retrievecontent", "makestubs",
             "convertxml", "filtersql"]
    skip_step_flags = ["no" + s for s in steps]

    convert_titles_options = ["titles=", "mwtitles=", "mdltitles=", "tmpltitles="]
    retrieve_content_options = ["titleswithprefix=", "tmpltitleswithprefix="]
    make_stubs_options = ["maincontent=", "tmplcontent=", "content="]
    convert_xml_filter_sql_options = ["stubs=", "pageids="]

    files = [fopt[:-1] for fopt in convert_titles_options + retrieve_content_options +
             make_stubs_options + convert_xml_filter_sql_options]

    misc_flags = ["verbose", "help", "extendedhelp"]

    all_options = (main_options + cmd_options + skip_step_flags + convert_titles_options +
                   retrieve_content_options + make_stubs_options +
                   convert_xml_filter_sql_options + misc_flags)
    try:
        (options, remainder) = getopt.gnu_getopt(sys.argv[1:], "", all_options)
    except getopt.GetoptError as e:
        usage(e.msg)

    for (opt, val) in options:

        # main opts
        if opt == "--template":
            o['template'] = val
        elif opt == "--sqlfiles":
            o['sql_files'] = val
        elif opt == "--mwversion":
            o['mw_version'] = val
        elif opt == "--lang":
            o['lang_code'] = val
        elif opt == "--project":
            o['project'] = val
        elif opt == "--batchsize":
            if not val.isdigit():
                usage("batch size must be a number")
            o['batch_size'] = int(val)
        elif opt == "--output":
            o['output_dir'] = val
        elif opt == "--auth":
            if ':' in val:
                o['username'], o['password'] = val.split(':')
            else:
                o['username'] = val

        # command opts
        elif opt == "--sqlfilter":
            o['sqlfilter'] = val
        elif opt == "--mwxml2sql":
            o['mwxml2sql'] = val
        elif opt == "--wcr":
            o['wcr'] = val

        # step options
        elif opt.startswith("--no"):
            process_step_option(opt[4:], o)

        # file options
        elif opt[2:] in files:
            process_file_option(opt[2:], val, o)

        # misc flags
        elif opt == "--verbose":
            verbose = True
        elif opt == "--help":
            usage("Options help:\n")
        elif opt == "--extendedhelp":
            usage("Options help:\n", True)
        else:
            usage("Unknown option specified: %s" % opt)

    if len(remainder) > 0:
        usage("Unknown option specified: <%s>" % remainder[0])

    # output files will have this date in their names
    date = time.strftime("%Y-%m-%d-%H%M%S", time.gmtime(time.time()))
    out = Path(o['output_dir'], o['lang_code'], o['project'], date)

    # processing begins
    if o['retrieve_titles']:
        if not o['wcr']:
            usage("in retrieve_titles: Missing mandatory option wcr.")
        if not o['template']:
            usage("in retrieve_titles: Missing mandatory option template.")
        if ':' not in o['template']:
            usage("in retrieve_titles: template option should start with 'Template:' " +
                  "or the equivalent in the wiki's language")
        if not o['mw_version']:
            usage("in retrieve_titles: Missing mandatory option mwversion.")

        if (verbose):
            sys.stderr.write("Retrieving page titles from wiki\n")

        r = Retriever(o['wcr'], o['output_dir'], o['lang_code'], o['project'], verbose)
        if not o['titles_path']:
            # get titles corresponding to the template
            o['titles_path'] = r.get_titles_embedded_in(o['template'], out.make_file("main-titles.gz"))
            if verbose:
                sys.stderr.write("main content titles file produced: <%s>\n" % o['titles_path'])

        if not o['mediawiki_titles_path']:
            # get the mediawiki page titles
            o['mediawiki_titles_path'] = r.get_titles_in_namespace("8", out.make_file("mw-titles.gz"))
            if verbose:
                sys.stderr.write("mediawiki titles file produced: <%s>\n" % o['mediawiki_titles_path'])

        if not o['module_titles_path']:
            # get the module (lua) page titles
            o['module_titles_path'] = r.get_titles_in_namespace("828", out.make_file("mod-titles.gz"))
            if verbose:
                sys.stderr.write("modules (lua) titles file produced: <%s>\n" % o['module_titles_path'])

        if not o['template_titles_path']:
            # get the template page titles
            o['template_titles_path'] = r.get_titles_in_namespace("10", out.make_file("tmpl-titles.gz"))
            if verbose:
                sys.stderr.write("templates titles file produced: <%s>\n" % o['template_titles_path'])

        if (verbose):
            sys.stderr.write("Done retrieving page titles from wiki, have " +
                             "%s, %s, %s and %s\n" % (
                                 o['titles_path'], o['mediawiki_titles_path'],
                                 o['module_titles_path'], o['template_titles_path']))

    if o['convert_titles']:
        if (not o['titles_path'] or not o['mediawiki_titles_path'] or not o['module_titles_path'] or
                not o['template_titles_path']):
            usage("Missing mandatory option for skipping previous step.", True)
        if not o['wcr']:
            usage("Missing mandatory option wcr.")

        if (verbose):
            sys.stderr.write("Converting retrieved titles \n")

        r = Retriever(o['wcr'], o['output_dir'], o['lang_code'], o['project'], verbose)

        # get namespaces from the api
        ns_dict = r.get_ns_dict()

        ns_dict_by_string = {}
        for nsnum in ns_dict.keys():
            ns_dict_by_string[ns_dict[nsnum]] = nsnum

        if verbose:
            sys.stderr.write("namespace dicts assembled\n")

        # get list of titles with prefix, not the talk pages but the actual ones,
        # (for use for download) - without dups
        # also create a hash with title, list of ns for this title (it will have
        # at least one entry in the list)
        t = Titles(ns_dict, ns_dict_by_string)

        # check main, file, category, project talk namespaces and convert to
        # main, file, category, project talk namespaces
        t.add_related_titles_from_file(o['titles_path'], ["1", "5", "7", "15"], ["0", "4", "6", "14"])

        if verbose:
            sys.stderr.write("page title hash assembled\n")

        t.add_titles_from_file(o['mediawiki_titles_path'], "8")
        if verbose:
            sys.stderr.write("mediawiki titles added to page title hash\n")

        t.add_titles_from_file(o['module_titles_path'], "828")
        if verbose:
            sys.stderr.write("module titles added to page title hash\n")

        t.add_titles_from_file(o['template_titles_path'], "10")
        if verbose:
            sys.stderr.write("template titles added to page title hash\n")

        t.uniq()

        o['main_titles_with_prefix_path'] = out.make_path("main-titles-with-nsprefix.gz")
        out_fd = File.open_output(o['main_titles_with_prefix_path'])
        for line in t.list:
            out_fd.write(line + "\n")
        out_fd.close()

        o['tmpl_titles_with_prefix_path'] = out.make_path("tmpl-titles-with-nsprefix.gz")
        out_fd = File.open_output(o['tmpl_titles_with_prefix_path'])
        for line in t.list_templates:
            out_fd.write(line + "\n")
        out_fd.close()

        if (verbose):
            sys.stderr.write("Done converting retrieved titles, have %s and %s\n"
                             % (o['main_titles_with_prefix_path'], o['tmpl_titles_with_prefix_path']))

    if o['retrieve_content']:
        if not o['main_titles_with_prefix_path'] or not o['tmpl_titles_with_prefix_path']:
            usage("in retrieve_content: Missing mandatory option for skipping previous step.", True)

        if (verbose):
            sys.stderr.write("Retrieving page content from wiki\n")

        if not o['template_content_path']:
            # filter out the template titles from the main_titles_with_prefix_path file
            # and just download the rest
            o['template_content_path'] = r.get_content(o['tmpl_titles_with_prefix_path'],
                                                       out.make_file("template-content.gz"))
            if verbose:
                sys.stderr.write("content retrieved from template page titles\n")

        if not o['main_content_path']:
            o['main_content_path'] = r.get_content(o['main_titles_with_prefix_path'],
                                                   out.make_file("rest-content.gz"))
            if verbose:
                sys.stderr.write("content retrieved from page titles\n")

        o['content_path'] = out.make_path("content.gz")
        File.combine_xml([o['template_content_path'], o['main_content_path']], o['content_path'])

        if (verbose):
            sys.stderr.write("Done retrieving page content from wiki, have %s, %s and %s\n"
                             % (o['template_content_path'], o['main_content_path'], o['content_path']))

    if o['make_stubs']:
        if not o['content_path']:
            usage("in make_stubs: Missing mandatory option for skipping previous step.", True)

        if (verbose):
            sys.stderr.write("Generating stub XML file and pageids file from downloaded content\n")
        s = Stubber(o['output_dir'], verbose)
        # generate stub XML file for converting sql and list of page ids for filtering sql
        o['stubs_path'] = out.make_path("stubs.gz")
        o['page_ids_path'] = out.make_path("pageids.gz")
        s.write_stub_and_page_ids(o['content_path'], o['stubs_path'], o['page_ids_path'])
        if (verbose):
            sys.stderr.write("Done generating stub XML file and pageids file from " +
                             "downloaded content, have %s and %s\n" % (
                                 o['stubs_path'], o['page_ids_path']))

    if o['convert_xml']:
        if not o['content_path']:
            usage("in convert_xml: Missing mandatory option for skipping previous step.", True)
        if not o['mwxml2sql']:
            usage("in convert_xml: Missing mandatory option mwxml2sql.")

        if (verbose):
            sys.stderr.write("Converting content to page, revision, text tables\n")
        c = Converter(o['mwxml2sql'], o['output_dir'], verbose)
        # convert the content file to page, revision and text tables
        c.convert_content(o['content_path'], o['stubs_path'], o['mw_version'])
        if verbose:
            sys.stderr.write("Done converting content to page, revision, text tables\n")

    if o['filter_sql']:
        if not o['page_ids_path']:
            usage("in filter_sql: Missing mandatory option for skipping previous step.", True)
        if not o['sql_files']:
            usage("in filter_sql: Missing mandatory option sqlfiles.")
        if not o['sqlfilter']:
            usage("in filter_sql: Missing mandatory option sqlfilter.")

        if verbose:
            sys.stderr.write("Filtering sql tables against page ids for import\n")

        f = Filter(o['sqlfilter'], o['output_dir'], verbose)
        # filter all the sql tables (which should be in some nice directory)
        # against the pageids in page_ids_path file
        for table in ["categorylinks", "externallinks", "imagelinks", "interwiki",
                      "iwlinks", "langlinks", "page_props", "page_restrictions",
                      "pagelinks", "protected_titles", "redirect", "templatelinks"]:
            sql_filename = o['sql_files'].format(t=table)
            filtered_filename = os.path.basename(sql_filename)
            f.filter(sql_filename,
                     filtered_filename,
                     o['page_ids_path'])
        if (verbose):
            sys.stderr.write("Done filtering sql tables against page ids for import\n")

        # the one file we can't filter, it's not by pageid as categories might not have pages
        # so we'll have to import it wholesale... (or you can ignore them completely)
        sql_filename = o['sql_files'].format(t='category')
        new_filename = os.path.join(o['output_dir'], os.path.basename(sql_filename))
        if verbose:
            sys.stderr.write("about to copy %s to %s\n" % (sql_filename, new_filename))
        shutil.copyfile(sql_filename, new_filename)

    if (verbose):
        sys.stderr.write("Done!\n")
    sys.exit(0)


if __name__ == "__main__":
    do_main()
