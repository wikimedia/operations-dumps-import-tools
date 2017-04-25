# -*- coding: utf-8 -*-
import re
import sys
import getopt
import urllib
import json
import string
from wikifile import File


class WikiContentErr(Exception):
    pass


class NsDict(object):

    def __init__(self, lang_code, project, verbose=False):
        """Constructor. Arguments:
        lang_code   -- language code of project, like en el etc.
        project    -- type of project, like wiktionary, wikipedia, etc.
        verbose    --  display progress messages"""
        self.lang_code = lang_code
        self.project = project
        self.verbose = verbose

    def get_ns_dict(self):
        """Retrieve namespace informtion for a wiki via the MediaWiki api
        and store in in dict form.
        On error raises an exception."""

        # http://en.wikipedia.org/w/api.php?action=query&meta=siteinfo&siprop=namespaces&format=json
        api_url = ("http://" + self.lang_code + "." + self.project + "." +
                   "org/w/api.php" + "?action=query&meta=siteinfo&siprop=namespaces&format=json")
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


class TitlesDict(object):
    def __init__(self, ns_dict_by_string):
        """Constructor. Arguments:
        ns_dict_by_string  -- hash of nstitle => nsnum"""
        self.ns_dict_by_string = ns_dict_by_string

    def get_titles_dict(self, sql_file):
        """Arguments:
        sql_file         -- file containing pageid whitespace nsnum whitespace pagetitle where the title
                           is expected to be sql escaped and can be enclosed with single quotes"""
        fd = File.open_input(sql_file)
        t = {}
        for line in fd:
            (pageid, ns, title) = line.split(' ', 3)
            ns = int(ns)
            if title in t:
                t[title][ns] = pageid
            else:
                t[title] = {ns: pageid}
        return t


class LoggingXml(object):
    def __init__(self, ns_dict_by_string, titles_dict, xml_file, log_out_file, user_out_file):
        """Constructor. Arguments:
        ns_dict_by_string  -- hash of nstitle => nsnum
        titles_dict      -- hash of pagetitle => [pageid, nsnum]
        xml_file         -- path to filename with logging.xml
        log_out_file      -- path to logging output filename"""

        self.ns_dict_by_string = ns_dict_by_string
        self.titles_dict = titles_dict
        self.xml_file = xml_file
        self.log_out_file = log_out_file
        self.user_out_file = user_out_file

        self.logitem_pattern = "^\s*<logitem>\s*\n$"
        self.compiled_logitem_pattern = re.compile(self.logitem_pattern)
        self.id_pattern = "^\s*<id>(?P<i>.+)</id>\s*\n$"
        self.compiled_id_pattern = re.compile(self.id_pattern)
        self.timestamp_pattern = "^\s*<timestamp>(?P<t>.+)</timestamp>\s*\n$"
        self.compiled_timestamp_pattern = re.compile(self.timestamp_pattern)
        self.contributor_pattern = "^\s*<contributor>\n$"
        self.compiled_contributor_pattern = re.compile(self.contributor_pattern)
        self.username_pattern = "^\s*<username>(?P<u>.+)</username>\s*\n$"
        self.compiled_username_pattern = re.compile(self.username_pattern)
        self.end_contributor_pattern = "^\s*</contributor>\n$"
        self.compiled_end_contributor_pattern = re.compile(self.end_contributor_pattern)
        self.comment_pattern = "^\s*<comment>(?P<c>.+)</comment>\s*\n$"
        self.compiled_comment_pattern = re.compile(self.comment_pattern, re.DOTALL)
        self.type_pattern = "^\s*<type>(?P<t>.+)</type>\s*\n$"
        self.compiled_type_pattern = re.compile(self.type_pattern)
        self.action_pattern = "^\s*<action>(?P<a>.+)</action>\s*\n$"
        self.compiled_action_pattern = re.compile(self.action_pattern)
        self.logtitle_pattern = "^\s*<logtitle>(?P<l>.+)</logtitle>\s*\n$"
        self.compiled_logtitle_pattern = re.compile(self.logtitle_pattern)
        self.params_pattern = '^\s*<params\s+xml:space="preserve">(?P<p>.+)</params>\s*\n$'
        self.compiled_params_pattern = re.compile(self.params_pattern, re.DOTALL)
        self.no_params_pattern = '^\s*<params\s+xml:space="preserve" />\s*\n$'
        self.compiled_no_params_pattern = re.compile(self.no_params_pattern)
        self.end_logitem_pattern = "^\s*</logitem>\s*\n$"
        self.compiled_end_logitem_pattern = re.compile(self.end_logitem_pattern)
        self.all = string.maketrans('', '')
        self.nodigs = self.all.translate(self.all, string.digits)

    def skip_header(self, fd):
        """skip over mediawiki site header etc"""
        end_header_pattern = "^\s*</siteinfo>"
        compiled_end_header_pattern = re.compile(end_header_pattern)
        for line in fd:
            if compiled_end_header_pattern.match(line):
                return True
        return False  # never found it

    def un_xml_escape(self, title):
        """Convert XML sanitized title to its regular format.
        This expects no newlines, \r or \t in titles and unescapes
        these characters: & " ' < >
        Arguments:
        title   -- title to be desantized"""

        title = title.replace("&quot;", '"')
        title = title.replace("&lt;", '<')
        title = title.replace("&gt;", '>')
        title = title.replace("&#039;", "'")
        title = title.replace("&amp;", '&')  # this one must be last
        return title

    def sql_escape(self, string, underscores=True):
        """Escape string in preparation for it to be written
        to an sql file for import.
        $wgLegalTitleChars = " %!\"$&'()*,\\-.\\/0-9:;=?@A-Z\\\\^_`a-z~\\x80-\\xFF+";
        Escapes these characters:  ' "  \   by adding leading \
        Note that in the database all titles are stored with underscores instead of spaces
        so replace those; also enclose the title in single quotes
        Arguments:
        string  -- string of to escape"""

        string = string.replace('\\', "\\\\")  # must insert new backslashs after this step
        string = string.replace("\'", "\\'")
        string = string.replace('"', '\\"')
        if underscores:
            string = string.replace(' ', '_')
        return "'" + string + "'"

    # format:
    #  <logitem>
    #    <id>1</id>
    #    <timestamp>2005-02-26T19:37:52Z</timestamp>
    #    <contributor>
    #      <username>Leonariso</username>
    #      <id>3</id>
    #    </contributor>
    #    <comment>content was: ''''Έντονης γραφής κείμενο'''''Πλάγιας γραφής \
    #                 κείμενο''[[Τίτλος σύνδεσης]]== Headline text ==[[...'</comment>
    #    <type>delete</type>
    #    <action>delete</action>
    #    <logtitle>Βικιλεξικό:By topic</logtitle>
    #    <params xml:space="preserve" />
    #  </logitem>
    def do_log_item(self, fd, logout_fd, userout_fd):
        # note that it's possible for a comment or the params to have an embedded newline in them
        # the rest of the fields, no

        line = fd.readline()
        result = self.compiled_logitem_pattern.match(line)
        if not result:
            if "</mediawiki" in line:
                return True  # eof
            else:
                raise WikiContentErr("bad line in logging file, expected <logitem>, found <%s>\n" % line)

        line = fd.readline()
        result = self.compiled_id_pattern.match(line)
        if not result:
            raise WikiContentErr("bad line in logging file, expected <id>, found <%s>\n" % line)
        logid = result.group("i")

        line = fd.readline()
        result = self.compiled_timestamp_pattern.match(line)
        if not result:
            raise WikiContentErr("bad line in logging file, expected <timestamp>, found <%s>\n" % line)
        timestamp = result.group("t")

        line = fd.readline()
        result = self.compiled_contributor_pattern.match(line)
        if not result:
            if "<contributor deleted" not in line:
                raise WikiContentErr("bad line in logging file, expected <contributor>, " +
                                     "found <%s>\n" % line)
            else:
                username = ''
                userid = '0'
        else:
            line = fd.readline()
            result = self.compiled_username_pattern.match(line)
            if not result:
                raise WikiContentErr("bad line in logging file, expected <username>, " +
                                     "found <%s>\n" % line)
            username = result.group("u")

            line = fd.readline()
            result = self.compiled_id_pattern.match(line)
            if not result:
                raise WikiContentErr("bad line in logging file, expected <id>, found <%s>\n" % line)
            userid = result.group("i")

            line = fd.readline()
            result = self.compiled_end_contributor_pattern.match(line)
            if not result:
                raise WikiContentErr("bad line in logging file, expected </contributor>, " +
                                     "found <%s>\n" % line)

        line = fd.readline()
        if "<comment>" not in line:
            # apparently comment is optional. OR it can be 'deleted'. wonderful.
            if "<comment deleted" in line:
                line = fd.readline()
            comment = ''
        else:
            while "</comment>" not in line:
                line = line + fd.readline()
            result = self.compiled_comment_pattern.match(line)
            if not result:
                raise WikiContentErr("bad line in logging file, expected <comment>, found <%s>\n" % line)
            comment = result.group("c")
            line = fd.readline()

        result = self.compiled_type_pattern.match(line)
        if not result:
            raise WikiContentErr("bad line in logging file, expected <type>, found <%s>\n" % line)
        type = result.group("t")

        line = fd.readline()
        result = self.compiled_action_pattern.match(line)
        if not result:
            raise WikiContentErr("bad line in logging file, expected <action>, found <%s>\n" % line)
        action = result.group("a")

        line = fd.readline()
        result = self.compiled_logtitle_pattern.match(line)
        if not result:
            if "<text deleted" in line:
                logtitle = ''
            else:
                raise WikiContentErr("bad line in logging file, expected <logtitle>, " +
                                     "found <%s>\n" % line)
        else:
            logtitle = result.group("l")

        line = fd.readline()
        # do the no params case first
        result = self.compiled_no_params_pattern.match(line)
        if result:
            params = ''
            line = fd.readline()
        else:
            if "<params" in line:
                # ok it has some params, possibly over more than one line
                while "</params>" not in line:
                    line = line + fd.readline()
                result = self.compiled_params_pattern.match(line)
                if not result:
                    raise WikiContentErr("bad line in logging file, expected " +
                                         "<params  xml:space=\"preserve\" />, " +
                                         "found <%s> for %s\n" % (line, logtitle))
                else:
                    params = result.group("p")
                    line = fd.readline()
            else:  # it's some other tag, this elt was missing altogether
                params = ''

        result = self.compiled_end_logitem_pattern.match(line)
        if not result:
            raise WikiContentErr("bad line in logging file, expected </logitem>, found <%s>\n" % line)

        # turn logtitle into pageid, namespace, title-with-no-namespace-prefix
        sep = logtitle.find(":")
        if sep != -1:
            prefix = logtitle[:sep]
            if prefix in self.ns_dict_by_string:
                pagetitle = self.sql_escape(self.un_xml_escape(logtitle[sep + 1:]))
                nsnum = self.ns_dict_by_string[prefix]
                if pagetitle in self.titles_dict:
                    pageid = self.titles_dict[pagetitle][nsnum]
                else:
                    pageid = "NULL"
            else:
                pagetitle = self.sql_escape(self.un_xml_escape(logtitle))
                nsnum = 0
                if pagetitle in self.titles_dict:
                    pageid = self.titles_dict[pagetitle][0]
                else:
                    pageid = "NULL"
        else:
            pagetitle = self.sql_escape(self.un_xml_escape(logtitle))
            nsnum = 0
            if pagetitle in self.titles_dict:
                pageid = self.titles_dict[pagetitle][0]
            else:
                pageid = "NULL"

        comment = self.sql_escape(self.un_xml_escape(comment), False)
        username = self.sql_escape(self.un_xml_escape(username), False)
        params = self.sql_escape(self.un_xml_escape(params), False)

        line = ("INSERT INTO logging ( log_id, log_type, log_action, " +
                "log_timestamp, log_user, log_user_text, log_namespace, " +
                "log_title, log_page, log_comment, log_params, log_deleted ) VALUES ")
        logout_fd.write(unicode(line).encode('utf-8'))
        username = username.decode('utf-8')
        pagetitle = pagetitle.decode('utf-8')
        comment = comment.decode('utf-8')
        params = params.decode('utf-8')
        nsnum = str(nsnum)
        # need 20130425122902, have 2005-07-23T16:43:37Z
        timestamp = timestamp.translate(self.all, self.nodigs)

        line = "( %s );\n" % ", ".join(
            [logid, "'" + type + "'", "'" + action + "'", "'" + timestamp + "'",
             userid, username, nsnum, pagetitle, pageid, comment, params, '0'])
        logout_fd.write(unicode(line).encode('utf-8'))

        if self.user_out_file and userid not in self.user_dict:
            line = ("INSERT INTO user ( user_id, user_name, user_real_name, " +
                    "user_password, user_newpassword, user_newpass_time, " +
                    "user_email, user_touched, user_token, user_email_authenticated, " +
                    "user_email_token, user_email_token_expires, user_registration, " +
                    "user_editcount ) VALUES ")
            userout_fd.write(unicode(line).encode('utf-8'))
            line = "( %s );\n" % ", ".join(
                [userid, username, "''", "''", "''", "NULL", "''",
                 "'20010101000000'", "'6f9b27b447a7fd49bc525e51cc82320b'",
                 "NULL", "NULL", "NULL", "NULL", "0"])
            userout_fd.write(unicode(line).encode('utf-8'))

            self.user_dict[userid] = True

        return False

    def write_sql(self):
        self.user_dict = {1: True}
        fd = File.open_input(self.xml_file)
        logout_fd = File.open_output(self.log_out_file)
        if self.user_out_file:
            userout_fd = File.open_output(self.user_out_file)
        else:
            userout_fd = None
        if not self.skip_header(fd):
            raise WikiContentErr("failed to find end of mediawiki/siteinfo header in xml file\n")
        eof = False
        while not eof:
            eof = self.do_log_item(fd, logout_fd, userout_fd)
        fd.close()
        logout_fd.close()
        if self.user_out_file:
            userout_fd.close()
        return


def usage(message=None):
    """Show usage and help information. Arguments:
    message   -- message to be shown (e.g. error message) before the help"""

    if message:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """Usage: python pageslogging2sql.py --lang langcode --project filename
           --sqlfile filename --logfile filename --logout filename
           [--userout filename]

This script converts a pages-logging.xml file to an sql file suitable
for import into the logging table of a MediaWiki installation.
It may get some things wrong because page ids of page titles can change
over time and this program isn't clever about how it looks that up.
We needed this script for testing logging dumps.  If you need it for
production purposes, better test it carefully.

Options:

--lang         the language code of the project from which the logging
               table was dumped, i.e. en, fr, el etc.
--project      the type of wiki from which the logging table was dumped,
               i.e. wikipedia, wiktionary, wikisource, etc.
--sqlfile      path to an sql fle containing fields pageid namespacenum
               pagetitle space-separated and one triple per line, pagetitle
               should be sql escaped as it would be if written out by
               mysqldump, and it should not contain the namespace
               prefix.
--loggingfile  path to the xml pages-logging file to be converted
--logout       path to the file where the converted sql will be written
--userout      path to file where fake user table sql will be written, if
               specified
               the user table is used when generating xml dumps of the log
               table; any user id found in the logging sql file with non
               null username will be added except for the user with uid 1,
               yes this is a hack
               Make sure that there are no other users except uid 1 already
               in the table and that the username is not in the produced sql
               BEFORE using it for import
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def do_main():
    lang_code = None
    project = None
    sql_file = None
    logging_file = None
    log_out_file = None
    user_out_file = None

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "", ["lang=", "project=", "sqlfile=", "loggingfile=", "logout=", "userout="])
    except:
        usage("Unknown option specified")

    for (opt, val) in options:

        # main opts
        if opt == "--lang":
            lang_code = val
        elif opt == "--project":
            project = val
        elif opt == "--sqlfile":
            sql_file = val
        elif opt == "--loggingfile":
            logging_file = val
        elif opt == "--logout":
            log_out_file = val
        elif opt == "--userout":
            user_out_file = val
        else:
            usage("Unknown option specified: %s" % opt)

    if len(remainder) > 0:
        usage("Unknown option specified: <%s>" % remainder[0])

    if not lang_code:
        usage("Missing mandatory option <%s>" % "lang")
    if not sql_file:
        usage("Missing mandatory option <%s>" % "sqlfile")
    if not project:
        usage("Missing mandatory option <%s>" % "project")
    if not logging_file:
        usage("Missing mandatory option <%s>" % "loggingfile")
    if not log_out_file:
        usage("Missing mandatory option <%s>" % "logout")

    ns = NsDict(lang_code, project)
    ns_dict = ns.get_ns_dict()

    ns_dict_by_string = {}
    for nsnum in ns_dict.keys():
        ns_dict_by_string[ns_dict[nsnum]] = nsnum

    td = TitlesDict(ns_dict_by_string)
    titles_dict = td.get_titles_dict(sql_file)
    lx = LoggingXml(ns_dict_by_string, titles_dict, logging_file, log_out_file, user_out_file)
    lx.write_sql()


if __name__ == "__main__":
    do_main()
