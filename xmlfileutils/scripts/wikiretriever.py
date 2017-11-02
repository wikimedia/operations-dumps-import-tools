# -*- coding: utf-8 -*-
import os
import re
import sys
import getopt
import httplib
import urllib
import time
import calendar
import getpass
from xml.etree import ElementTree as ElementTree
from wikifile import File


class WikiRetrieveErr(Exception):
    pass


class WikiConnection(object):
    """Base class for a connection to a MediaWiki wiki, holding authentication
    credentials, wiki name, type of api request, etc.
    This class is responsible for performing the actual GET request and for checking
    the response, for logging in, and for checking maxlag.
    All connections are https but with no certificate checks."""

    def __init__(self, wikiname, username, password, verbose):
        """Constructor. Arguments:
        wikiname        -- host name of the wiki, e.g. en.wikipedia.org
        username        -- username with which to authenticate to the wiki, if any;
                           if not supplied, requests are made anonymously (via the user IP)
        password        -- password for auth to the wiki, if any; if username is
                           supplied and password is not, the user will be
                           prompted to supply one
        verbose         -- if set, display various progress messages on stderr"""

        self.wikiname = wikiname
        self.username = username
        self.password = password
        self.verbose = verbose
        self.logged_in = False
        self.user_agent = "wikicontentretriever.py/0.1"
        self.queryapi_url_base = "/w/api.php?action=query&format=xml&maxlag=5"
        self.error_pattern = re.compile("<error code=\"([^\"]+)\"")
        self.lagged = False
        self.cookies = []

    def geturl(self, url, method="GET", params=None):
        """Request a specific url and return the contents. On error
        writes an error message to stderr and returns None. Arguments:
        url      -- everything that follows the hostname in a normal url, eg.
                    /w/api.php?action=query&list=allpages&ns=0
        methd    -- GET, PUT, POST etc.
        params   -- dict of name/value query pairs for POST requests"""

        self.lagged = False
        if params:
            params = urllib.urlencode(params)
        try:
            http_conn = httplib.HTTPSConnection(self.wikiname)
            http_conn.putrequest(method, url, skip_accept_encoding=True)
            http_conn.putheader("Accept", "text/html")
            http_conn.putheader("Accept", "text/plain")
            http_conn.putheader("Cookie", "; ".join(self.cookies))
            http_conn.putheader("User-Agent", self.user_agent)
            if params:
                http_conn.putheader("Content-Length", len(params))
                http_conn.putheader("Content-Type", "application/x-www-form-urlencoded")

            http_conn.endheaders()
            if params:
                http_conn.send(params)
            http_result = http_conn.getresponse()
            if http_result.status != 200:
                if http_result.status == 503:
                    contents = http_result.read()
                    http_conn.close()
                    if contents.find("seconds lagged"):
                        if self.verbose:
                            sys.stderr.write(contents)
                        self.lagged = True
                        return contents
                sys.stderr.write("status %s, reason %s\n" % (http_result.status, http_result.reason))
                raise httplib.HTTPException
        except Exception:
            sys.stderr.write("failed to retrieve output from %s\n" % url)
            return None

        contents = http_result.read()
        http_conn.close()

        # format <error code="maxlag"
        result = self.error_pattern.search(contents)
        if result:
            if result.group(1) == "maxlag":
                self.lagged = True
            else:
                sys.stderr.write("Error '%s' encountered\n" % result.group(1))
                return None
        else:
            self.lagged = False
        return contents

    def login(self):
        """Log in to the wiki with the username given to the class as argument.
        If no such argument was supplied, this method does nothing.
        On success, stores a cookie for use with future requests, on
        error raises an exception"""

        if self.username and not self.logged_in:
            url = "/w/api.php?action=login"
            params = {"lgname": self.username, "lgpassword": self.password, "format": "xml"}
            contents = self.geturl(url, "POST", params)
            if not contents:
                sys.stderr.write("Login failed for unknown reason\n")
                raise httplib.HTTPException

            tree = ElementTree.fromstring(contents)
            # format <?xml version="1.0"?><api>
            # <login result="NeedToken"
            # token="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
            # cookieprefix="enwiktionary"
            # sessionid="yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy" />
            # </api>
            p = tree.find("login")
            if p is None:
                sys.stderr.write("Login failed, no login element found in <%s>\n" % contents)
                raise httplib.HTTPException

            if p.get("result") == "NeedToken":
                wikiprefix = p.get("cookieprefix")
                token = p.get("token")
                url = url + "&lgtoken=%s" % token
                self.cookies = ["%s_session=%s" % (wikiprefix, p.get("sessionid"))]
                contents = self.geturl(url, "POST", params)
                if not contents:
                    sys.stderr.write("Login failed for unknown reason\n")
                    raise httplib.HTTPException
                # format <?xml version="1.0"?><api>
                # <login result="Success" lguserid="518" lgusername="AtouBot"
                # lgtoken="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                # cookieprefix="elwiktionary"
                # sessionid="yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy" />
                # </api>
                tree = ElementTree.fromstring(contents)
                p = tree.find("login")
                if p is None:
                    sys.stderr.write("login failed, <%s>\n" % contents)
                    raise httplib.HTTPException

            if p.get("presult") == "NeedToken":
                sys.stderr.write("Login failed, claiming token needed after second attempt, giving up\n")
                raise httplib.HTTPException

            if p.get("result") != "Success":
                sys.stderr.write("Login failed, <%s>\n" % contents)
                raise httplib.HTTPException

            wikiprefix = p.get("cookieprefix")
            lgtoken = p.get("lgtoken")
            lgusername = p.get("lgusername").encode("utf8")
            lguserid = p.get("lguserid")
            sessionid = p.get("sessionid")

            self.logged_in = True
            self.cookies = ["%s_session=%s" % (wikiprefix, sessionid),
                            "%sUserName=%s" % (wikiprefix, lgusername),
                            "%sUserID=%s" % (wikiprefix, lguserid),
                            "%sToken=%s" % (wikiprefix, lgtoken)]


class Content(object):
    """Download page content from a wiki, given a WikiConnection object for it.
    This class also provides methods for converting titles into various
    formats (linked, removing sql escaping, etc.)"""

    def __init__(self, wiki_conn, titles_file, outdir_name, outfile_name, batch_size,
                 max_retries, verbose):
        """Constructor.  Arguments:
        wiki_conn    -- initialized WikiConnection object for a wiki
        titles_file  -- path to list of titles for which to retrieve page content
        outdir_name  -- directory in which to write any output files
        outfile_name -- filename for content output
        batch_size   -- number of pages to download at once (default 500)
        max_retries  -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        self.wiki_conn = wiki_conn
        self.titles_file = titles_file
        self.outdir_name = outdir_name
        if not os.path.isdir(self.outdir_name):
            os.makedirs(self.outdir_name)
        self.batch_size = batch_size
        self.timestamp = time.strftime("%Y-%m-%d-%H%M%S", time.gmtime())
        if outfile_name:
            self.outfile_name = os.path.join(self.outdir_name, outfile_name)
        else:
            self.outfile_name = os.path.join(self.outdir_name, "content-%s-%s.gz" % (
                self.wiki_conn.wikiname, self.timestamp))
        self.export_url = "/w/index.php?title=Special:Export&action=submit&maxlag=5"
        self.max_retries = max_retries
        self.verbose = verbose

    def unsql_escape(self, title):
        """Remove sql escaping from a page title.
        $wgLegalTitleChars = " %!\"$&'()*,\\-.\\/0-9:;=?@A-Z\\\\^_`a-z~\\x80-\\xFF+";
        so we unescape:  '  "   \   only, by removing leading \
        Note that in the database all titles are stored with underscores instead of
        spaces so convert those; remove enclosing single quotes too, if the title has them.
        Returns cleaned up title.
        Arguments:
        title   -- page title"""

        # expect: \\\\\" -> \\"
        #         \\\\a -> \\a
        #         \\\a and \\" : impossible
        if title[0] == "'" and title[-1] == "'":
            title = title[1:-1]
        title = title.replace("\\\\", '\\')
        title = title.replace("\\'", "\'")
        title = title.replace('\\"', '"')
        title = title.replace('_', ' ')
        return title

    def strip_link(self, title):
        """Remove wikilink markup from title if it exists.
        Returns cleaned up title.
        Arguments:
        title   -- page title"""

        if title.startswith("[[") and title.endswith("]]"):
            return title[2:-2]
        else:
            return title

    def titles_format(self, titles):
        """Format titles for content retrieval via the MediaWiki api.
        Returns formatted title list.
        Arguments:
        titles   -- list of page titles"""

        return [self.unsql_escape(self.strip_link(t)) for t in titles]

    def get_batch_page_content(self, titles):
        """Get content for one batchsize (for example 500) pages via the MediaWiki api.
        Returns content.  If the pages are large and the batchsize is huge, this
        could consume a lot of memory.
        If the servers are overloaded it will retry up to max_retries, waiting a few
        seconds between retries.
        Arguments:
        titles   -- list of page titles"""

        titles_formatted = self.titles_format(titles)
        params = {"wpDownload": "1", "curonly": "1", "pages": "\n".join(titles_formatted) + "\n"}
        self.retries = 0
        while self.retries < self.max_retries:
            if self.wiki_conn.lagged:
                self.retries = self.retries + 1
                if self.verbose:
                    sys.stderr.write("server lagged, sleeping 5 seconds\n")
                time.sleep(5)
            if self.verbose:
                sys.stderr.write("getting batch of page content via %s\n" % self.export_url)
            contents = self.wiki_conn.geturl(self.export_url, "POST", params)
            if not self.wiki_conn.lagged:
                break
        if self.retries == self.max_retries:
            raise WikiRetrieveErr("Server databases lagged, max retries %s reached" % self.max_retries)

        return contents

    def strip_site_footer(self, content):
        """Remove </mediawiki> footer from complete XML text for page content
        If no such tag is found, this indicates damaged input.
        On error, raises WikiRetrieveErr exception
        Arguments:
        content   -- complete XML text for page content"""

        if not content.endswith("</mediawiki>\n"):
            raise WikiRetrieveErr("no mediawiki end tag found, uh oh.")
        return(content[:-13])

    def strip_site_header_and_footer(self, content):
        """Remove <mediawiki> and <siteinfo>...</siteinfo> header from
        complete XML text for page content, also remove the footer
        </mediawiki> from the end
        If no such tag is found, this indicates damaged input.
        On error, raises WikiRetrieveErr exception
        Arguments:
        content   -- complete XML text for page content"""

        # don't parse, just find </siteinfo>\n in the string and toss everything before that
        start = content.find("</siteinfo>\n")
        if not start:
            raise WikiRetrieveErr("no siteinfo header found, uh oh.")
        if not content.endswith("</mediawiki>\n"):
            raise WikiRetrieveErr("no mediawiki end tag found, uh oh.")
        return(content[start + 12: -13])

    def get_all_entries(self):
        """Retrieve page content for all titles in accordance with arguments
        given to constructor, in batches, writing it out to a file.
        On error (failure to retrieve some content), raises WikiRetrieveErr exception"""

        self.output_fd = File.open_output(self.outfile_name)
        self.input_fd = File.open_input(self.titles_file)
        first = True
        count = 0

        eof = False
        while not eof:
            linecount = 0
            titles = []
            while not eof:
                line = self.input_fd.readline()
                if line == "":
                    eof = True
                line = line.strip()
                if line:
                    titles.append(line)
                    linecount = linecount + 1
                if linecount >= self.batch_size:
                    break

            if (not titles):
                break

            count = count + self.batch_size
            content = self.get_batch_page_content(titles)

            if not len(content):
                raise WikiRetrieveErr("content of zero length returned, uh oh.")

            if first:
                first = False
                content = self.strip_site_footer(content)
            else:
                content = self.strip_site_header_and_footer(content)

            self.output_fd.write(content)

        # cheap hack
        self.output_fd.write("</mediawiki>\n")
        self.output_fd.close()
        self.input_fd.close()


class Entries(object):
    """Base class for downloading page titles from a wiki, given a
    WikiConnection object for it. This class also provides methods for
    converting titles into various formats (linked, sql escaped, etc.)."""

    def __init__(self, wiki_conn, props, outdir_name, outfile_name, linked, sql_escaped,
                 batch_size, max_retries, verbose):
        """Constructor. Arguments:
        props       -- comma-separated list of additional properties to request
        wiki_conn    -- initialized WikiConnection object for a wiki
        outdir_name  -- directory in which to write any output files
        outfile_name -- filename for content output
        linked      -- whether or not to write the page titles as links
                       in wikimarkup (i.e. with [[ ]] around them)
        sql_escaped  -- whether or not to write the page titles in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batch_size   -- number of pages to download at once (default 500)
        max_retries  -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        self.wiki_conn = wiki_conn
        if props:
            if ',' in props:
                props = props.split(',')
            else:
                props = [props]
        else:
            props = []
        self.props = props  # extra properties requested by the caller

        self.outdir_name = outdir_name
        if not os.path.isdir(self.outdir_name):
            os.makedirs(self.outdir_name)
        self.linked = linked
        self.sql_escaped = sql_escaped
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.timestamp = time.strftime("%Y-%m-%d-%H%M%S", time.gmtime())
        if outfile_name:
            self.outfile_name = os.path.join(self.outdir_name, outfile_name)
        else:
            self.outfile_name = os.path.join(self.outdir_name, "titles-%s-%s.gz" % (
                self.wiki_conn.wikiname, self.timestamp))
        self.continue_from = None
        self.more = None
        self.verbose = verbose

        self.date_formatter = None
        self.start_date_string = None
        self.end_date_string = None
        self.start_date_secs = None
        self.end_date_secs = None

        # subclasses should set these up as appropriate
        self.url = None

        # the one or two letter prefix that is the name of the XML tag
        # for every entry returned of this query type, e.g. "rc" for recent changes
        self.entrytag_name = None

        self.start_date = None
        self.end_date = None
        self.start_date_param = None
        self.end_date_param = None

        # the one or two letter prefix that is tacked on to all standard
        # param names for this query type, override this if it's not the
        # same as entrytag_name
        self.param_prefix = None

    def setup_props_attrs(self, default_props, extra_props, xml_attrs):
        """set up the properties that will be requested for each entry,
        along with the attributes that will be extracted from each XML entry
        Note that some queries don't take property lists at all, so the lists
        might be empty. They should never be None but we'll handle that justincase.
        Arguments:
        default_props  -- properties we always want for the type of query (e.g. 'title')
        extra_props    -- additional properties the caller requested
        xml_attrs      -- attributes present in the xml though not specifically requested"""
        self.props_to_request = self.combinelists_nodups([default_props, extra_props])
        self.attrs_to_extract = self.combinelists_nodups([xml_attrs, default_props, extra_props])
        if not self.param_prefix:
            self.param_prefix = self.entrytag_name
        if len(self.props_to_request):
            self.prop_param = '&' + self.param_prefix + "prop=" + '|'.join(self.props_to_request)
        else:
            self.prop_param = ""

    def sql_escape(self, title):
        """Escape title in preparation for it to be written
        to an sql file for import.
        $wgLegalTitleChars = " %!\"$&'()*,\\-.\\/0-9:;=?@A-Z\\\\^_`a-z~\\x80-\\xFF+";
        Escapes these characters:  ' "  \   by adding leading \
        Note that in the database all titles are stored with underscores instead of spaces
        so replace those; also enclose the title in single quotes
        Arguments:
        title  -- page title to escape"""

        title = title.replace('\\', "\\\\")  # must insert new backslashs after this step
        title = title.replace("\'", "\\'")
        title = title.replace('"', '\\"')
        title = title.replace(' ', '_')
        return "'" + title + "'"

    def desanitize(self, title):
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

    def combinelists_nodups(self, list_of_lists):
        """Combines all passed lists into one, maintaining order and
        dropping duplicates.
        Arguments:
        list_of_lists -- list of lists which will be combined in order"""

        new_list = []
        for l in list_of_lists:
            if not l:
                continue
            for item in l:
                if item not in new_list:
                    new_list.append(item)
        return new_list

    def write_entry_info(self, entries, linked=False, sql_escaped=False):
        """Write list of entries to an open file, optionally formatting
        them for sql use, and possibly linking the first element in each
        entry, which should be the title or username or other linkable attribute
        Arguments:
        entries   -- list of entries to write, typically containing a title
                     along with possibly other attributes"""

        for e in entries:
            # escape all fields but link only the first, if requested
            if linked:
                e[0] = "[[" + e[0] + "]]"
            if sql_escaped:
                self.output_fd.write(" ".join([self.sql_escape(attr) for attr in e]) + "\n")
            else:
                self.output_fd.write(" ".join(e) + "\n")

    def get_all_entries(self):
        """Retrieve entries such as page titles from wiki in accordance with arguments
        given to constructor, in batches, writing them out to a file.
        On error (failure to rerieve some titles), raises WikiRetrieveErr exception."""

        self.more = True

        if self.start_date:
            self.date_formatter = _date()
            self.start_date_string = self.date_formatter.format_date(self.start_date)
            self.end_date_string = self.date_formatter.format_date(self.end_date)
            self.start_date_secs = self.date_formatter.get_secs(self.start_date_string)
            self.end_date_secs = self.date_formatter.get_secs(self.end_date_string)

        self.output_fd = File.open_output(self.outfile_name)

        count = 0
        while True:
            count = count + self.batch_size
            entries = self.get_batch_entries()
            self.write_entry_info(entries)
            if not len(entries):
                # not always an error
                break
            # FIXME is there a possibility that there will be a continue elt and
            # we'll be served the same titles again?
            if not self.more:
                break
        self.output_fd.close()

    def extract_items_from_xml(self, tree):
        return [[self.desanitize(entry.get(a).encode("utf8")) for a in self.attrs_to_extract]
                for entry in tree.iter(self.entrytag_name)]

    def get_batch_entries(self):
        """Retrieve one batch of entries such as page titles via the MediaWiki api
        If the servers are overloaded it will retry up to max_retries, waiting a few
        seconds between retries.
        NOTE:
        If getting user contribs worked the way it should, we would get a unique
        continue param which would guarantee that the new batch of titles has no
        overlap with the old batch. However, since the continue param is a timestamp,
        and it's possible that there are multiple entries for that timestamp, and
        it's possible that the previous batch ended in the middle of that timestamp,
        we can't rule out the possibility of dups.
        The caller should therefore deal with potential dup titless from this method.
        At least the defaut batchsize of 500 is large enough that we should never wind
        up in a loop getting the same batch every time.
        See bugs https://phabricator.wikimedia.org/T37786 and
        https://phabricator.wikimedia.org/T26782 for more info.
        """

        entries = []
        contents = None
        url = self.url

        # start off with an empty param, because the api requires it, see
        # http://www.mediawiki.org/wiki/API:Query#Continuing_queries
        if self.more:
            if (self.continue_from):
                for key in self.continue_from.keys():
                    url = url + "&%s=%s" % (key, urllib.pathname2url(self.continue_from[key]))
            else:
                    url = url + "&%s=%s" % ("continue", "")
        # usercontribs use ucstart (start date param) as its continuation param too,
        # don't want it in the url twice
        if (self.start_date_string and
                (not self.continue_from or self.start_date_param not in self.continue_from)):
            url = url + "&%s=%s" % (self.start_date_param, urllib.pathname2url(self.start_date_string))
        if self.end_date_string:
            url = url + "&%s=%s" % (self.end_date_param, urllib.pathname2url(self.end_date_string))

        self.retries = 0
        while self.retries < self.max_retries:
            if self.wiki_conn.lagged:
                self.retries = self.retries + 1
                if self.verbose:
                    sys.stderr.write("server lagged, sleeping 5 seconds\n")
                    time.sleep(5)

            if self.verbose:
                sys.stderr.write("getting batch of titles via %s\n" % url)
            contents = self.wiki_conn.geturl(url)
            if not self.wiki_conn.lagged:
                break
            if self.retries == self.max_retries:
                raise WikiRetrieveErr(
                    "Server databases lagged, max retries %s reached" % self.max_retries)

        if contents:
            tree = ElementTree.fromstring(contents)
            # format:
            #  <continue continue="-||" cmcontinue="page|444f472042495343554954|4020758" />
            #  <continue continue="-||" eicontinue="10|!|600" />
            #  <continue continue="-||" apcontinue="B&amp;ALR" />
            #  <continue continue="-||" ucstart="2011-02-24T22:47:06Z" />
            # etc.
            p = tree.find("continue")
            if p is None:
                self.more = False
            else:
                self.more = True
                self.continue_from = p.attrib
                for k in self.continue_from.keys():
                    self.continue_from[k] = self.continue_from[k].encode("utf8")

            # format:
            #  <cm ns="10" title="Πρότυπο:-ακρ-" />
            #  <ei pageid="230229" ns="0" title="μερικοί" />
            #  <p pageid="34635826" ns="0" title="B" />
            #  <item userid="271058" user="YurikBot" ns="0" title="Achmet II" />
            # etc.
            entries = self.extract_items_from_xml(tree)

        return entries


class CatTitles(Entries):
    """Retrieves titles of pages in a given category.  Does not include
    subcategories but that might be nice for the future."""

    def __init__(self, wiki_conn, cat_name, props, outdir_name, outfile_name, linked,
                 sql_escaped, batch_size, retries, verbose):
        """Constructor. Arguments:
        wiki_conn    -- initialized WikiConnection object for a wiki
        cat_name     -- name of category from which to retrieve page titles
        outdir_name  -- directory in which to write any output files
        outfile_name -- filename for content output
        linked      -- whether or not to write the page titles as links
                       in wikimarup (i.e. with [[ ]] around them)
        sql_escaped  -- whether or not to write the page titles in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batch_size   -- number of pages to download at once (default 500)
        retries     -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        super(CatTitles, self).__init__(wiki_conn, props, outdir_name, outfile_name, linked,
                                        sql_escaped, batch_size, retries, verbose)
        self.cat_name = cat_name
        # format <cm ns="10" title="Πρότυπο:-ακρ-" />
        self.entrytag_name = "cm"
        self.setup_props_attrs(["title"], self.props, [])
        self.url = "%s&list=categorymembers&cmtitle=Category:%s&cmlimit=%d%s" % (
            self.wiki_conn.queryapi_url_base, self.cat_name, self.batch_size, self.prop_param)


class EmbeddedTitles(Entries):
    """Retrieves titles of pages that have a specific page embedded in them
    (link, used as template, etc.)"""

    def __init__(self, wiki_conn, page_title, props, outdir_name, outfile_name, linked,
                 sql_escaped, batch_size, retries, verbose):
        """Constructor. Arguments:
        wiki_conn    -- initialized WikiConnection object for a wiki
        page_title   -- title of page for which to find all pages with it embedded
        outdir_name  -- directory in which to write any output files
        outfile_name -- filename for content output
        linked      -- whether or not to write the page titles as links
                       in wikimarup (i.e. with [[ ]] around them)
        sql_escaped  -- whether or not to write the page titles in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batch_size   -- number of pages to download at once (default 500)
        retries     -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        super(EmbeddedTitles, self).__init__(wiki_conn, props, outdir_name, outfile_name,
                                             linked, sql_escaped, batch_size, retries, verbose)
        self.page_title = page_title
        # format <ei pageid="230229" ns="0" title="μερικοί" />
        self.entrytag_name = "ei"
        self.setup_props_attrs([], self.props, ["title"])
        self.url = "%s&list=embeddedin&eititle=%s&eilimit=%d" % (self.wiki_conn.queryapi_url_base,
                                                                 self.page_title, self.batch_size)


class NamespaceTitles(Entries):
    """Retrieves titles of pages in a given namespace."""

    def __init__(self, wiki_conn, namespace, props, outdir_name, outfile_name, linked,
                 sql_escaped, batch_size, retries, verbose):
        """Constructor. Arguments:
        wiki_conn    -- initialized WikiConnection object for a wiki
        namespace   -- number of namespace for which to get page titles
        outdir_name  -- directory in which to write any output files
        outfile_name -- filename for content output
        linked      -- whether or not to write the page titles as links
                       in wikimarup (i.e. with [[ ]] around them)
        sql_escaped  -- whether or not to write the page titles in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batch_size   -- number of pages to download at once (default 500)
        retries     -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        super(NamespaceTitles, self).__init__(wiki_conn, props, outdir_name, outfile_name,
                                              linked, sql_escaped, batch_size, retries, verbose)
        if not namespace.isdigit():
            raise WikiRetrieveErr("namespace should be a number but was %s" % namespace)

        self.namespace = namespace
        # format <p pageid="34635826" ns="0" title="B" />
        self.entrytag_name = "p"
        self.setup_props_attrs([], self.props, ["title"])
        self.url = "%s&list=allpages&apnamespace=%s&aplimit=%d" % (self.wiki_conn.queryapi_url_base,
                                                                   self.namespace, self.batch_size)


class Users(Entries):
    """Retrieves all user names, ids, editcounts and registration info."""

    def __init__(self, wiki_conn, props, outdir_name, outfile_name, linked, sql_escaped,
                 batch_size, retries, verbose):
        """Constructor. Arguments:
        wiki_conn    -- initialized WikiConnection object for a wiki
        outdir_name  -- directory in which to write any output files
        outfile_name -- filename for content output
        linked      -- whether or not to write the user names as links
                       in wikimarup (i.e. with [[ ]] around them)
        sql_escaped  -- whether or not to write the user names in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batch_size   -- number of users to request info for at once (default 500)
        retries     -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        super(Users, self).__init__(wiki_conn, props, outdir_name, outfile_name,
                                    linked, sql_escaped, batch_size, retries, verbose)
        # format <u userid="146308" name="!" editcount="93" registration="2004-12-04T19:39:42Z" />
        self.entrytag_name = "u"
        self.param_prefix = "au"
        self.setup_props_attrs(["editcount", "registration"], self.props, ["name", "userid"])

        self.url = "%s&list=allusers&aulimit=%d%s" % (self.wiki_conn.queryapi_url_base,
                                                      self.batch_size, self.prop_param)


class RCTitles(Entries):
    """Retrieves page titles in recent changes, within a specified date range"""

    def __init__(self, wiki_conn, namespace, props, start_date, end_date, outdir_name,
                 outfile_name, linked, sql_escaped, batch_size, retries, verbose):
        """Constructor. Arguments:
        wiki_conn    -- initialized WikiConnection object for a wiki
        namespace   -- number of namespace for which to get page titles
        start_date   -- starting timestamp for edits,
                       now|today [- num[d|h|m|s]] (days, hours, minutes, seconds, default s) or
                       yyyy-MM-dd [hh:mm:ss]      (UTC time)
        end_date     -- ending timestamp  for edits,
                       now|today [- num[d|h|m|s]] (days, hours, minutes, seconds, default s) or
                       yyyy-MM-dd [hh:mm:ss]      (UTC time)
        outdir_name  -- directory in which to write any output files
        outfile_name -- filename for content output
        linked      -- whether or not to write the page titles as links
                       in wikimarup (i.e. with [[ ]] around them)
        sql_escaped  -- whether or not to write the page titles in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batch_size   -- number of pages to download at once (default 500)
        retries     -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        super(RCTitles, self).__init__(wiki_conn, props, outdir_name, outfile_name, linked,
                                       sql_escaped, batch_size, retries, verbose)
        self.namespace = namespace
        # format: <rc type="edit" ns="0" title="The Blind Assassin" />
        self.entrytag_name = "rc"
        self.setup_props_attrs(["title"], self.props, [])
        # if the props include 'sizes' we need to
        # pull that out of attrs_to_extract and put in oldlen and newlen
        if "sizes" in self.attrs_to_extract:
            ind = self.attrs_to_extract.index("sizes")
            self.attrs_to_extract[ind:ind + 1] = ["oldlen", "newlen"]
        if self.namespace:
            if not self.namespace.isdigit():
                raise WikiRetrieveErr("namespace should be a number but was %s" % namespace)
            self.url = "%s&list=recentchanges&rcnamespace=%s&rclimit=%d%s" % (
                self.wiki_conn.queryapi_url_base, self.namespace, self.batch_size, self.prop_param)
        else:
            self.url = "%s&list=recentchanges&rclimit=%d%s" % (self.wiki_conn.queryapi_url_base,
                                                               self.batch_size, self.prop_param)
        # need these for "&rcstart=$rcstartdate&rcend=$rcenddate"
        self.start_date_param = "rcstart"
        self.end_date_param = "rcend"
        self.start_date = start_date
        self.end_date = end_date


class UserContribsTitles(Entries):
    """Retrieves pages edited by a given user, within a specified date range"""

    def __init__(self, wiki_conn, username, props, start_date, end_date, outdir_name,
                 outfile_name, linked, sql_escaped, batch_size, retries, verbose):
        """Constructor. Arguments:
        wiki_conn    -- initialized WikiConnection object for a wiki
        start_date   -- starting timestamp for edits,
                       now|today [- num[d|h|m|s]] (days, hours, minutes, seconds, default s) or
                       yyyy-MM-dd [hh:mm:ss]      (UTC time)
        end_date     -- ending timestamp  for edits,
                       now|today [- num[d|h|m|s]] (days, hours, minutes, seconds, default s) or
                       yyyy-MM-dd [hh:mm:ss]      (UTC time)
        outdir_name  -- directory in which to write any output files
        outfile_name -- filename for content output
        linked      -- whether or not to write the page titles as links
                       in wikimarup (i.e. with [[ ]] around them)
        sql_escaped  -- whether or not to write the page titles in sql-escaped
                       format, enclosed in single quotes and with various
                       characters quoted with backslash
        batch_size   -- number of pages to download at once (default 500)
        retries     -- number of times to wait and retry if dbs are lagged, before giving up
        verbose     -- display progress messages on stderr"""

        super(UserContribsTitles, self).__init__(wiki_conn, props, outdir_name, outfile_name,
                                                 linked, sql_escaped, batch_size, retries, verbose)
        self.username = username
        # format: <item userid="271058" user="YurikBot" ns="0" title="Achmet II" />
        self.entrytag_name = "item"
        self.setup_props_attrs(["title"], self.props, [])
        self.url = "%s&list=usercontribs&ucuser=%s&uclimit=%d%s" % (
            self.wiki_conn.queryapi_url_base, self.username, self.batch_size, self.prop_param)
        # need these for "&ucstart=$rcstartdate&ucend=$rcenddate"
        self.start_date_param = "ucstart"
        self.end_date_param = "ucend"
        self.start_date = start_date
        self.end_date = end_date


class LogEventsTitles(Entries):
    """Retrieves titles frm log entries for a given log type and action, within a specified date range"""

    def __init__(self, wiki_conn, log_event_action, props, start_date, end_date, outdir_name,
                 outfile_name, linked, sql_escaped, batch_size, retries, verbose):
        """Constructor. Arguments:
        wiki_conn       -- initialized WikiConnection object for a wiki
        log_event_action -- log type and action, separated by '/'  e.g. 'upload/overwrite'
        start_date      -- starting timestamp for log events,
                          now|today [- num[d|h|m|s]] (days, hours, minutes, seconds, default s) or
                          yyyy-MM-dd [hh:mm:ss]      (UTC time)
        end_date        -- ending timestamp  for log events,
                          now|today [- num[d|h|m|s]] (days, hours, minutes, seconds, default s) or
                          yyyy-MM-dd [hh:mm:ss]      (UTC time)
        outdir_name     -- directory in which to write any output files
        outfile_name    -- filename for content output
        linked         -- whether or not to write the page titles as links
                          in wikimarup (i.e. with [[ ]] around them)
        sql_escaped     -- whether or not to write the page titles in sql-escaped
                          format, enclosed in single quotes and with various
                          characters quoted with backslash
        batch_size      -- number of pages to download at once (default 500)
        retries        -- number of times to wait and retry if dbs are lagged, before giving up
        verbose        -- display progress messages on stderr"""

        super(LogEventsTitles, self).__init__(wiki_conn, props, outdir_name, outfile_name,
                                              linked, sql_escaped, batch_size, retries, verbose)
        self.log_event_action = log_event_action
        # format: <item ns="6" title="File:Glenmmont Fire Station.jpg" />
        self.entrytag_name = "item"
        self.setup_props_attrs(["title"], self.props, [])

        self.url = "%s&list=logevents&leaction=%s&lelimit=%d%s" % (
            self.wiki_conn.queryapi_url_base, self.log_event_action, self.batch_size, self.prop_param)
        # need these for "&lestart=<startdate>&leend=<enddate>"
        self.start_date_param = "lestart"
        self.end_date_param = "leend"
        self.start_date = start_date
        self.end_date = end_date


# parse user-supplied dates, compute 'now - d/m/s' expressions,
# format date strings for use in retrieving user contribs (or other lists
# which can be limited by time interval)
class _date(object):
    """Manipulate date and time strings."""

    def __init__(self):
        """Constructor. Duh."""

        self.time_pattern = re.compile("\s+([0-9]+):([0-9])+(:[0-9]+)?$")
        self.date_pattern = re.compile("^([0-9]{4})-([0-9][0-9]?)-([0-9][0-9]?)$")
        self.incr_pattern = re.compile("^(now|today)\s*-\s*([0-9]+)([dhms]?)$")

    def get_date_format_string(self):
        """Return format string we use with strftime for converting all
        user entered date and time strings to a canonical format"""

        return "%Y-%m-%dT%H:%M:%SZ"

    def get_now_minus_incr(self, date_string):
        """Convert date string in format "now|today [- Xd/h/m/s (default seconds)]
        to YYYY-MM-DDThh:mm:ssZ
        Arguments:
        date_string  -- date string to convert"""

        if date_string == "now" or date_string == "today":
            return time.strftime(self.get_date_format_string(), time.gmtime(time.time()))
        result = self.incr_pattern.search(date_string)
        if result:
            increment = int(result.group(2))
            incrtype = result.group(3)
            if incrtype == 'd':
                increment = increment * 60 * 60 * 24
            elif incrtype == 'h':
                increment = increment * 60 * 60
            elif incrtype == 'm':
                increment = increment * 60
            else:
                # incrtype == 's' or omitted
                pass
            return time.strftime(self.get_date_format_string(), time.gmtime(time.time() - increment))
        return None

    def get_ymdhms(self, date_string):
        """Convert date string in form yyyy-MM-dd [hh:mm:ss]
        to form YYY-MM-DDThh:mm:ssZ
        Arguments:
        datestring   -- string to convert"""

        # yyyy-mm-dd [hh:mm:ss]
        years = months = days = hours = mins = secs = 0

        date = date_string
        result = self.time_pattern.search(date_string)
        if result:
            date = date_string[:result.start()]
            hours, mins = int(result.group(1)), int(result.group(2))
            if len(result.group(3)):
                secs = int(result.group(3))

        result = self.date_pattern.search(date)
        if result:
            years, months, days = int(result.group(1)), int(result.group(2)), int(result.group(3))
        if not years:
            return False
        else:
            return time.strftime(self.get_date_format_string(),
                                 (years, months, days, hours, mins, secs, 0, 0, 0))

    def format_date(self, date_string):
        """Convert user-supplied date argument into canonical format
        YYYY-MM-DDThh:mm:ssZ
        Allowable input formats:
          now/today [- Xh/m/d/s (default seconds)]
          yyyy-mm-dd [hh:mm:ss]
        Arguments:
        date_string --  string to convert"""

        date_string = date_string.strip()
        if date_string.startswith("now") or date_string.startswith("today"):
            return(self.get_now_minus_incr(date_string))
        return(self.get_ymdhms(date_string))

    def get_secs(self, date_string_formatted):
        """Given a date string in X format, return the number of seconds since Jan 1 1970
        represented by that date
        Arguments:
        date_stringFormatted  -- date string in the specified format"""

        return calendar.timegm(time.strptime(date_string_formatted, self.get_date_format_string()))


def get_auth_from_file(authfile, username, password):
    """Get username and password from file, overriding
    them with the values that were passed as args, if any
    returns a tuple of the new username and password
    on error, raises exception
    Arguments:
    username -- username that will override value in file, if not None
    password -- password that will override value in file, if not None"""

    if username and password:
        return(username, password)

    fd = open(authfile, "r")
    for line in fd:
        if line[0] == '#' or line.isspace():
            continue

        (keyword, value) = line.split(None, 1)
        value.strip()

        if keyword == "username":
            if not username:
                username = value
        elif keyword == "password":
            if not password:
                password = value
        else:
            raise WikiRetrieveErr("Unknown keyword in auth file <%s>" % keyword)
    fd.close()
    return(username, password)


def usage(message):
    """Display help on all options to stderr and exit.
    Arguments:
    message   -- display this message, with newline added, before
    the standard help output."""

    if message:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: python %s --query querytype [--param value] [--wiki wikiname]
                 [--outputdir dirname] [--outputfile filename]
                 [--startdate datestring] [--enddate datestring]
                 [--linked] [--sql_escaped] [--batchsize batchsize]
                 [--auth username:password] [--authfile filename] [--verbose]
""" % sys.argv[0]
    usage_message = usage_message + """
This script uses the MediaWiki api to download titles of pages in a
specific category, or that include a specific template, or that were
edited by a specific user on a specified wiki.
Alternatively it can retrieve content for a list of titles.
The script may be run as an an anonymous user on that wiki, or with
authentication.
The path to the output file will be written to stdout just before the program
exits, if the run was successful.
Warning: if there happens to be a schema change or namespace change while this
script is running, the results will be inconsistent and maybe broken. These changes
are rare but do happen.

--query (-q):      one of 'category', 'embeddedin', 'log', 'namespace',
                   'usercontribs', 'users' or 'content'
--param (-p):      mandatory for all queries but 'users' and 'rc'
                   for titles: name of the category for which to get titles or name of the
                   article for which to get links, or the number of the namespace from which
                   to get all titles, or the user for which to get changes; for the 'users'
                   query this option should not be specified
                   for log: the log action for which log entries should be retrieved, e.g. upload/upload
                   or move/move_redir; a full list of such entries can be found at
                   http://www.mediawiki.org/w/api.php under the section list=logevents,
                   parameter leaction
                   for rc: namespace for which to retrieve titles (if not specified,
                   retrieve all changes)
                   for content: name of the file containing titles for download
                   for the namespace query, standard namespaces (with their unlocalized names) are:
                   0    Main (content)   1    Talk
                   2    User             3    User talk
                   4    Project          5    Project talk
                   6    File             7    File talk
                   8    MediaWiki        9    MediaWiki talk
                   10   Template         11   Template talk
                   12   Help             13   Help talk
                   14   Category         15   Category talk
                   828  Module           829  Module talk
--props (-P):      additional properties to retrieve (e.g. timestamp, user, etc) as known to MW api
                   separated by commas; may be used with all query types except for 'embeddedin'
                   and 'namespace'
--wiki (-w):       name of the wiki from which to get the category titles
                   default: en.wikipedia.org
--outputdir (-o):  relative or full path to the directory where all files will
                   be created; directory will be created if it does not exist
--outputfile (-O): filename for titles or content output, if it ends in gz or bz2
                   the file will be compressed appropriately
                   default: for title listings, titles-wikiname-yyyy-mm-dd-hhmmss.gz
                   and for content retrieval, content-wikiname--yyyy-mm-dd-hhmmss.gz
--startdate (-S):  start date of titles, for usercontribs or log queries, must be later than enddate
--enddate (-E):    end date of titles, for usercontribs or log queries
--linked (-l):     write titles as wikilinks with [[ ]] around the text
--sqlescaped (-s): write titles with character escaping as for sql INSERT statements
--batchsize (-b):  number of titles to get at once (for bots and sysadmins this
                   can be 5000, but for other users 500, which is the default)
--retries (-r):    number of times a given http request will be retried if the
                   wiki databases are lagged, before giving up
                   default: 20
--auth (-a):       username:password if you need to authenticate for the
                   action or to use a large batchsize; if password is not provided
                   the user will be prompted to enter one
--authfile (-A):   name of file containing authentication information; values that
                   are specified via the auth option will override this
                   file format: each line contains keyword<spaces>value
                   lines with blanks or starting with # will be skipped,
                   keywords are username and password
--verbose (-v):    display messages about what the program is doing
--help:            display this usage message

_date format can be one of the following:
   now|today [- num[d|h|m|s]]    (days, hours, minutes, seconds, default s)
   yyyy-MM-dd [hh:mm:ss]         (UTC time)
Examples:
   today
   now-30
   now-3600 (seconds implied)
   2013-02-01
   2013-03-12 14:01:59
"""
    usage_message = usage_message + """
Example usage:
   python %s --query category --param 'Πρότυπα για τα μέρη του λόγου' \\
             --wiki el.wiktionary.org
   python %s --query usercontribs --param ArielGlenn --startdate now \\
             --enddate 2012-05-01 --outputdir junk
   python %s --query embeddedin --param 'Template:WikiProject Cats' -o junk -v
   python %s -q namespace --param 10 -w as.wikisource.org -o junk -v
   python %s -q log -p upload/upload -o wikisourceuploads -S 2012-05-03 -E 2012-05-01
   python %s -q users -w el.wikisource.org -o wikisourceusers --sqlescape -v
   python %s --query content --param page_titles/titles-2013-03-28-064814.gz \\
             --outputdir junk_content
   python %s -q rc --param 3 -w en.wikipedia.org -o junk -v --startdate now \\
             --enddate 2013-09-25 --props user,comment,sizes -s
""" % (sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0], sys.argv[0],
       sys.argv[0], sys.argv[0], sys.argv[0])
    sys.stderr.write(usage_message)
    sys.exit(1)


def do_main():
    param = None
    query = None
    props = None
    batch_size = 500
    wikiname = "en.wikipedia.org"
    linked = False  # whether to write the page titles with [[ ]] around them
    sql_escaped = False  # whether to sql-escape the title before writing it
    verbose = False
    outdir_name = os.path.join(os.getcwd(), "page_titles")
    outfile_name = None
    max_retries = 20
    username = None
    password = None
    authfile = None
    start_date = None
    end_date = None

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "q:p:P:S:E:w:o:O:lsb:r:a:A:vh",
            ["query=", "param=", "props=", "startdate=", "enddate=", "wiki=", "outputdir=",
             "outputfile=", "linked", "sqlescaped", "batchsize=", "retries=", "auth=",
             "authfile=", "verbose", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-a", "--auth"]:
            if ':' in val:
                username, password = val.split(':')
            else:
                username = val
        elif opt in ["-A", "--authfile"]:
            authfile = val
        elif opt in ["-p", "--param"]:
            param = val
        elif opt in ["-P", "--props"]:
            props = val
        elif opt in ["-S", "--startdate"]:
            start_date = val
        elif opt in ["-E", "--enddate"]:
            end_date = val
        elif opt in ["-b", "--batchsize"]:
            if not val.isdigit():
                usage("batch size must be a number")
            batch_size = int(val)
        elif opt in ["-r", "--retries"]:
            if not val.isdigit():
                usage("retries must be a number")
            retries = int(val)
        elif opt in ["-q", "--query"]:
            query = val
        elif opt in ["-w", "--wiki"]:
            wikiname = val
        elif opt in ["-o", "--outputdir"]:
            outdir_name = val
        elif opt in ["-O", "--outputfile"]:
            outfile_name = val
        elif opt in ["-l", "--linked"]:
            linked = True
        elif opt in ["-s", "--sqlescaped"]:
            sql_escaped = True
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage("Options help:")
        else:
            usage("Unknown option specified: %s" % opt)

    if len(remainder) > 0:
        usage("Unknown option specified: <%s>" % remainder[0])

    if not query or (query != 'users' and not param):
        usage("Missing mandatory option query or param")

    if authfile:
        (username, password) = get_auth_from_file(authfile, username, password)

    if username and not password:
        password = getpass.getpass("Password: ")

    if not (query == "usercontribs" or query == "log" or query == "rc") and (start_date or end_date):
        usage("startdate or enddate specified for wrong query type")

    if props and (query == "embeddedin" or query == "namespace"):
        usage("props specified for wrong query type")

    wiki_conn = WikiConnection(wikiname, username, password, verbose)
    wiki_conn.login()

    if query != "content":
        if param:
            param = urllib.pathname2url(param)
    if query == "category":
        retriever = CatTitles(wiki_conn, param, props, outdir_name, outfile_name, linked,
                              sql_escaped, batch_size, max_retries, verbose)
    elif query == "embeddedin":
        retriever = EmbeddedTitles(wiki_conn, param, props, outdir_name, outfile_name,
                                   linked, sql_escaped, batch_size, max_retries, verbose)
    elif query == "namespace":
        retriever = NamespaceTitles(wiki_conn, param, props, outdir_name, outfile_name,
                                    linked, sql_escaped, batch_size, max_retries, verbose)
    elif query == "usercontribs":
        retriever = UserContribsTitles(wiki_conn, param, props, start_date, end_date,
                                       outdir_name, outfile_name, linked, sql_escaped,
                                       batch_size, max_retries, verbose)
    elif query == "log":
        retriever = LogEventsTitles(wiki_conn, param, props, start_date, end_date,
                                    outdir_name, outfile_name, linked, sql_escaped,
                                    batch_size, max_retries, verbose)
    elif query == 'rc':
        retriever = RCTitles(wiki_conn, param, props, start_date, end_date, outdir_name,
                             outfile_name, linked, sql_escaped, batch_size, max_retries, verbose)
    elif query == "content":
        retriever = Content(wiki_conn, param, outdir_name, outfile_name,
                            batch_size, max_retries, verbose)
    elif query == 'users':
        retriever = Users(wiki_conn, props, outdir_name, outfile_name, linked, sql_escaped,
                          batch_size, max_retries, verbose)
    else:
        usage("Unknown query type specified")

    retriever.get_all_entries()

    # this is the only thing we display to the user, unless verbose is set.
    # wrapper scripts that call this program can grab this in order to do
    # further processing of the titles.
    print retriever.outfile_name

    if verbose:
        sys.stderr.write("Done!\n")


if __name__ == "__main__":
    do_main()
