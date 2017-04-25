# -*- coding: utf-8 -*-
import re
import gzip


class File(object):
    """open bz2, gz and uncompressed files"""

    @staticmethod
    def open_input(filename):
        """Open for input a file optionally gz or bz2 compressed,
        determined by existence of .gz or .bz2 suffix"""

        if (filename.endswith(".gz")):
            fd = gzip.open(filename, "rb")
        else:
            fd = open(filename, "r")
        return fd

    @staticmethod
    def open_output(filename):
        """Open for output a file optionally gz or bz2 compressed,
        determined by existence of .gz or .bz2 suffix"""

        if (filename.endswith(".gz")):
            fd = gzip.open(filename, "wb")
        else:
            fd = open(filename, "w")
        return fd

    @staticmethod
    def combine_xml(path_list, output_path):
        """Combine multiple content or stub xml files into one,
        skipping extra headers (siteinfo etc) and footers
        There is a small risk here tht the site info is
        actually different between the files, if we were really
        paranoid we would check that
        Arguments:
        path_list   -- list of full paths to xml content or stub files
        output_path -- full path to combined output file"""

        end_header_pattern = "^\s*</siteinfo>"
        compiled_end_header_pattern = re.compile(end_header_pattern)
        end_mediawiki_pattern = "^\s*</mediawiki>"
        compiled_end_mediawiki_pattern = re.compile(end_mediawiki_pattern)

        out_fd = File.open_output(output_path)
        i = 0
        list_len = len(path_list)
        for f in path_list:
            in_header = True
            in_fd = File.open_input(f)
            for line in in_fd:
                if (i + 1 < list_len):  # skip footer of all files but last one
                    if compiled_end_mediawiki_pattern.match(line):
                        continue
                if i and in_header:  # skip header of all files but first one
                    if compiled_end_header_pattern.match(line):
                        in_header = False
                else:
                    out_fd.write(line)
            in_fd.close()
            i = i + 1

        out_fd.close()
