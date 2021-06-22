"""Skript zur Zusammenführung von OAI-Datenpaketen in einer XML-Datei.
   Script to merge multiple OAI data packages into one xml file

Getestet auf - tested on: Linux, macos und Windows 10
Voraussetzung - requirements: Python 3.9+
Version 2.0 - Autor: vit
"""

# --------------------------------------------------------------------------------------------------
# IMPORT
# --------------------------------------------------------------------------------------------------
import os
import sys
import re
import platform
import multiprocessing as mp
import xml.etree.ElementTree as ET


# Funktion "Display Progress Bar" -- Function "Display Progress Bar"
# The MIT License (MIT)
# Copyright (c) 2016 Vladimir Ignatev
# --------------------------------------------------------------------------------------------------
def progress(count, total, status=''):
    """Display a progressbar on stdout. The function requires a loop.

    :param count: count of iterator
    :type count: (int)
    :param total: total length of iterator
    :type total: (int)
    :param status: explanatory string to display
    :type status: (str)
    """
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    sys.stdout.write('\r[%s] %s%s (%s/%s %s)' % (bar, percents, '%', count, total, status))
    if count != total:
        sys.stdout.flush()
    else:
        sys.stdout.write('\n')


# Funktion "Transformiere XML" - function "Transform XML"
# --------------------------------------------------------------------------------------------------
def parse_xml(input_file):
    """Transform XML files using xmlETree.

    :param input_file: path to XML file
    :type input_file: (str)
    :return: string of xml record nodes
    :rtype: (str)
    """
    # set list of records
    rec_list = []
    # define namespaces
    ns = {'a': 'http://www.openarchives.org/OAI/2.0/', 'b': 'http://www.loc.gov/MARC21/slim'}
    # parse oai-pmh xml file
    for (event, node) in ET.iterparse(input_file):
        # select oai-pmh-record
        if node.tag == '{http://www.openarchives.org/OAI/2.0/}record':
            # look for oaimarc-record with defined namespaces
            rec = node.find('a:metadata/b:record', ns)
            # if a oaimarc-record is present add it to list as a string
            if rec is not None and rec.find('b:controlfield[@tag="008"]', ns) is not None:
                t_008 = rec.find('b:controlfield[@tag="008"]', ns).text
                if t_008 is not None:
                    if len(t_008) > 40:
                        pass
                    elif not t_008[7:11].isnumeric() and not t_008[11:15].isnumeric():
                        rec_list.append(ET.tostring(rec, encoding='unicode'))
                    elif t_008[7:11].isnumeric() and not t_008[11:15].isnumeric():
                        if int(t_008[7:11]) < 2000:
                            rec_list.append(ET.tostring(rec, encoding='unicode'))
                    elif t_008[11:15].isnumeric() and not t_008[7:11].isnumeric():
                        if int(t_008[11:15]) < 2000:
                            rec_list.append(ET.tostring(rec, encoding='unicode'))
                    elif t_008[7:11].isnumeric() and t_008[11:15].isnumeric():
                        if int(t_008[7:11]) < 2000 or int(t_008[11:15]) < 2000:
                            rec_list.append(ET.tostring(rec, encoding='unicode'))
    # remove all namespaces and namespace-prefixes because the namespace is defined globally
    rec_string = re.sub(r'ns0:', '', re.sub(r' xmlns:ns0=\".*\.xsd\"', '', '\n'.join(rec_list)))

    return rec_string


# Funktion "Hauptprogramm" - main programm
# --------------------------------------------------------------------------------------------------
def main():
    """Start main program."""
    # Ask for input path to oaimarc files
    while True:
        pfad = input('Bitte kompletten Pfad zu den Daten angeben: ').rstrip('/\ ').strip("'\"")
        # remove escape characters from path on macos
        if '\\' in pfad and platform.system() == 'Darwin':
            pfad = pfad.replace('\\', '')
        if os.path.isdir(pfad):
            break
        else:
            print('Pfad kann nicht gefunden werden. Bitte Pfadnamen "{}" überprüfen.'.
                  format(pfad))
            continue

    # Create list of all oaimarc files (test for ".xml" and omit resource forks)
    flist = [datei for datei in os.listdir(pfad)
             if os.path.isfile(os.path.join(pfad, datei)) and datei.lower().endswith('.xml')
             and not datei.startswith('._')]
    # Create list of all oaimarc files with complete path
    file_list = []
    for datei in flist:
        file_list.append(os.path.join(pfad, datei))

    # Create output file using multiprocessing if file list contains files
    if len(file_list) > 0:
        # Create output file
        with open('output.xml', 'w', encoding='utf-8') as output:
            output.write('<?xml version="1.0" encoding="UTF-8"?>\n<collection \
xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xsi:schemaLocation="http://www.loc.gov/MARC21/slim \
http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">\n')
            # iterate over the file_list with preset xslt_transform and file_list as iterator
            with mp.Pool(mp.cpu_count()) as pool:
                for cnt, _ in enumerate(pool.imap_unordered(parse_xml, file_list), 1):
                    output.write(_)
                    output.flush()
                    progress(cnt, len(file_list), status='XML verarbeitet')
            output.write('</collection>')
        print('Datei "output.xml" wurde produziert.')
    else:
        sys.exit('Keine Dateien vorhanden...')


# --------------------------------------------------------------------------------------------------
# HAUPTPROGRAMM
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
