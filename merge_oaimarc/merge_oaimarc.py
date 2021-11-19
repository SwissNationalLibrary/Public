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
import logging
from logging.handlers import QueueHandler, QueueListener
import multiprocessing as mp
import xml.etree.ElementTree as ET


def worker_init(q):
    # all records from worker processes go to qh and then into q
    qh = QueueHandler(q)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(qh)


def logger_init():
    q = mp.Queue()
    # this is the handler for all log records
    # handler = logging.StreamHandler()
    handler = logging.FileHandler('log.csv', 'w')
    handler.setFormatter(logging.Formatter("%(message)s"))

    # ql gets records from the queue and sends them to the handler
    ql = QueueListener(q, handler)
    ql.start()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # add the handler to the logger so records from this process are handled
    logger.addHandler(handler)

    return ql, q


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
                t_001 = rec.find('b:controlfield[@tag="001"]', ns).text
                t_008 = rec.find('b:controlfield[@tag="008"]', ns).text
                leader = rec.find('b:leader', ns).text
                if len(leader) != 24:
                    logging.info('Leader inkorrekt;{};{}'.format(t_001, leader))
                    year = 1992
                elif leader[6] in ['e', 'f']:
                    year = 1994
                else:
                    year = 1992
                if t_008 is not None:
                    if len(t_008) > 40:
                        logging.info('008 zu lang;{};{}'.format(t_001, t_008))
                        if t_008[7:11].isnumeric():
                            if int(t_008[7:11]) <= year:
                                rec_list.append(ET.tostring(rec, encoding='unicode'))
                        elif t_008[11:15].isnumeric():
                            if int(t_008[11:15]) <= year:
                                rec_list.append(ET.tostring(rec, encoding='unicode'))
                    elif len(t_008) < 40:
                        logging.info('008 zu kurz;{};{}'.format(t_001, t_008))
                    elif t_008[6] == 's':
                        if t_008[11:15].isnumeric() or 'u' in t_008[11:15] or 'n' in t_008[11:15]:
                            logging.info('Jahr falsch kodiert;{};{}'.format(t_001, t_008))
                            if 'u' in t_008[7:11]:
                                rec_list.append(ET.tostring(rec, encoding='unicode'))
                            if t_008[7:11].isnumeric():
                                if int(t_008[7:11]) <= year:
                                    rec_list.append(ET.tostring(rec, encoding='unicode'))
                        else:
                            if not t_008[7:11].isnumeric():
                                rec_list.append(ET.tostring(rec, encoding='unicode'))
                            elif t_008[7:11].isnumeric():
                                if int(t_008[7:11]) <= year:
                                    rec_list.append(ET.tostring(rec, encoding='unicode'))
                    else:
                        if not t_008[7:11].isnumeric() and not t_008[11:15].isnumeric():
                            rec_list.append(ET.tostring(rec, encoding='unicode'))
                        elif t_008[7:11].isnumeric() and not t_008[11:15].isnumeric():
                            if int(t_008[7:11]) <= year:
                                rec_list.append(ET.tostring(rec, encoding='unicode'))
                        elif t_008[11:15].isnumeric() and not t_008[7:11].isnumeric():
                            if int(t_008[11:15]) == 9999 or int(t_008[11:15]) <= year:
                                rec_list.append(ET.tostring(rec, encoding='unicode'))
                        elif t_008[7:11].isnumeric() and t_008[11:15].isnumeric():
                            if int(t_008[7:11]) <= year or int(t_008[11:15]) == 9999 \
                              or int(t_008[11:15]) <= year:
                                rec_list.append(ET.tostring(rec, encoding='unicode'))
                else:
                    logging.info('008 fehlt;{}'.format(t_001))
    # remove all namespaces and namespace-prefixes because the namespace is defined globally
    rec_string = re.sub(r'ns0:', '', re.sub(r' xmlns:ns0=\".*\.xsd\"', '', '\n'.join(rec_list)))

    return rec_string


# Funktion "Hauptprogramm" - main programm
# --------------------------------------------------------------------------------------------------
def main():
    """Start main program."""
    q_listener, q = logger_init()
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
            with mp.Pool(mp.cpu_count(), worker_init, [q]) as pool:
                for cnt, _ in enumerate(pool.imap_unordered(parse_xml, file_list), 1):
                    output.write(_)
                    output.flush()
                    progress(cnt, len(file_list), status='XML verarbeitet')
                q_listener.stop()
            output.write('</collection>')
        print('Datei "output.xml" wurde produziert.')
    else:
        sys.exit('Keine Dateien vorhanden...')


# --------------------------------------------------------------------------------------------------
# HAUPTPROGRAMM
# --------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()
