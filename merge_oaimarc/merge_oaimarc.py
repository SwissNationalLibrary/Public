""" Skript zur Zusammenführung von OAI-Datenpaketen in einer XML-Datei.
    Script to merge multiple OAI data packages into one xml file

Getestet auf Linux, macos und Windows 10
Tested on linux, macos and windows 10
Voraussetzung - requirements: Python 3.9+, Java 8+, Saxon 9+
Version 1.0 - Autor: vit
"""

# --------------------------------------------------------------------------------------------------
# IMPORT
# --------------------------------------------------------------------------------------------------
import os
import sys
import subprocess
import re
import platform
import multiprocessing as mp
from functools import partial


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


# Funktion "Transformiere XML" mittels Multiprocessor --
# Function "Transform XML using Multiprocessor"
# --------------------------------------------------------------------------------------------------
def poolit(file_list):
    """Generate Multiprocessor pool to transform XML files.

    :param file_list: list of files to process
    :type file_list: (list)
    """
    # Find path_to_saxon and set path_to_xslt
    if platform.system() == 'Windows':
        if 'saxon' in os.getenv('Path').lower():
            path_to_saxon = 'Transform'
        else:
            sys.exit('Saxon "Transfom" ist nicht installiert oder nicht im PATH.')
    elif platform.system() == 'Darwin' or platform.system() == 'Linux':
        possible_locations = ['/opt/saxon/saxon9he.jar',
                              '/opt/saxon/saxon9pe.jar',
                              '/usr/local/saxon/saxon9he.jar']
        for val in possible_locations:
            if os.path.exists(val):
                path_to_saxon = 'java -jar {}'.format(val)
                break
            else:
                continue
        try:
            path_to_saxon = path_to_saxon
        except 'NameError':
            sys.exit('Saxon ist nicht installiert oder kann nicht gefunden werden.')

    path_to_xslt = os.path.abspath(os.path.join(os.getcwd(), 'read_records.xsl'))

    # Initialise multiprocessing
    pool = mp.Pool(mp.cpu_count())
    # set fixed arguments for xslt transformation
    func = partial(xsl_transform, path_to_saxon, path_to_xslt)
    # iterate over the file_list with preset xslt_transform and file_list as iterator
    for cnt, _ in enumerate(pool.imap_unordered(func, file_list), 1):
        progress(cnt, len(file_list), status='XML verarbeitet')
    pool.close()


# Funktion "Transformiere XML"
# --------------------------------------------------------------------------------------------------
def xsl_transform(path_to_saxon, path_to_xslt, input_file):
    """Transform XML files using subprocess and Saxon.

    :param path_to_saxon: path to saxon (saxon.jar or Transform)
    :type path_to_saxon: (str)
    :param path_to_xslt: path to XSLT file
    :type path_to_xslt: (str)
    :param input_file: path to XML file
    :type input_file: (str)
    """
    # set up saxon to output to stdout
    args = '{} -s:"{}" -xsl:"{}"'.format(path_to_saxon, input_file, path_to_xslt)
    # run transformation with stdout PIPE
    out = subprocess.run(args, shell=True, stdout=subprocess.PIPE)
    # write stdout to temporary file
    with open('output.xml', 'a', encoding='utf-8') as tmp:
        # write stdout to file and remove unwanted namespaces
        tmp.write(re.sub(r' xmlns=\".*\.xsd\"', '', out.stdout.decode('utf-8')))


# Funktion "Hauptprogramm" - main programm
# --------------------------------------------------------------------------------------------------
def main():
    """Start main program."""

    # Ask for input path to oaimarc files
    while True:
        pfad = input('Bitte kompletten Pfad zu den Daten angeben: ').rstrip('/\ ').strip("'\"")
        # remove escape characters from path
        if '\\' in pfad and platform.system() == 'Darwin':
            pfad = pfad.replace('\\', '')
        if os.path.isdir(pfad):
            break
        else:
            print('Pfad kann nicht gefunden werden. Bitte Pfadnamen "{}" überprüfen.'.
                  format(pfad))
            continue

    # Create list of all oaimarc files (test for ".xml")
    flist = [datei for datei in os.listdir(pfad)
             if os.path.isfile(os.path.join(pfad, datei)) and datei.lower().endswith('.xml')
             and not datei.startswith('._')]
    # Create list of all oaimarc files with complete path
    file_list = []
    for datei in flist:
        file_list.append(os.path.join(pfad, datei))

    # Create output file using multiprocessing if file list contains files
    if len(file_list) > 0:
        # Create temp file
        # if __name__ == "__main__":
        #     poolit(file_list)
        # Create output file
        with open('output.xml', 'w', encoding='utf-8') as output:
            output.write('<?xml version="1.0" encoding="UTF-8"?>\n<collection \
xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xsi:schemaLocation="http://www.loc.gov/MARC21/slim \
http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">\n')
            # read temp file
            # with open('tmp.xml', 'r', encoding='utf-8') as tmp:
            #     lines = tmp.readlines()
            #     for line in lines:
            #         output.write(line)
        poolit(file_list)
        with open('output.xml', 'a', encoding='utf-8') as output:
            output.write('</collection>')
            # remove temp file
            # os.remove('tmp.xml')
        print('Datei "output.xml" wurde produziert.')
    else:
        sys.exit('Keine Dateien vorhanden...')


# --------------------------------------------------------------------------------------------------
# HAUPTPROGRAMM
# --------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
