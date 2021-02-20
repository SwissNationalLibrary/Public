# Description
This script merges multiple oaimarc files into one marcxml file for further processing. It requires python and saxon to process the oaimarc files. It is tested on linux macos and windows 10.
The output file is created in the same location as the script file.
## Troubleshooting
1. On Windows the script expects saxon to be in the PATH. On linux and macos it checks several predefined paths in /opt/local and /usr/local. If you installed saxon in another location you have to add your path to the list.
2. The script generates a temporary xml file and then creates the output file. Be sure that you have sufficent disk space for this.
