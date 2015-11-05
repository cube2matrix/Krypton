User Manual

1. Make sure you have install Python 2.7.x, and make sure you install the 64 bit version.
because 32 bit version only can take 2GB memory, I am not guraante Python 3.x will work.
For install instruction, please go to http://www.python.org

2. Make sure you have installed PIP, which is package manage tool. For install instruction,
please go to https://pip.pypa.io/en/latest/installing.html

3. Go to same folder with this file, run "sudo pip install --allow-all-external -r requirement.txt" and wait.

4. Run "python main.py --help" at same folder for help. There are options, --parse="pubmed"
means parse PUBMED dataset, --source="./data/pubmed/" means require you give the source file,
--output="./data/save/" means require you give where you want save, --process=5 means cocurrency
with 5 subprocess.

Example:
python main.py --parse="pubmed" --source="./test_data/pubmedMedline/" --output="./test_data/save/" --process=5

Other:
bulk_load_postgres.sh is file to bulk load multiple tsv data file into postgres database with COPY command. The first parameter is source folder patt, the second parameter is database ip, and the third parameter is database username. Good luck!
