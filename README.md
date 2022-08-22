### Instructions on running against new data:
Script assumes data to be loaded in the existing data directory `data/<new_source>/.../file.json`

running via docker containers assuming docker is already installed:
```
# Run the following at the same level of the makefile
# spinning containers process and sqlite3 commandline
make init 

# running process against new data
make run

# resets the tables in the sqlite3 database
make reset

# stopping and removing the containers
make finish
```

running locally, assuming python3 is already installed:
```
# Run the following within the top level of this directory

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 process/process.py
pytest tests/test_process.py 

# when finished
deactivate 
```

#### Assumptions:
- System has docker/or python installed when running via container/or locally and on linux/mac os
- The new data has similar folder structure being `<source>/.../file.json`
- There are no duplicates in the data
- Assumes the json data files are properly formatted and clean with no malformities

#### Shortcomings:
- Processing the data in memory: In a production pipeline you might be storing the parsed and transformed data in 
intermediate files for further processing downstreaming and for tracability.
Importing the flat files for loading via sql to a structured database at the end of your pipeline, 
rather than storing the parsed data in memory and calling the database directly.
- The script traverses through the whole data directory and 
processes all existing files rather than just select ones. I had initially
used a yaml file to identify new sources that need processing but
it would require the user to update the yaml file everytime.
- Assumptions of the source input data as being clean.
