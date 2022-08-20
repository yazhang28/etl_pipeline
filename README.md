things i thought during the process
writing to file batch or continuous
flat file vs csv for intermediate data store
batch process vs individual record process for when converting json to structured row

if youve got multiple instances running the process function

might want an id for users with the same first last name and live in the same zipcode
maybe not unless your extending the table then yes
process json before and not during the insert into database for less compute
ideally you wouldnt be using a yaml file to distinguish whether file needs processing

user table would have a unique pk to identify each user

divide data into smaller chunks first
before processing


ideally you would be importing file for loading
rather than storing data in memory
for bigger files chunking/partitioning of data

assuming all filenames are unique


pool of connections rather than opening and closing frequently

