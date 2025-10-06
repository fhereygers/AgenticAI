# Setup guidelines todo

## prerequisites

Access to cloudera CDP Public Cloud Tenant


## steps

- create project from this git repository
- Launch Agent Studio
- set AGENT_STUDIO_NUM_WORKFLOW_RUNNERS =10 when asked
- run upgrade job
- setup data connectors / verify data connectors
- run /code/insertdata.py to get hive tables in datalake
- go to agent studio
- add an llm (e.g. openai api key)
- add "hol db tool" to tool template by copying code and adding the python packages pandas, impala and impyla to the requirements.txt
- add "hol web scraper" to tool template by copying code and adding the python packages bs4 to the requirements.txt
  
