# Alert project
*Alert project is utility to analyse errors from csv file in online mode*
### Run
to start program run **docker compose up**
### Notes
- to start processing you need:
1. add file to container(just add it to dir *date*),
2. run command(inside container or using docker exec):
	*cp date/**filename** date_to_process/*
- to add rule you need:
1. create class based on **CustomRule** class and override methods,
2. append your rule class to rules list at *process_csv()* method(*main.py*)

### Used technologies
- python3
  - pandas
  - watchdog
- docker 
