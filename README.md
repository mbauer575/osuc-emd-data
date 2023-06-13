
# osu-cascades-energy-monitoring


Current state: 
- Will download from servers while connected to OSU Private network
- Does some calculations on data and saves nessasary information to AZURE DATABASE
- Duplicate data checking only works while its running. May send same datapoint when newly started up. 

Requirements:
- Use pip install -r requirements.txt
- Must have appkeys.json file to run locally 
- Must have .env with Azure connection string
- Azure CLI (currently must be logged in to admin via AD)


Setup with Azure SQL Database instructions:
- Make sure to be logged into Azure Cli
1. Login to Azure and get connection strings from database you want to use
2. Put connection string with filled out password into .env file
3. Edit SQL Query Table name in setup_database(), send_to_space(), and get_from_space() *Need to fix*
4. On first start of Raw_Data.py make sure to use the True flag on startup command to setup database
5. After database is initally setup you can use no flag on startup command
6. On second startup you may have issues with duplicate data

To run:
- First run: python3 Raw_Data.py True
- After setup: python3 Raw_Data.py 
