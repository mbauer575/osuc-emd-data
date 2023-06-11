
# osu-cascades-energy-monitoring


Current state: 
- Will download from servers while connected via LAN only.  
- Uploads to Azure container via blob storage
- Will update day week and month csvs with all data 
- does not update to Azure yet.  waiting on further functionality of day week month csvs. 

Current State: Problems:
- error on line 66 in fill_5_min_master() function 


Requirements:
- Use pip install -r requirements.txt
- Must have appkeys.json file to run locally 

To run:
- python3 Raw_Data.py

TODO:
- Make Time periods work. 
- Store Keys in a more secure way than .json file (maybe use Azure Key Vault)
- Add more error handling
- Add more logging maybe to an external file
- Organize funcitons, Download functions are kind of messy