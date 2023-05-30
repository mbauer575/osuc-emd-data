
# osu-cascades-energy-monitoring


Current state: 
- Will download from servers while connected via LAN only.  
- Uploads to Azure container via blob storage


Requirements:
- Use pip install -r requirements.txt
- Must have appkeys.json file to run.

To run:
- python3 Raw_Data.py

TODO:
- Make Time periods work. 
- Store Keys in a more secure way than .json file (maybe use Azure Key Vault)
- Add more error handling
- Add more logging maybe to an external file
- Organize funcitons, Download functions are kind of messy