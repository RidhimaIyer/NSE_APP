# NSE_APP

Parameter file "parameter.json" contains column names to be extracted. It also contains dates that were public holidays to be skipped for download. 

Run below script to download and extract previous 30 day files. It also maintains a log file "app.log" to track any exceptions. 

python data_extract.py 

Run below script to run flask job which creates a simple web service that filters out extracted data based on "SYMBOLS". Above job also creates a master file for all securites to be used by flask web api.

python App/flask_app.py