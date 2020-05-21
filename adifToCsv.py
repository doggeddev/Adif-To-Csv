#!/usr/bin/env python
# coding: utf-8

# In[8]:


import re
import requests
import pandas as pd

#identify tags in file
ADIF_REC_RE = re.compile(r'<(.*?):(\d+).*?>([^<\t\f\v]+)')

#for splitting the header and records
ADIF_ROW_SPLIT = '<eor>|<eoh>(?i)'


#Currently, these tags are taken from the ADIF specification. https://www.adif.org/
#To make this a bit more robust, the headers could be retrieved from the columns names of the table this data will be written to.
#Right now, this gets all the data the is defined by the ADIF spec, but we only need a few fields for scoring/QSO verification
#purposes
headers = ['address','address_intl','age','a_index','ant_az','ant_el','ant_path','arrl_sect','award_submitted','award_granted','band','band_rx','call','check','class','clublog_qso_upload_date','clublog_qso_upload_status','cnty','comment','comment_intl','cont','contacted_op','contest_id','country_intl','cqz','credit_submitted','credit_granted','distance','dxcc','email','eq_call','eqsl_qslrdate','eqsl_qslsdate','eqsl_qsl_rcvd','eqsl_qsl_sent','fists','fists_cc','force_init','freq','freq_rx','gridsquare','guest_op','hrdlog_qso_upload_date','hrdlog_qso_upload_status','iota','iota_island_id','ituz','k_index','lat','lon','lotw_qslrdate','lotw_qslsdate','lotw_qsl_rcvd','lotw_qsl_sent','max_bursts','mode','ms_shower','my_city','my_city_intl','my_cnty','my_country','my_country_intl','my_cq_zone','my_dxcc','my_fists','my_gridsquare','my_iota','my_iota_island_id','my_itu_zone','my_lat','my_lon','my_name','my_name_intl','my_postal_code','my_postal_code_intl','my_rig','my_rig_intl','my_sig','my_sig_intl','my_sig_info','my_sig_info_intl','my_sota_ref','my_state','my_street','my_street_intl','my_usaca_counties','my_vucc_grids','name','name_intl','notes','notes_intl','nr_bursts','nr_pings','operator','owner_callsign','pfx','precedence','prop_mode','public_key','qrzcom_qso_upload_date','qrzcom_qso_upload_status','qslmsg','qslmsg_intl','qslrdate','qslsdate','qsl_rcvd','qsl_rcvd_via','qsl_sent','qsl_sent_via','qsl_via','qso_complete','qso_date','qso_date_off','qso_random','qth','qth_intl','rig','rig_intl','rst_rcvd','rst_sent','rx_pwr','sat_mode','sat_name','sfi','sig','sig_intl','sig_info','sig_info_intl','skcc','sota_ref','srx','srx_string','state','station_callsign','stx','stx_string','submode','swl','ten_ten','time_off','time_on','tx_pwr','usaca_counties','ve_prov','vucc_grids','web']

#this could be broken down further by club if other clubs using hamclubs wanted to extract their member logs
workbookSourceUrl = "http://hamclubs.info/070-app/data/"

#Uncomment below to parse an already downloaded adif file
#fileName = 'PATH/FILE NAME HERE'

#callsign of club member - REQUIRED IF DOWNLOADING FROM HAMCLUBS.INFO
callSign = ""

if callSign == "":
    raise ValueError('Callsign must be provided')    
    
workbookToDownload = ('{}/{}.adi'.format(workbookSourceUrl, callSign))

def downloadAdif(url):  
    return requests.get(url, stream=True)
    
def parseFile(fileName):    
    raw = re.split(ADIF_ROW_SPLIT, open(fileName).read()) #Split records from header in file
    log = parse(raw);
    return log

def parseData(data):
    raw = re.split(ADIF_ROW_SPLIT, data) #Split records from header in data object
    log = parse(raw);
    return log

def parse(raw):
    logbook =[]
    for record in raw[1:-1]:
        row = {}
        elements = ADIF_REC_RE.findall(record)
        for element in elements:
                row[element[0].lower()] = element[2][:int(element[1])]
        logbook.append(row)
    return logbook

def logToCsv(log, headers):    
    csv_data = []    
    for d in log:
        csv_data.append([d[h] if h in d else 'null' for h in headers])    
    df = pd.DataFrame(csv_data, columns=headers)
    return df.to_csv(index=False)

#Download adif from specifc club on the site
response = downloadAdif(workbookToDownload)
log = parseData(response.text)

#If parsing from file locally
#log = parseFile(fileName) 

#column separator = ",", row separator = "\r\n"
csvData = logToCsv(log, headers)

#do something with csvData. Write to file, load into DB, copy to web site, etc
print(csvData)


# In[ ]:





# In[ ]:




