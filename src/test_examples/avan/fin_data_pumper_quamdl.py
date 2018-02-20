import quandl

quandl.ApiConfig.api_key = "oJSdyTHy_foE-UjEPz3x"

# WTI Crude Oil Price - dataset
#data = quandl.get("EIA/PET_RWTC_D")
#print data

# in NumPy array
#data = quandl.get("EIA/PET_RWTC_D", returns="numpy")
#print data

# download table for Nokia
#data = quandl.get_table('MER/F1', compnumber="39102", paginate=True)
#print data

data = quandl.get("EIA/PET_RWTC_D", collapse="monthly")
print data
