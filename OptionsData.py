# TDA API
from tda.auth import easy_client

# pandas
import pandas as pd

# dotenv allows us to store the API Key in a .env file
import dotenv

# Error catching
import traceback

# Working with time data
import time
import datetime
import pytz

# Working with Discord
import requests
from discord_webhook import DiscordWebhook, DiscordEmbed

### We will use TDA-API to grab live Options data from TD Ameritrade's API
### You can learn more about TDA-API here:
# https://tda-api.readthedocs.io/en/latest/

### This code borrows some from
# https://chrischow.github.io/dataandstuff/2022-01-19-open-options-chains-part-ii/
### with added functionality

### For getting started with TDA, please refer to
# https://www.youtube.com/watch?v=n-YS4aM4FVI&ab_channel=PartTimeLarry

class TDAOptions:

    # Authenticate with TDA
    def create_tda_object(self):

        # retrieve the API Key. It is stored in a file called data.env, in the same directory
        data = dotenv.dotenv_values("data.env")

        # Create the TDA client object
        tda_client = easy_client(
                    api_key=data['STORED_API_KEY'],
                    redirect_uri='https://localhost:8080',
                    token_path='./token.pickle')
        
        return tda_client

    # Make sure all columns that should be Floats, are indeed Floats.
    # If passed in the string NaN, nan, NULL, Null, null, then turn it into an actual NaN value.
    def clean_floats(self, x):
        return pd.to_numeric(x.astype(str).str.replace('NaN|nan|NULL|Null|null', '', regex=True))

    # Normalize all types passed in as Strings
    def clean_strings(self, x):
        return x.astype(str)

    # Normalize all types passed in as Integers
    def clean_ints(self, x):
        return x.astype('int64')

    # Pull Options data from TDA's API and package it into a cleaned Dataframe
    def get_options_data(self, ticker):

        # This code is meant for 0DTE contracts, you can change the days=0 get whatever amount of data you'd like
        end_date = datetime.date.today() + datetime.timedelta(days=0)

        # Create the TDA client object.
        tda_client = self.create_tda_object()

        # TDA's API enjoys crashing every so often, just sorta how it is. Wrap it in a Try/Catch.
        try:
            # get the Epoch for this data
            epoch = int(time.time())

            # Important point is that TDA doesn't give you a *good* way to get specific data, so you're better off
            # getting everything and then distilling it down to what you care about.
            # Pull options chain data from TDA's API
            response =  tda_client.get_option_chain(ticker, contract_type=tda_client.Options.ContractType.ALL,
                                    strike_range=tda_client.Options.StrikeRange.ALL, to_date=end_date)

            # Sleep it if you're rate-limited
            if response.status_code == 429:
                print("Being rate-limited")

                # # You can use this to post an error message on discord
                # requests.post(webhookURL, json = {"content": str(f"TDA's API is rate-limiting the script at:  {datetime.datetime.now(pytz.timezone('US/Eastern'))} \nSleeping for 5 seconds"), "username": "OptionsFailure"})
                
                # Sleep for 5 seconds.
                time.sleep(5)
            else:
                # The future container for options contracts data
                contracts = []

                # The pure options data. It comes in JSON format
                rawOptionsData = response.json()

                # For both Calls & Puts
                for contract_type in ['callExpDateMap', 'putExpDateMap']:
                    # convert data to a Dictionary
                    contract = dict(rawOptionsData)[contract_type]
                    # for easy access to the keys - in this case, all of the expirations and strikes
                    expirations = contract.keys()
                    # run through all the contracts in the expiration
                    for expiry in list(expirations):
                        strikes = contract[expiry].keys()
                        # run through all the strikes in each contract
                        for strike in list(strikes):
                            entry = contract[expiry][strike][0]
                            # Create list of dictionaries with the flattened JSON
                            contracts.append(entry)

                # All of the data, which will be made into the columns of the dataframe
                theColumns = ['putCall', 'strikePrice', 'symbol', 'description', 'bid', 'ask', 'lastPrice', 'bidSize',
                        'askSize', 'lastSize', 'highPrice', 'lowPrice', 'openPrice',
                        'closePrice', 'totalVolume', 'quoteTimeInLong', 'volatility', 'delta',
                        'gamma', 'theta', 'vega', 'rho', 'openInterest', 'expirationDate', 'daysToExpiration']
                
                # Create the Dataframe from the columns above
                contracts = pd.DataFrame(contracts, columns=theColumns)

                # Create a column for the DateTime data
                contracts['theDateTime'] = datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')

                # Normalize floats
                for col in ['bid', 'ask', 'lastPrice', 'highPrice', 'lowPrice', 'openPrice',
                            'closePrice', 'volatility', 'delta', 'gamma', 'theta', 'vega',
                            'rho', 'strikePrice']:
                    contracts[col] = self.clean_floats(contracts[col])

                # Normalize strings
                for col in ['putCall', 'symbol', 'description']:
                    contracts[col] = self.clean_strings(contracts[col])

                # Normalize integers
                for col in ['bidSize', 'askSize', 'lastSize', 'totalVolume', 'quoteTimeInLong',
                            'openInterest', 'expirationDate', 'daysToExpiration']:
                    contracts[col] = self.clean_ints(contracts[col])

                # Change the column name 'description' to 'theDescription' because the former is an SQL keyword
                contracts = contracts.rename(columns={"description": "theDescription"})

                # Nice to have a column that has symbol.epoch for our primary key, but it needs some work
                # First get the symbols into a List
                tdaSymbolList = contracts["symbol"].values.tolist()
                
                # List that will hold the resulting symbol.epoch data
                symbolEpochList = []

                # TDA's symbol value given from the API is: 'QQQ_012523P220'
                # The actual symbol for ToS that we want is 'QQQ230125P220'
                # ....I dunno, buddy, don't ask me
                for con in tdaSymbolList:
                    # Split the symbol at the underscore
                    splitVar = con.split('_')
                    # and use [1] to get the right half, 012523P220. Using the * turns it into the list of chars
                    unpacked = [*splitVar[1]]
                    # Move it all around. It ain't pretty, I know.
                    # .join(unpacked[6:] is useful because it adds all the end stuff back on
                    newStr = splitVar[0] + unpacked[4] + unpacked[5] + unpacked[0] + unpacked[1] + unpacked[2] + unpacked[3] + ''.join(unpacked[6:])
                    # Add in the epoch
                    newStr = newStr + "." + str(epoch)
                    # and you're done. Build a List of these values
                    symbolEpochList.append(newStr)

                # Pandas makes it really, really easy to add this List to the end of the dataframe
                contracts['primaryKey'] = symbolEpochList

                # Now that we're near the end of our tidying, fill any missing values with a zero value
                contracts = contracts.fillna(0)

                # Specifically for puts delta: make it positive
                contracts['delta'] = abs(contracts['delta'])

                return contracts

        # If there was an error
        except Exception:
            # Print it
            traceback.print_exc()
            # and sleep for 1 second
            time.sleep(1)