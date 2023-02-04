import requests
import pandas as pd
from datetime import date
from logger import logger


class Reddit_API():
    """This class accesses the Reddit API.
    """

    def __init__(self, personal_use_script_code,
                 secret_token, header_info, username, password):
        self.headers = self.storeAccess(personal_use_script_code,
                                        secret_token, header_info, username,
                                        password)

    def getApiAuthentification(self, personal_use_script_code, secret_token):
        """ This function calls the API and verifies the service """
        auth = requests.auth.HTTPBasicAuth(
            personal_use_script_code, secret_token)
        return auth

    def loginUser(self, username, password):
        """ This function stores our login information and returns it """
        data = {'grant_type': 'password',
                'username': username,
                'password': password}
        return data

    def storeAccess(self, personal_use_script_code, secret_token, header_info,
                    username, password):
        """This function uses the helper functions getApiAuthentification and
        loginUser so we are able to create a connection to Reddit API and get 
        data. The functions returns a header, which we need to mention in every
        request we do."""
        headers = {'User-Agent': header_info}

        res = requests.post('https://www.reddit.com/api/v1/access_token',
                            auth=self.getApiAuthentification(
                                personal_use_script_code, secret_token),
                            data=self.loginUser(username, password),
                            headers=headers)
        TOKEN = res.json()['access_token']

        headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}

        return headers

    def make_api_request(self):
        """This function makes the actual request to the Reddit API. It
        processes the posts and puts them into a pandas.DataFrame data
        structure.

        Returns
        -------
        pd.DataFrame
            A pd.DataFrame which contains the processed hottest 100 posts on
            Reddit in terms of food.
        """
        res = requests.get(
            "https://oauth.reddit.com/r/food/hot", headers=self.headers)

        df = pd.DataFrame()  # initialize dataframe

        # loop through each post retrieved from GET request
        for post in res.json()['data']['children']:
            # append relevant data to dataframe
            df = df.append({
                'subreddit': post['data']['subreddit'],
                'title': post['data']['title'],
                'selftext': post['data']['selftext'],
                'upvote_ratio': post['data']['upvote_ratio'],
                'ups': post['data']['ups'],
                'downs': post['data']['downs'],
                'date': date.today()
            }, ignore_index=True)
        return df
