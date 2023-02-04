# This file contains all the main functions. It just imports the necessary
# functions from the other scripts
from email_access.resource_email import Email_connection
from database.database_connection import Database_connection
from preprocessing.data_preprocessing import Data_prep
from reddit.reddit_api import Reddit_API
from places_api.places_api import GooglePlacesTrender
from secrets import (email_username, email_password, email_server,
                     db_password, db_ip,
                     reddit_personal_use_script_code, reddit_secret_token,
                     reddit_header_info, reddit_username, reddit_password,
                     places_api_key)
import pandas as pd

# This reads the emails from the designated email server
email_ = Email_connection(email_username, email_password, email_server)
emails = email_.read_emails()
emails

emails.to_csv("tmp_emails_classified.csv")

# Database connection
database = Database_connection(db_password, db_ip)
# places = GooglePlacesTrender(API_KEY=places_api_key)

# data_offenburg = places.search_in_location(query="food", location="Offenburg, Germany",
#                                            radius=25000, language="de", first_n=100)
# data_emmendingen = places.search_in_location(query="food", location="Emmendingen, Germany",
#                                              radius=25000, language="de", first_n=100)

# database.create_database("foodtrends")
# database.add_data(
#     pd.concat([data_offenburg, data_emmendingen]), "places", "foodtrends")


database.add_data(emails, "emails_classified", "foodtrends")
database.read_data("emails", "foodtrends")

# This will access the Reddit posts today
# reddit = Reddit_API(reddit_personal_use_script_code,
#                     reddit_secret_token, reddit_header_info, reddit_username,
#                     reddit_password)
# top_food_posts = reddit.make_api_request()

# Test Database connection class
# database = Database_connection(db_password, db_ip)
# database.create_database("foodtrends")
# database.add_data(top_food_posts, "reddits", "foodtrends")
# database.read_data("reddits", "foodtrends")

# Now we are doing the data Analysis part
data_analysis = Data_prep(db_password, db_ip)
data = data_analysis.remove_duplicate_rows("places", "foodtrends")
