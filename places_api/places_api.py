# First install this package: pip install -U googlemaps
# Import library to access google places more easily
# This is the offical library from google themselves
import googlemaps
from datetime import date
import time
from typing import Union
import pandas as pd
from logger import logger


class GooglePlacesTrender():
    """This class receives Google Maps Data for all given locations. It extracts
    the necessary data which is needed in order to later on identify foodtrends.
    In order to use this function, you need a API Key for Google Places API. 
    """

    def __init__(self, API_KEY: str):
        """This function initializes the class. It takes the API KEY for googles
        places API as input and verfies it against the API.

        Parameters
        ----------
        API_KEY : str
            The Key which can be obtained in the cloud dashboard under API ->
            Places API -> Security.
        """
        self.gmaps = googlemaps.Client(key=API_KEY)
        logger.info("Connected to Places API")

    def search_in_location(self, query: Union[str, list], radius: int = None,
                           language: str = None, first_n: int = 60,
                           region: str = None, location: str = None) -> pd.DataFrame:
        """This function will start making calls towards the places API from
        google. It will return a dict, which can then further be processed.

        Parameters
        ----------
        query : str, list
            The keyword(s) you are looking for. This could be "food", or "restaurant".
            You can either just pass one keyword as string or a list of strings.
            If you are looking for a more exhaustive list of types check out
            https://developers.google.com/maps/documentation/places/web-service/supported_types
        location : str
            Within this function this refers to a city in the format City, Country.
            E.g. "Mainz, Germany"
        radius : int
            In what vicinity of the given city should be searched. This parameter
            describes the radius in meters, hence 1000 would mean one km.
        language : str
            The language you want to obtain you results in, e.g. "de" for german.
            For more info checkout https://developers.google.com/maps/faq#languagesupport
        first_n : int
            The first n food places which shall be obtained by the google API
        """
        # This line makes the request to the google API, it obtains the first
        # 20 results as well as the next page token
        request = self.gmaps.places(query=query, location=location,
                                    radius=radius, language=language, region=region)
        # Return the number of pages which need to be scraped
        pages = first_n//20
        # Create a list of all dataframes
        frames = [self._process_places_api_response(
            request["results"][i]) for i in range(len(request["results"]))]
        # If pages returns one, just return the concatenated dataframe, since
        # one request was already made.
        if pages == 1:
            logger.info(f"Return dataframe with {first_n} results.")
            return pd.concat(frames)
        # If pages is bigger than 1, then iterate n - 1 times over it (since one
        # request was already made) and append the next obtained dataframes to
        # the frames list.
        for j in range(pages-1):
            try:
                next_page_token = request["next_page_token"]
                time.sleep(3)
                request = self.gmaps.places(query=query, location=location,
                                            radius=radius, language=language,
                                            page_token=next_page_token,
                                            region=region)
                [frames.append(self._process_places_api_response(
                    request["results"][i])) for i in range(len(request["results"]))]
            except KeyError:
                logger.info(f"Return dataframe with {first_n} results.")
                return pd.concat(frames)
        # Concatenate all dataframes from the list and return it.
        logger.info(f"Return dataframe with {first_n} results.")
        return pd.concat(frames)

    def _process_places_api_response(self, list_: list) -> pd.DataFrame:
        """This functions takes a list of a dictionary as input. This function
        is specifically designed to work with the output of the return object
        of the Google Places API. It will extract the most important keys, values
        pairs and returns them in a pandas dataframe.

        Parameters
        ----------
        list_ : list
            The list of the dictionaries.

        Returns
        -------
        pd.DataFrame
            A dataframe with the extracted information from the given list.
        """
        return pd.DataFrame({"date_of_scraping": date.today(),
                             "business_status": list_["business_status"],
                             "formatted_address": list_["formatted_address"],
                             "latitude": list_["geometry"]["location"]["lat"],
                             "longitude": list_["geometry"]["location"]["lng"],
                             "icon": list_["icon"],
                             "icon_background_color": list_["icon_background_color"],
                             "name": list_["name"],
                             "place_id": list_["place_id"],
                             "rating": list_["rating"],
                             "types": ' '.join(map(str, list_["types"])),
                             "user_ratings_total": list_["user_ratings_total"]},
                            index=range(1))
