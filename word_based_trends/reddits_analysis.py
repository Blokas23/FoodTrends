from word_based_trends.reddits_processing import Word_based_trends
from database.database_connection import Database_connection
import pandas as pd
from secrets import db_ip, db_password
import nltk
from datetime import date, timedelta
from pattern.text.en import singularize

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')


def filterRedditDataByTime(reddit_dataframe, days):
    """This function applies the day filter.

        Parameters
        ----------
        reddit_dataframe : DataFrame
            The DataFrame where will apply our filter
        days : int
            How many of the latest reddit data days shall we use.
        
        df : DataFrame
            The output DataFrame with the applied filter.
    """
    date_one_month_ago = date.today() - timedelta(days=days)
    df = reddit_dataframe[reddit_dataframe['date'] >= date_one_month_ago]
    return df


def getRedditData(database_ip, database_password, days):
    """This retrieves the reddit data.

        Parameters
        ----------
        database_name : str
            The name of the table which shall be accessed.
        database_password : str
            Password to access the table in the database.
        days : int
            How many of the latest reddit data days shall we use.
        df : DataFrame
            The output dataframe with the applied day filter.
    """
    reddits = Word_based_trends(db_ip=db_ip, db_password=db_password)
    reddits.data("reddits", "foodtrends")
    df = pd.DataFrame(reddits.data_)
    df = filterRedditDataByTime(df, days)
    return df

def cleanRedditData(reddit_dataframe):
    """This cleans the reddit data.
       We convert the column "title" of the DataFrame to a list
       consisting of strings. Afterwards, the string is splitted by whitespace.
       In the cleaning process we delete all strings which include a square bracket.
       Furthermore, the word "homemade" gets deleted. 
       The output is a list with cleaned strings.

        Parameters
        ----------
        reddit_dataframe : DataFrame
            The reddit dataframe where we extract the column title,
            convert it to a list and clean it.
        cleaned_foods : list
            Output is a list with cleaned strings.
            
    """
    food = ' '.join(reddit_dataframe['title'].tolist())
    foods = food.split()
    cleaned_foods = []

    for food in foods:
        if "[" not in food and "]" not in food and food != 'homemade' and "/" not in food:
            cleaned_foods.append(food)
    return cleaned_foods


def tokenizeRedditFood(food_list):
    """This function tokenizes all words in the list, 
       so we can only keep the nouns for our further analysis.
       All nouns get singularized and get outputted as a list.

        Parameters
        ----------
        food_list : list
            The input is a list of extracted strings.
        singularized_tokenized_reddit_food : list
            The output is a list of nouns. Which we can analyze.
    """
    text =' '.join(food_list).lower()
    tokens = nltk.word_tokenize(text)
    tags = nltk.pos_tag(tokens)
    reddit_foods = [word for word,pos in tags if (pos == 'NN' or pos == 'NNP' or pos == 'NNS' or pos == 'NNPS')]
    singularized_tokenized_reddit_food = singularizeRedditFood(reddit_foods) 
    return singularized_tokenized_reddit_food


def singularizeRedditFood(plural_food_list):
    """This function singularizes every food term in the list.

        Parameters
        ----------
        plural_food_list : list
            The input is a list with nouns. The function finds to each value in the list
            it's lemma. 
        singular_reddit_food : list
            The output is a list with same length as input. Some words got singularized in the list.
    """
    singular_reddit_food = [singularize(food) for food in plural_food_list]
    return singular_reddit_food


df = getRedditData(database_ip=db_ip, database_password=db_password, days=30)
cleaned_foods = cleanRedditData(df)
yummy = tokenizeRedditFood(cleaned_foods)
preprocessed_yummy_food = pd.DataFrame()
preprocessed_yummy_food['yummy'] = yummy

#preprocessed_yummy_food.to_csv('preprocessed_yummy_food.csv')

database = Database_connection(db_password=db_password,
                               db_ip=db_ip,)

database.add_data(dataframe=preprocessed_yummy_food, 
                  table_name= "Tableau_Reddit",
                   database_name= "foodtrends", 
                   if_exists_parameter="replace")
