    """!!! This script only contains tests
    """
from google.cloud import language_v1
from preprocessing.data_preprocessing import Data_prep
import pandas as pd
from secrets import db_ip, db_password, deepl_api_key


class Word_based_trends(Data_prep):

    def data(self, table_name: str, database_name: str):
        """This function retrieves the cleaned data

        Parameters
        ----------
        table_name : str
            The name of the table which shall be accessed.
        database_name : str
            The name of the database wherein the tables lies.
        """
        self.emails = self.remove_duplicate_rows(table_name=table_name,
                                                 database_name=database_name)

    def food_classifer(self):
        """This function should read each email and extract the parts where
        one talks about food.
        """
        raise(NotImplementedError)

    def named_entity_recognition(self):
        """This function takes the parts of emails, which classify as food
        and analyzes whether there is an entity named with it. 
        """
        raise(NotImplementedError)


# test
email = Word_based_trends(db_ip=db_ip, db_password=db_password)
email.data("emails_classified", "foodtrends")
# email.language_classifier()

foods_and_drinks = email.emails.query("category == 'Food & Drink'")


def sample_analyze_entities(text_content):
    """
    Analyzing Entities in a String
    Args:
      text_content The text content to analyze
    """

    client = language_v1.LanguageServiceClient()

    # text_content = 'California is a state.'

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "en"
    document = {"content": text_content, "type_": type_, "language": language}

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = language_v1.EncodingType.UTF8

    response = client.analyze_entities(
        request={'document': document, 'encoding_type': encoding_type})

    consumer_goods, entity_name = [], []
    # Loop through entitites returned from the API
    for entity in response.entities:
        # print(u"Representative name for the entity: {}".format(entity.name))
        if language_v1.Entity.Type(entity.type_).name == "CONSUMER_GOOD":
            consumer_goods.append(language_v1.Entity.Type(entity.type_).name)
            entity_name.append(entity.name)
        # Get entity type, e.g. PERSON, LOCATION, ADDRESS, NUMBER, et al
        # print(u"Entity type: {}".format(
        #     language_v1.Entity.Type(entity.type_).name))

        # Get the salience score associated with the entity in the [0, 1.0] range
        # print(u"Salience score: {}".format(entity.salience))

        # Loop over the metadata associated with entity. For many known entities,
        # the metadata is a Wikipedia URL (wikipedia_url) and Knowledge Graph MID (mid).
        # Some entity types may have additional metadata, e.g. ADDRESS entities
        # may have metadata for the address street_name, postal_code, et al.
    #     for metadata_name, metadata_value in entity.metadata.items():
    #         print(u"{}: {}".format(metadata_name, metadata_value))

    #     # Loop over the mentions of this entity in the input document.
    #     # The API currently supports proper noun mentions.
    #     for mention in entity.mentions:
    #         print(u"Mention text: {}".format(mention.text.content))

    #         # Get the mention type, e.g. PROPER for proper noun
    #         print(
    #             u"Mention type: {}\n".format(
    #                 language_v1.EntityMention.Type(mention.type_).name)
    #         )

    # # Get the language of the text, which will be the same as
    # # the language specified in the request or, if not specified,
    # # the automatically-detected language.
    # print(u"Language of the text: {}".format(response.language))
    return pd.DataFrame({"consumer_goods": consumer_goods,
                         "entity_name": entity_name})


foods_and_drinks["body_html"][4]
test = sample_analyze_entities(foods_and_drinks["body_html"][4])

import requests  # nopep8
url = "https://cloud-api.gate.ac.uk/process-document/annie-named-entity-recognizer"
headers = {'Content-Type': 'text/plain'}
response = requests.post(url, data=foods_and_drinks["body_html"][5].encode(
    "utf-8"), headers=headers).json()

import json  # nopep8
print(json.dumps(response, indent=2))


def gate_ner(sentence):
    import requests
    return [(sentence[entity["indices"][0]:entity["indices"][1]] + f" ({entity['gender']})", entity_type) if entity_type == "Person" and "gender" in entity else (sentence[entity["indices"][0]:entity["indices"][1]], entity_type) for entity_type, entities in requests.post("https://cloud-api.gate.ac.uk/process-document/annie-named-entity-recognizer", data=sentence, headers={'Content-Type': 'text/plain'}).json()["entities"].items() for entity in entities]


gate_ner()
