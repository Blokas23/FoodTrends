# Packages we need in order to access the email inbox
from imap_tools import MailBox, AND
import pandas as pd
import re
from bs4 import BeautifulSoup
from logger import logger
from google.cloud import language_v1
import deepl
from secrets import deepl_api_key


class Email_connection():
    """This class handles all tasks related to the email inbox, viz. connection,
    reading emails and saving their values.
    """

    def __init__(self, email_username: str, email_password: str,
                 email_server: str, email_mailbox: str = "INBOX"):
        """This function instantiates the Email_connection class. It creates a
        dictionary with all the necessary infos regarding the login for the
        email client.

        Parameters
        ----------
        email_username : str
            The email username
        email_password : str
            The email password
        email_server : str
            The IMAP/POP3 server address of the email provider
        email_mailbox : str, optional
            The mailbox which shall be scraped, by default "inbox"
        """
        self.config = {
            'user': email_username,
            'password': email_password,
            'server': email_server,
            'mailbox': email_mailbox
        }
        logger.info("Initialized the Email_connection class")

    def connector(self):
        """This function connects to the email server.
        """
        connector = (MailBox(self.config["server"])
                     .login(self.config["user"],
                            self.config["password"],
                            self.config["mailbox"]))
        logger.info("Successfully connected to Email inbox")
        return connector

    def cleanhtml(self, raw_html: str):
        """This function cleans the html text. More specifically it gets rid
        of emojis and the "secured whitespace".

        Parameters
        ----------
        raw_html : str
            The raw html text.

        Returns
        -------
        str
            A cleaned html string.
        """
        cleanr = re.compile(r'[\n\r\t]')
        cleantext = re.sub(cleanr, ' ', raw_html)
        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"  # emoticons
                                   u"\U0001F300-\U0001F5FF"  # symbols
                                   u"\U0001F680-\U0001F6FF"  # transport
                                   u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                   u"\U00002500-\U00002BEF"  # chinese char
                                   u"\U00002702-\U000027B0"
                                   u"\U00002702-\U000027B0"
                                   u"\U000024C2-\U0001F251"
                                   u"\U0001f926-\U0001f937"
                                   u"\U00010000-\U0010ffff"
                                   u"\u2640-\u2642"
                                   u"\u2600-\u2B55"
                                   u"\u200d"
                                   u"\u23cf"
                                   u"\u23e9"
                                   u"\u231a"
                                   u"\ufe0f"  # dingbats
                                   u"\u3030"
                                   u"\xa0"
                                   "]+", flags=re.UNICODE)
        return emoji_pattern.sub(r' ', cleantext)

    def classify_email(self, text):
        """Classify the input text into categories. """

        language_client = language_v1.LanguageServiceClient()

        document = language_v1.Document(
            content=text, type_=language_v1.Document.Type.PLAIN_TEXT
        )
        response = language_client.classify_text(
            request={'document': document})
        categories = response.categories

        try:
            cat = categories[0].name[1:].split("/")
            while len(cat) < 3:
                if len(cat) == 3:
                    break
                cat.append("none")
            df = pd.DataFrame(cat).T
            df["confidence"] = categories[0].confidence
            df.columns = ["category", "subcategory",
                          "subsubcategory", "confidence"]
            return df
        except IndexError:
            return pd.DataFrame({"category": ["none"],
                                "subcategory": ["none"],
                                 "subsubcategory": ["none"],
                                 "confidence": [0]})

    def read_emails(self):
        """This functions makes use of the connector function and reads the
            emails from the defined mailboxes.
            """
        # Instatiate connection
        connection = self.connector()

        # Variables to save the outcomes
        from_, body_html, date, date_dt, subject, body = [], [], [], [], [], []

        # TODO: Change the parameter "mark_seen" to false if the received
        # emails shall not be marked seen.
        for msg in connection.fetch(AND(seen=False), mark_seen=True,
                                    bulk=True):
            from_.append(msg.from_)
            body_html.append(self.cleanhtml(
                BeautifulSoup(msg.html, "lxml").text))
            date.append(msg.date_str)
            date_dt.append(msg.date)
            subject.append(msg.subject)
            body.append(msg.text)

        df = pd.DataFrame(
            columns=["category", "subcategory", "subsubcategory", "confidence"])
        for i in body_html:
            try:
                df = df.append(self.classify_email(i))
            except:
                translator = deepl.Translator(deepl_api_key)
                df = df.append(self.classify_email(
                    translator.translate_text(i, target_lang="EN-US").text))

        logger.info("Read all emails")
        df1 = pd.DataFrame({"from": from_,
                            "body_html": body_html,
                            "date": date,
                            "date_dt": date_dt,
                            "subject": subject,
                            "body": body})
        # Return the values
        df = df.reset_index(drop=True)
        return pd.concat([df1, df], axis=1)
