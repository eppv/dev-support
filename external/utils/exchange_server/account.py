from exchangelib import Credentials, Account, Configuration, DELEGATE


def get_account(server='mail.example.com', login='example@mail.com', password='', ):

    credentials = Credentials(login, password)
    config = Configuration(server=server, credentials=credentials)
    account = Account(primary_smtp_address=login,
                      config=config,
                      autodiscover=False,
                      access_type=DELEGATE)

    account.root.refresh()
    return account
