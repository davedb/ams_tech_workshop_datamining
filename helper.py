class Helper:
    __AWS_SECRET_FILE = './grant/aws_secret.txt'
    __ACCOUNTS_FILE = './grant/accounts_secret.txt'

    aws_access_key = ''
    aws_secret_key = ''
    aws_bucket = ''
    account_1 = ''
    account_2 = ''


    def __init__(self):
        f = open(self.__AWS_SECRET_FILE, 'r')
        ulist = f.read().split(',')
        self.aws_access_key = ulist[0].strip()
        self.aws_secret_key = ulist[1].strip()
        self.aws_bucket = ulist[2].strip()
        f.close()

        f = open(self.__ACCOUNTS_FILE, 'r')
        ulist = f.read().split(',')
        self.account_1 = ulist[0].strip()
        self.account_2 = ulist[1].strip()
        f.close()
