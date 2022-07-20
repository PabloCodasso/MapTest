from urllib.parse import urlparse


class Application:  # Highest level application class
    def __init__(self):
        self.user_uinput = str()
        self.main_switch = True

    def execution(self):  # Main loop application function
        while self.main_switch:
            self.user_uinput = input("Please, enter URL or 0 to exit: ")

            if self.user_uinput == '0':
                self.main_switch = False
                continue

            if not self.url_validation(self.user_uinput):
                print("Uncorrect URL, try again. ")
                continue



    @staticmethod
    def url_validation(url_str):  # URL syntax check function
        prsd_url = urlparse(url_str)
        return bool(prsd_url.netloc) and bool(prsd_url.scheme)
