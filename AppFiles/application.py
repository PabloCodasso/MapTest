from urllib.parse import urlparse
from AppFiles.collector import Collector


class Application:
    def __init__(self):
        self.main_switch = True
        self.user_uinput = str()
        self.result_set = set()

    def execution(self):
        while self.main_switch:
            self.user_uinput = input("Enter URL or 0 to exit: ")

            if self.user_uinput == '0':
                self.main_switch = False
                continue

            if not self.url_validation(self.user_uinput):
                print("Uncorrect URL syntax, try again. ")
                continue

            collector = Collector(self.user_uinput)
            self.result_set = collector.collecting()

        return "Sitemap completed with %d URLS" % len(self.result_set)

    @staticmethod
    def url_validation(url_str):
        prsd_url = urlparse(url_str)
        return bool(prsd_url.netloc) and bool(prsd_url.scheme)
