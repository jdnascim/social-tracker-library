from bs4 import BeautifulSoup
import requests


def google_news_full_cover_urls(url, real_url=False):
    """ given the google news full coverage of an event, get the urls """

    page = 1
    links = set()
    length = 0

    urls = []
    if requests.get(url).url.startswith("https://news.google.com/"):
        while True:
            html = requests.get(url.format(page))
            soup = BeautifulSoup(html.content, "html.parser")
            links.update([a['href'] for a in soup.find_all('a', href=True)])

            if len(links) == length:
                break

            length = len(links)
            page += 1

        for link in sorted(links):
            if link.startswith("./article"):
                urls.append(link)
    else:
        print("Not a Google News url")

    Surls = set(urls)

    if real_url is False:
        return set(["https://news.google.com" + link[1:] for link in Surls])
    else:
        return set([requests.get("https://news.google.com" + link[1:]).url for link in Surls])

    return urls
