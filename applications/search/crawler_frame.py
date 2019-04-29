

import logging
from datamodel.search.RscafidiReynagaa_datamodel import RscafidiReynagaaLink, OneRscafidiReynagaaUnProcessedLink, add_server_copy, get_downloaded_content
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter, ServerTriggers
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4
from bs4 import BeautifulSoup

from urlparse import urlparse, parse_qs
from uuid import uuid4

url_set = set()
outlinks = {}

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"



@Producer(RscafidiReynagaaLink)
@GetterSetter(OneRscafidiReynagaaUnProcessedLink)
@ServerTriggers(add_server_copy, get_downloaded_content)

class CrawlerFrame(IApplication):
    def __init__(self, frame):
        self.starttime = time()
        self.app_id = "RscafidiReynagaa"
        self.frame = frame


    def initialize(self):
        self.count = 0
        l = RscafidiReynagaaLink("http://www.ics.uci.edu/")
        print l.full_url
        self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get(OneRscafidiReynagaaUnProcessedLink)
        if unprocessed_links:
            link = unprocessed_links[0]
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(RscafidiReynagaaLink(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")
    
def extract_next_links(rawDataObj):
    outputLinks = []

    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.
    
    Suggested library: lxml
    '''
    soup = BeautifulSoup(rawDataObj.content, 'html.parser')
    for link in soup.find_all('a'):
        print (link.get('href'))
        full_path = rawDataObj.url
        if ("http" not in str(link.get('href'))) and not (".php" in str(link.get('href'))):
            full_path = str(full_path) + str(link.get('href'))
            print "Link updated to " + full_path + " and added to list."
            outputLinks.append(full_path)
            url_set.add(full_path)
        elif (".php" in str(link.get('href'))):
            #print "Link is .php, skipping"
            continue
        elif (".pdf" in str(link.get('href'))):
            #print "link is .pdf, skipping"
            continue
        elif ("mailto" in str(link.get('href'))):
            print "Link is address, skipping"
            continue
        elif ('#' in str(link.get('href'))):
            #print "link is bookmark, skipping."
            continue
        elif (str(link.get('href')) not in url_set):
            #print "Link added to list"
            full_path = str((link.get('href')))
            outputLinks.append(full_path)
            url_set.add(full_path)
    #print "Printing the outputLinks to return to frontier:"
    #print outputLinks
    #print "Printing the set of URLs:"
    #print url_set

    if (outlinks.has_key(full_path)):
        outlinks[full_path] += 1
    else:
        outlinks[full_path] = 1
    for item in outlinks:
        print item, outlinks[item]
    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False