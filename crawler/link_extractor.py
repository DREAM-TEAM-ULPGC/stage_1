from html.parser import HTMLParser
import contextlib, urllib.parse as urlparse
from utils import normalize_url,is_http_url

class LinkExtractor(HTMLParser):
    def __init__(self): 
        super().__init__(); 
        self.links=[]
    def handle_starttag(self,tag,attrs):
        if tag.lower()=="a":
            for k,v in attrs:
                if k.lower()=="href" and v: self.links.append(v)

def extract_links(base,html):
    p=LinkExtractor()
    with contextlib.suppress(Exception): p.feed(html)
    out=[]
    for h in p.links:
        if not h: continue
        if h.startswith("#") or h.lower().startswith(("javascript:","mailto:")): continue
        u=urlparse.urljoin(base,h)
        if is_http_url(u): out.append(normalize_url(u))
    return out
