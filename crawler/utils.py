import re, urllib.parse as urlparse

def normalize_url(url:str)->str:
    url=url.strip()
    p=urlparse.urlsplit(url)
    scheme=p.scheme.lower() if p.scheme else "http"
    netloc=p.netloc.lower()
    if ":" in netloc:
        h,port=netloc.split(":",1)
        try:
            pn=int(port)
            if (scheme=="http" and pn==80) or (scheme=="https" and pn==443): netloc=h
        except: pass
    path=p.path or "/"
    path=re.sub(r"/{2,}","/",path)
    return urlparse.urlunsplit((scheme,netloc,path,p.query,""))

def is_http_url(url:str)->bool:
    return urlparse.urlsplit(url).scheme.lower() in ("http","https")

def same_reg_domain(u1:str,u2:str)->bool:
    h1=urlparse.urlsplit(u1).hostname or ""
    h2=urlparse.urlsplit(u2).hostname or ""
    return h1==h2 or h1.endswith("."+h2) or h2.endswith("."+h1)
