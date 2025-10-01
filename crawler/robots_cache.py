import re,time,urllib.parse as urlparse,urllib.robotparser as robotparser

class RobotsCache:
    def __init__(self,session,ua): 
        self.s=session; 
        self.ua=ua; self.c={}
    async def _f(self,u):
        try:
            async with self.s.get(u,headers={"User-Agent":self.ua}) as r:
                return "" if r.status>=400 else await r.text(errors="ignore")
        except: return ""
    async def get(self,u):
        h=urlparse.urlsplit(u).netloc.lower()
        if h in self.c: return self.c[h][0],self.c[h][1]
        b=urlparse.urlsplit(u)
        ru=urlparse.urlunsplit((b.scheme,b.netloc,"/robots.txt","",""))
        c=await self._f(ru)
        rp=robotparser.RobotFileParser(); rp.parse(c.splitlines() if c else [])
        d=None
        if c:
            for blk in re.split(r"(?im)^\s*user-agent\s*:\s*",c)[1:]:
                ls=blk.splitlines()
                if ls and (ls[0].strip()=="*" or ls[0].lower() in self.ua.lower()):
                    for l in ls[1:]:
                        m=re.match(r"(?i)^\s*crawl-delay\s*:\s*([\d.]+)",l.strip())
                        if m: 
                            try:d=float(m.group(1))
                            except: pass
        self.c[h]=(rp,d or None,time.time())
        return rp,d
