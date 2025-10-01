import asyncio,time
from collections import defaultdict

class PoliteLimiter:
    def __init__(self,d=1.0): 
        self.d=d; 
        self.l=defaultdict(asyncio.Lock); 
        self.t=defaultdict(float); 
        self.ds={}
    def set_delay(self,h,d): 
        self.ds[h]=max(0.0,d)
    async def wait_turn(self,h):
        async with self.l[h]:
            de=self.ds.get(h,self.d); n=time.time(); tt=self.t[h]
            if n<tt: await asyncio.sleep(tt-n)
            self.t[h]=time.time()+de