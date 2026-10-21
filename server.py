import asyncio, os, re, time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, Response



# ---------- ПАРСЕР ----------

_R = [
("Мурманская область","мурманск"),("Ненецкий АО",r"(?<!ямало-)ненецк|\bнао\b"),("Ямало-Ненецкий АО","ямал|ямало"),("Чукотский АО","чукот"),
("Республика Карелия","карели|петрозаводск"),("Архангельская область","архангельск"),("Республика Коми",r"\bкоми\b|сыктывкар"),
("Ханты-Мансийский АО",r"ханты|хмао|югр"),("Красноярский край","красноярск"),("Республика Саха (Якутия)",r"якути|\bсаха\b"),("Магаданская область","магадан"),
("Санкт-Петербург",r"петербург|\bспб\b"),("Ленинградская область","ленинградск"),("Вологодская область","вологод|череповец"),
("Кировская область",r"кировск\w*\s+обл"),("Пермский край",r"пермск|\bпермь\b|\bперми\b"),("Свердловская область","свердловск|екатеринбург"),
("Тюменская область","тюмен"),("Томская область","томск"),("Иркутская область","иркутск"),("Хабаровский край","хабаровск"),("Камчатский край","камчат"),
("Псковская область",r"псковск|\bпсков(а|е|у)?\b"),("Новгородская область","новгородск|великий новгород|великого новгорода"),
("Тверская область",r"тверск|\bтвер(ь|и)\b"),("Ярославская область",r"ярославск|ярославл"),("Костромская область","костром"),
("Удмуртия","удмурт|ижевск"),("Башкортостан",r"башкир|башкорт|\bуф(а|ы|е)\b"),("Челябинская область","челябинск"),
("Курганская область",r"курганск|\bкурган\b"),("Омская область",r"\bомск"),("Новосибирская область","новосибирск"),
("Кемеровская область","кемеровск|кузбасс"),("Хакасия","хакас"),("Республика Тыва",r"\bтыв(а|е|у|ы)\b|\bтува\b"),
("Республика Бурятия","бурят"),("Забайкальский край","забайкал"),("Амурская область","амурск"),("Еврейская АО",r"еврейск|\bеао\b"),
("Сахалинская область","сахалин"),("Калининградская область","калининград"),("Смоленская область","смоленск"),
("Москва",r"\bмоскв"),("Московская область",r"(?<!ново)московск|подмосков"),("Владимирская область",r"владимирск\w*\s+обл"),
("Нижегородская область",r"нижегородск|нижн\w+\s+новгород"),("Татарстан","татарстан|казан"),
("Алтайский край",r"алтайск\w*\s+кра|барнаул"),("Республика Алтай",r"республик\w*\s+алтай|горно-алтайск"),
("Приморский край",r"приморск(ий|ого|ому|ом|им)\b|приморь"),
("Брянская область","брянск"),("Калужская область",r"калужск|калуг"),("Тульская область",r"тульск|\bтул(а|е|у|ы)\b|новомосковск|алексин|ефремов|щекино|щекин|узлов"),
("Рязанская область","рязан"),("Республика Мордовия","мордов|саранск"),("Ульяновская область","ульяновск"),
("Самарская область",r"самарск|\bсамар(а|е|у|ы)\b"),("Оренбургская область","оренбург"),
("Курская область","курск"),("Орловская область",r"орловск|\bорл(а|у|е|ом)?\b|\bорел\b"),("Липецкая область","липецк"),
("Тамбовская область","тамбов"),("Пензенская область",r"пензенск|\bпенз(а|е|у|ы)\b"),("Саратовская область","саратов|энгельс"),
("Белгородская область","белгород"),("Воронежская область","воронеж"),("Волгоградская область","волгоград"),("Астраханская область","астрахан"),
("Республика Крым",r"\bкрым(а|у|е|ом)?\b"),("Краснодарский край",r"краснодар|кубани|новороссийск|туапсе|темрюк|\bсочи\b|\bанап"),
("Ростовская область",r"ростовск|ростов-на-дону|\bростов\b"),("Ставропольский край","ставропол"),("Республика Калмыкия","калмык|элист"),
("Севастополь","севастопол"),("Карачаево-Черкесия","карачаево|черкесск"),("Кабардино-Балкария","кабардино|нальчик"),
("Северная Осетия","осети|алани"),("Чечня","чечн|грозн"),("Дагестан","дагестан|махачкал"),
]
REG = [(n, re.compile(p)) for n, p in _R]
NAMES = [n for n, _ in REG]

DRONE = re.compile(r"беспилотн|бпла")
MISSILE = re.compile(r"ракетн")
AIR = re.compile(r"воздушн\w*\s+(тревог|опасност)")
ALERTW = re.compile(r"опасност|тревог|угроз")
CLEAR = re.compile(r"отбо[йяе]")

def parse(text, default=None):
    """Возвращает список (регион, 'd'|'m', 1=опасность | 2=отбой).
    Понимает: «Курская область, отбой ракетной опасности», списки регионов под заголовком,
    список мест перед строкой «Тревога по БПЛА», канал с регионом по умолчанию."""
    out, carry, pending = [], None, []
    for seg in re.split(r"[\n.;!]+", text):
        low = seg.lower().replace("ё", "е").strip()
        if not low:
            continue
        kinds = set()
        if DRONE.search(low): kinds.add("d")
        if MISSILE.search(low): kinds.add("m")
        if AIR.search(low): kinds |= {"d", "m"}
        clear = bool(CLEAR.search(low))
        regs = [n for n, rx in REG if rx.search(low)]
        if clear or (kinds and ALERTW.search(low)):
            carry = (kinds or {"d", "m"}, 2 if clear else 1)
            if not regs:
                regs = pending or ([default] if default else [])
            pending = []
        elif kinds:            # «фиксации БПЛА», «сбит БПЛА» без объявления тревоги
            carry, pending = None, []
            continue
        elif regs and not carry:
            pending += [r for r in regs if r not in pending]   # места до заголовка
            continue
        elif not regs:
            carry, pending = None, []
        if regs and carry:
            ks, v = carry
            out += [(r, k, v) for r in regs for k in sorted(ks)]
    return list(dict.fromkeys(out))

# ---------- СТРАНИЦА ----------
HTML = r'''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<link rel="manifest" href="/manifest.json"><meta name="theme-color" content="#0b1117"><meta name="apple-mobile-web-app-capable" content="yes"><link rel="apple-touch-icon" href="/icon.svg">
<title>Радар опасности</title>
<style>
:root{--bg:#0b1117;--fg:#e6edf3;--mut:#8b98a5;--tile:#17212b;--bd:#26323f;--da:#f59e0b;--dc:#22c55e;--ma:#ef4444;--mc:#38bdf8;--s:38px;box-sizing:border-box;padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}
@media (prefers-color-scheme:light){:root:not([data-theme="dark"]){--bg:#f3f5f7;--fg:#14191f;--mut:#5b6773;--tile:#e2e7ec;--bd:#c9d1d9}}
*{box-sizing:border-box}
html,body{margin:0;height:100%}
body{background:var(--bg);color:var(--fg);font:14px/1.3 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;display:flex;flex-direction:column}
header{padding:10px 12px 6px}
.top{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px}
h1{font-size:17px;margin:0}
.z button{width:32px;height:32px;border-radius:8px;border:1px solid var(--bd);background:var(--tile);color:var(--fg);font-size:18px;margin-left:6px}
.stats{display:flex;gap:6px}
.stat{flex:1;border:1px solid var(--bd);border-radius:10px;padding:5px 2px;text-align:center}
.stat b{display:block;font-size:19px}
.stat span{font-size:10px;color:var(--mut)}
#map-wrap{flex:1;overflow:auto;padding:8px 12px}
#map{display:grid;grid-template-columns:repeat(21,var(--s));grid-auto-rows:var(--s);gap:3px;width:max-content}
.t{border:0;border-radius:8px;background:var(--tile);color:var(--mut);font-size:calc(var(--s)*.26);font-weight:600;padding:0;cursor:pointer;-webkit-tap-highlight-color:transparent;overflow:hidden}
.t.on{color:#fff;text-shadow:0 1px 2px rgba(0,0,0,.55)}
.t.pulse{animation:p 1.2s infinite}
.t.sel{outline:2px solid var(--fg);outline-offset:1px}
@keyframes p{50%{opacity:.65}}
footer{padding:8px 12px 10px;border-top:1px solid var(--bd)}
#sname{font-weight:600;margin-bottom:6px;min-height:19px}
.r{display:flex;gap:6px;align-items:center;margin-bottom:6px}
.r span{width:48px;font-size:12px;color:var(--mut)}
.r button,.row button{flex:1;padding:9px 4px;border-radius:9px;border:1px solid var(--bd);background:var(--tile);color:var(--fg);font-size:13px}
.r button.a{color:#fff;border-color:transparent}
.row{display:flex;gap:8px}
.lg{display:flex;flex-wrap:wrap;gap:4px 12px;font-size:11px;color:var(--mut);margin-top:8px}
.lg i{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:4px;vertical-align:-1px}
#log{max-height:60px;overflow:auto;font-size:12px;color:var(--mut);margin-top:6px}
</style>
</head>
<body>
<header>
<div class="top"><h1>Радар опасности</h1><div class="z"><button id="zm">−</button><button id="zp">+</button></div></div>
<div class="stats">
<div class="stat"><b id="c_d1" style="color:var(--da)">0</b><span>БПЛА</span></div>
<div class="stat"><b id="c_m1" style="color:var(--ma)">0</b><span>Ракеты</span></div>
<div class="stat"><b id="c_d2" style="color:var(--dc)">0</b><span>Отбой БПЛА</span></div>
<div class="stat"><b id="c_m2" style="color:var(--mc)">0</b><span>Отбой ракет</span></div>
</div>
</header>
<div id="map-wrap"><div id="map"></div></div>
<footer>
<div id="sname">Нажмите на регион</div>
<div id="det" style="font-size:13px;color:var(--mut);min-height:34px;margin-bottom:4px"></div>
<div class="lg"><span><i style="background:var(--da)"></i>Опасность БПЛА</span><span><i style="background:var(--dc)"></i>Отбой БПЛА</span><span><i style="background:var(--ma)"></i>Ракетная опасность</span><span><i style="background:var(--mc)"></i>Отбой ракетной</span></div>
<div id="log"></div>
<div id="upd" style="font-size:11px;color:var(--mut);margin-top:6px">Загрузка…</div>
</footer>
<script>
var D=[
"Мурманская область|3|0|Мурм","Ненецкий АО|5|0|НАО","Ямало-Ненецкий АО|8|0|ЯНАО","Чукотский АО|20|0|Чук",
"Республика Карелия|3|1|Кар","Архангельская область|4|1|Арх","Республика Коми|5|1|Коми","Ханты-Мансийский АО|8|1|ХМАО","Красноярский край|11|1|Крас","Республика Саха (Якутия)|16|1|Якут","Магаданская область|18|1|Маг",
"Санкт-Петербург|1|2|СПб","Ленинградская область|2|2|ЛО","Вологодская область|4|2|Вол","Кировская область|5|2|Киров","Пермский край|6|2|Перм","Свердловская область|7|2|Свер","Тюменская область|8|2|Тюм","Томская область|9|2|Том","Иркутская область|13|2|Ирк","Хабаровский край|17|2|Хаб","Камчатский край|19|2|Камч",
"Псковская область|0|3|Пск","Новгородская область|1|3|Новг","Тверская область|2|3|Твер","Ярославская область|3|3|Яро","Костромская область|4|3|Кос","Удмуртия|5|3|Удм","Башкортостан|6|3|Башк","Челябинская область|7|3|Чел","Курганская область|8|3|Кург","Омская область|9|3|Омск","Новосибирская область|10|3|НСК","Кемеровская область|11|3|Кем","Хакасия|12|3|Хак","Республика Тыва|13|3|Тыва","Республика Бурятия|14|3|Бур","Забайкальский край|15|3|Заб","Амурская область|16|3|Амур","Еврейская АО|17|3|ЕАО","Сахалинская область|18|3|Сах",
"Калининградская область|0|4|Клгд","Смоленская область|1|4|Смол","Москва|2|4|Мск","Московская область|3|4|МО","Владимирская область|4|4|Влд","Нижегородская область|5|4|НН","Татарстан|6|4|Тат","Алтайский край|10|4|АлтК","Республика Алтай|11|4|АлтР","Приморский край|17|4|Прим",
"Брянская область|0|5|Брян","Калужская область|1|5|Калу","Тульская область|2|5|Тула","Рязанская область|3|5|Ряз","Республика Мордовия|4|5|Морд","Ульяновская область|5|5|Улья","Самарская область|6|5|Сам","Оренбургская область|7|5|Орен",
"Курская область|0|6|Курс","Орловская область|1|6|Орёл","Липецкая область|2|6|Лип","Тамбовская область|3|6|Тамб","Пензенская область|4|6|Пенз","Саратовская область|5|6|Сар",
"Белгородская область|0|7|Белг","Воронежская область|2|7|Вор","Волгоградская область|4|7|Волг","Астраханская область|5|7|Астр",
"Республика Крым|0|8|Крым","Краснодарский край|1|8|Крд","Ростовская область|2|8|Рост","Ставропольский край|3|8|Став","Республика Калмыкия|4|8|Калм",
"Севастополь|0|9|Сев","Карачаево-Черкесия|1|9|КЧР","Кабардино-Балкария|2|9|КБР","Северная Осетия|3|9|СОс","Чечня|4|9|Чеч","Дагестан|5|9|Даг"
].map(function(r){var p=r.split("|");return{n:p[0],c:+p[1],r:+p[2],l:p[3]}});

var COL={d:["","var(--da)","var(--dc)"],m:["","var(--ma)","var(--mc)"]};
var TXT={d:["","опасность БПЛА","отбой БПЛА"],m:["","ракетная опасность","отбой ракетной"]};
var S={},E={},sel=null;
var map=document.getElementById("map"),nameEl=document.getElementById("sname"),det=document.getElementById("det"),logEl=document.getElementById("log"),upd=document.getElementById("upd");
D.forEach(function(x){
  var b=document.createElement("button");
  b.className="t";b.textContent=x.l;b.title=x.n;
  b.style.gridColumn=x.c+1;b.style.gridRow=x.r+1;
  b.onclick=function(){select(x.n)};
  map.appendChild(b);E[x.n]=b;S[x.n]={d:0,m:0,td:0,tm:0};
});
function paint(n){
  var s=S[n],b=E[n],dc=COL.d[s.d],mc=COL.m[s.m];
  b.className="t"+(s.d||s.m?" on":"")+(s.m===1?" pulse":"")+(sel===n?" sel":"");
  b.style.background=dc&&mc?"linear-gradient(135deg,"+dc+" 50%,"+mc+" 50%)":(dc||mc||"");
}
function ago(sec){var m=Math.round(sec/60);return m<1?"только что":m<60?m+" мин назад":Math.floor(m/60)+" ч "+(m%60)+" мин назад"}
function detail(){
  if(!sel)return;var s=S[sel];
  var a=s.d?TXT.d[s.d]+" · "+ago(s.td):"БПЛА: нет данных";
  var b=s.m?TXT.m[s.m]+" · "+ago(s.tm):"ракеты: нет данных";
  det.textContent="";[a,b].forEach(function(t){var d=document.createElement("div");d.textContent=t;det.appendChild(d)});
}
function select(n){var o=sel;sel=n;if(o)paint(o);paint(n);nameEl.textContent=n;detail()}
function count(){["d","m"].forEach(function(k){[1,2].forEach(function(v){
  document.getElementById("c_"+k+v).textContent=D.filter(function(x){return S[x.n][k]===v}).length})})}
async function load(){
  try{
    var j=await (await fetch("/api/state",{cache:"no-store"})).json();
    D.forEach(function(x){var s=j.regions[x.n]||{d:[0,0],m:[0,0]};S[x.n]={d:s.d[0],m:s.m[0],td:s.d[1],tm:s.m[1]};paint(x.n)});
    count();detail();
    logEl.textContent="";
    j.events.forEach(function(e){var d=document.createElement("div");
      d.textContent=new Date(e.t*1000).toTimeString().slice(0,5)+" · "+e.r+" — "+TXT[e.k][e.v];logEl.appendChild(d)});
    var bad=Object.keys(j.channels).filter(function(c){return !j.channels[c].ok}).length;
    upd.textContent=j.configured?("Обновлено "+new Date().toTimeString().slice(0,5)+" · каналов: "+j.configured+(bad?" · с ошибкой: "+bad:"")):"Каналы не заданы (переменная CHANNELS на сервере)";
  }catch(e){upd.textContent="Нет связи с сервером"}
}
function zoom(d){sz=Math.max(24,Math.min(64,sz+d));document.documentElement.style.setProperty("--s",sz+"px")}
var sz=38;
document.getElementById("zp").onclick=function(){zoom(6)};
document.getElementById("zm").onclick=function(){zoom(-6)};
load();setInterval(load,10000);
</script>
</body>
</html>
'''

# ---------- СЕРВЕР ----------
POLL = int(os.getenv("POLL_SEC", "60"))                  # как часто опрашивать каналы
ALERT_TTL = int(os.getenv("ALERT_TTL_MIN", "240")) * 60  # через сколько минут без отбоя сбросить тревогу
CLEAR_TTL = int(os.getenv("CLEAR_TTL_MIN", "30")) * 60   # сколько показывать отбой

def parse_channels(s):
    out = {}
    for part in s.split(","):
        part = part.strip().lstrip("@")
        if part:
            ch, _, reg = part.partition("=")
            out[ch.strip()] = reg.strip() or None
    return out

# CHANNELS="kanal1=Белгородская область,kanal2,kanal3=Курская область"
CHANNELS = parse_channels(os.getenv("CHANNELS", ""))

STATE = {n: {"d": [0, 0], "m": [0, 0]} for n in NAMES}  # [статус, время]; 1=опасность, 2=отбой
EVENTS, SEEN, HEALTH = deque(maxlen=40), set(), {}

def apply(post_id, ts, text, default):
    if post_id in SEEN:
        return
    SEEN.add(post_id)
    if time.time() - ts > ALERT_TTL:
        return
    for region, k, v in parse(text, default):
        if region in STATE and ts >= STATE[region][k][1]:
            STATE[region][k] = [v, ts]
            EVENTS.appendleft({"t": ts, "r": region, "k": k, "v": v})

async def fetch(client, ch):
    r = await client.get(f"https://t.me/s/{ch}")
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    posts = []
    for w in soup.select("div.tgme_widget_message"):
        body, tm = w.select_one("div.tgme_widget_message_text"), w.select_one("a.tgme_widget_message_date time")
        if not (w.get("data-post") and body and tm and tm.get("datetime")):
            continue
        for br in body.select("br"):
            br.replace_with("\n")
        posts.append((w["data-post"], datetime.fromisoformat(tm["datetime"]).timestamp(), body.get_text()))
    return sorted(posts, key=lambda p: p[1])

async def poller():
    headers = {"User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/120 Mobile Safari/537.36"}
    async with httpx.AsyncClient(timeout=20, headers=headers, follow_redirects=True) as c:
        while True:
            for ch, default in CHANNELS.items():
                try:
                    posts = await fetch(c, ch)
                    for pid, ts, text in posts:
                        apply(pid, ts, text, default)
                    HEALTH[ch] = {"ok": True, "posts": len(posts)}
                except Exception as e:
                    HEALTH[ch] = {"ok": False, "err": str(e)[:100]}
                await asyncio.sleep(2)
            await asyncio.sleep(POLL)

@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(poller())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

def view():
    now, out = time.time(), {}
    for n, s in STATE.items():
        row = {}
        for k in ("d", "m"):
            v, t = s[k]
            if v and now - t > (ALERT_TTL if v == 1 else CLEAR_TTL):
                v = 0
            row[k] = [v, int(now - t) if v else 0]
        out[n] = row
    return out

@app.get("/api/state")
def state():
    return JSONResponse({"regions": view(), "events": list(EVENTS)[:15], "channels": HEALTH, "configured": len(CHANNELS)},
                        headers={"Cache-Control": "no-store"})

@app.get("/api/parse")
def test_parse(text: str, default: str = ""):
    """Проверка парсера с телефона: /api/parse?text=Ракетная опасность в Курской области"""
    return parse(text, default or None)

@app.get("/")
def index():
    return HTMLResponse(HTML)

@app.get("/manifest.json")
def manifest():
    return {"name": "Радар опасности", "short_name": "Радар", "start_url": "/", "display": "standalone",
            "background_color": "#0b1117", "theme_color": "#0b1117",
            "icons": [{"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any"}]}

@app.get("/icon.svg")
def icon():
    svg = ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><rect width="100" height="100" rx="22" fill="#0b1117"/>'
           '<circle cx="50" cy="50" r="30" fill="none" stroke="#22c55e" stroke-width="4"/><circle cx="50" cy="50" r="16" fill="none" stroke="#22c55e" stroke-width="3"/>'
           '<path d="M50 50 L76 34" stroke="#f59e0b" stroke-width="5" stroke-linecap="round"/><circle cx="50" cy="50" r="4" fill="#ef4444"/></svg>')
    return Response(svg, media_type="image/svg+xml")
