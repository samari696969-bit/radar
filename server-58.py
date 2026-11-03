import asyncio, os, re, time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
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
("Тверская область",r"тверск|\bтвер(ь|и)\b"),("Ярославская область",r"ярославск|ярославл"),("Костромская область","костром"),("Республика Марий Эл","марий|йошкар"),("Чувашия","чуваш|чебоксар"),("Республика Адыгея","адыге|майкоп"),("Ингушетия","ингуш|магас|назран"),("Ивановская область",r"ивановск|\bиваново\b"),
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

# Конкретные типы. Чтобы добавить новый: строка здесь + строка в CATS в странице + ALL/FAMILY ниже.
SPECIFIC = {
    "h": re.compile(r"хорнет|hornet"),
    "t": re.compile(r"дартс|darts"),
    "v": re.compile(r"мвш"),
    "f": re.compile(r"фламинго|flamingo|фп-5|fp-5"),
    "s": re.compile(r"storm\s*shadow|шторм\s*ш[эе]доу|сторм\s*ш[эе]доу|scalp|скальп"),
}
ALL = ["d", "m", "h", "t", "v", "f", "s"]
FAMILY = {"d": ["h", "t", "v"], "m": ["f", "s"]}   # общий отбой снимает и конкретные типы семейства

def parse(text, default=None):
    """Возвращает список (регион, вид, 1=опасность | 2=отбой). Вид: d,m — общие; h,t,f,s — конкретные."""
    out, carry, pending = [], None, []
    for seg in re.split(r"[\n.;!]+", text):
        low = seg.lower().replace("ё", "е").strip()
        if not low:
            continue
        kinds = set()
        air = bool(AIR.search(low))
        if DRONE.search(low): kinds.add("d")
        if MISSILE.search(low): kinds.add("m")
        if air: kinds |= {"d", "m"}
        for k, rx in SPECIFIC.items():
            if rx.search(low): kinds.add(k)
        if not air:                                   # конкретный тип вытесняет общий
            if kinds & {"h", "t", "v"}: kinds.discard("d")
            if kinds & {"f", "s"}: kinds.discard("m")
        clear = bool(CLEAR.search(low))
        regs = [n for n, rx in REG if rx.search(low)]
        if clear or (kinds and ALERTW.search(low)):
            carry = (kinds or {"d", "m"}, 2 if clear else 1)
            if not regs:
                regs = pending or ([default] if default else [])
            pending = []
        elif kinds:
            carry, pending = None, []
            continue
        elif regs and not carry:
            pending += [r for r in regs if r not in pending]
            continue
        elif not regs:
            carry, pending = None, []
        if regs and carry:
            ks, v = carry
            out += [(r, k, v) for r in regs for k in sorted(ks)]
    return list(dict.fromkeys(out))

# ---------- СТРАНИЦА ----------
HTML = r'''<!DOCTYPE html>
<html lang="ru" data-theme="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<link rel="manifest" href="/manifest.json"><meta name="theme-color" content="#0b1117"><meta name="apple-mobile-web-app-capable" content="yes"><link rel="apple-touch-icon" href="/icon.svg">
<title>Радар опасности</title>
<style>
:root,:root[data-theme="dark"]{--bg:#0b1117;--fg:#e6edf3;--mut:#8b98a5;--tile:#17212b;--bd:#26323f;--da:#f59e0b;--dc:#22c55e;--ma:#ef4444;--mc:#38bdf8;--s:38px;color-scheme:dark;box-sizing:border-box;padding-top:env(safe-area-inset-top,0px);padding-bottom:env(safe-area-inset-bottom,0px)}
:root[data-theme="light"]{--bg:#f3f5f7;--fg:#14191f;--mut:#5b6773;--tile:#e2e7ec;--bd:#c9d1d9;color-scheme:light}
*{box-sizing:border-box}
html,body{margin:0;height:100%}
body{background:var(--bg);color:var(--fg);font:14px/1.3 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;display:flex;flex-direction:column}
header{padding:10px 12px 6px}
.top{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px}
h1{font-size:17px;margin:0}
.z{display:flex;flex-wrap:wrap;gap:4px;align-items:center}
.z button{min-width:40px;height:auto;border-radius:10px;border:1px solid var(--bd);background:var(--tile);color:var(--fg);font-size:12px;margin:0;padding:4px 6px;line-height:1.1;cursor:pointer}
.z button .ic{display:block;font-size:14px;text-align:center}
.z button .lb{display:block;font-size:9px;color:var(--mut);text-align:center}
.t{display:flex;flex-direction:column;align-items:center;justify-content:center;gap:1px}
.t .fg{width:42%;height:28%;object-fit:cover;border-radius:2px;pointer-events:none;box-shadow:0 0 0 1px rgba(0,0,0,.25)}
.t .cl{line-height:1.05;max-width:96%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}

.stats{display:flex;gap:6px}
.stat{flex:1;border:1px solid var(--bd);border-radius:10px;padding:5px 2px;text-align:center}
.stat b{display:block;font-size:19px}
.stat span{font-size:10px;color:var(--mut)}
#map-wrap{flex:1;overflow:auto;padding:8px 12px}
#map{display:grid;grid-template-columns:repeat(21,var(--s));grid-auto-rows:var(--s);gap:3px;width:max-content}
.t{border:0;border-radius:8px;background:var(--tile);color:var(--mut);font-size:calc(var(--s)*.26);font-weight:600;padding:0;cursor:pointer;-webkit-tap-highlight-color:transparent;overflow:hidden}
.t.on{color:#fff;text-shadow:0 1px 2px rgba(0,0,0,.55)}
.t.on{position:relative;z-index:1;animation:neon 1.8s ease-in-out infinite}
.t.al{animation-duration:1s}
.t.sel{z-index:2}
.t.sel{outline:2px solid var(--fg);outline-offset:1px}
@keyframes neon{
0%,100%{filter:brightness(1);box-shadow:inset 0 0 0 1px rgba(255,255,255,.45),0 0 3px 1px var(--g1),0 0 8px 1px var(--g2)}
50%{filter:brightness(1.35);box-shadow:inset 0 0 0 1px rgba(255,255,255,.85),0 0 8px 2px var(--g1),0 0 18px 4px var(--g2)}}
@media (prefers-reduced-motion:reduce){.t.on{animation:none;box-shadow:inset 0 0 0 1px rgba(255,255,255,.6),0 0 8px 2px var(--g1)}}
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
.chips{display:flex;gap:6px;overflow-x:auto;padding:8px 0 2px}
.chip{flex:none;display:flex;align-items:center;gap:4px;padding:6px 10px;border-radius:16px;border:1px solid var(--bd);background:var(--tile);color:var(--fg);font-size:12px;white-space:nowrap}
.chip.on{border-color:var(--fg);font-weight:600}
.chip i{width:9px;height:9px;border-radius:50%;display:inline-block}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px}
.t.on.lt{color:#14191f;text-shadow:none}
#map-wrap{position:relative;min-height:0}
#map-wrap.svg{overflow:hidden;padding:0}
#map.sv{position:absolute;inset:0;display:block;width:auto}
#map svg{width:100%;height:100%;display:block;touch-action:none;user-select:none}
.r{fill:var(--tile);stroke:var(--bd);stroke-width:.7;vector-effect:non-scaling-stroke;cursor:pointer}
.r.mk{stroke:var(--mut);stroke-width:1}
.r.on{stroke:rgba(255,255,255,.8);animation:sneon 1.8s ease-in-out infinite}
.r.al{animation-duration:1s}
.r.sel{stroke:var(--fg);stroke-width:2.2}
@keyframes sneon{
0%,100%{filter:brightness(1) drop-shadow(0 0 3px var(--g1)) drop-shadow(0 0 7px var(--g2))}
50%{filter:brightness(1.35) drop-shadow(0 0 7px var(--g1)) drop-shadow(0 0 16px var(--g2))}}
@media (prefers-reduced-motion:reduce){.r.on{animation:none}}
</style>
</head>
<body>
<header>
<div class="top"><h1 id="title">Радар опасности</h1>
<div class="z">
<button type="button" id="mode" title="Вид"><span class="ic" id="modeIc">▦</span><span class="lb" id="modeLb">ячейки</span></button>
<button type="button" id="flg" title="Флаги регионов"><span class="ic">🏳️</span><span class="lb" id="flgLb">флаги</span></button>
<button type="button" id="shot" title="Скрин"><span class="ic">📷</span><span class="lb">скрин</span></button>
<button type="button" id="lang" title="Язык"><span class="ic">🌐</span><span class="lb" id="langLb">RU</span></button>
<button type="button" id="thm" title="Тема"><span class="ic">◐</span><span class="lb">тема</span></button>
<button type="button" id="zp" title="+"><span class="ic">+</span></button>
<button type="button" id="zm" title="−"><span class="ic">−</span></button>
</div></div>
<div class="stats">
<div class="stat"><b id="cA" style="color:var(--ma)">0</b><span>Опасность · регионов</span></div>
<div class="stat"><b id="cC" style="color:var(--dc)">0</b><span>Отбой · регионов</span></div>
</div>
<div class="chips" id="chips"></div>
</header>
<div id="map-wrap"><div id="map"></div></div>
<footer>
<div id="sname">Нажмите на регион</div>
<div id="det" style="font-size:13px;color:var(--mut);min-height:34px;margin-bottom:4px"></div>
<div id="log"></div>
<div id="upd" style="font-size:11px;color:var(--mut);margin-top:6px">Загрузка…</div>
</footer>
<script>
var D=[
"Мурманская область|3|0|Мурм","Ненецкий АО|5|0|НАО","Ямало-Ненецкий АО|8|0|ЯНАО","Чукотский АО|20|0|Чук",
"Республика Карелия|3|1|Кар","Архангельская область|4|1|Арх","Республика Коми|5|1|Коми","Ханты-Мансийский АО|8|1|ХМАО","Красноярский край|11|1|Крас","Республика Саха (Якутия)|16|1|Якут","Магаданская область|18|1|Маг",
"Санкт-Петербург|1|2|СПб","Ленинградская область|2|2|ЛО","Вологодская область|4|2|Вол","Кировская область|5|2|Киров","Пермский край|6|2|Перм","Свердловская область|7|2|Свер","Тюменская область|8|2|Тюм","Томская область|9|2|Том","Иркутская область|13|2|Ирк","Хабаровский край|17|2|Хаб","Камчатский край|19|2|Камч",
"Псковская область|0|3|Пск","Новгородская область|1|3|Новг","Тверская область|2|3|Твер","Ярославская область|3|3|Яро","Костромская область|4|3|Кос","Удмуртия|5|3|Удм","Башкортостан|6|3|Башк","Челябинская область|7|3|Чел","Курганская область|8|3|Кург","Омская область|9|3|Омск","Новосибирская область|10|3|НСК","Кемеровская область|11|3|Кем","Хакасия|12|3|Хак","Республика Тыва|13|3|Тыва","Республика Бурятия|14|3|Бур","Забайкальский край|15|3|Заб","Амурская область|16|3|Амур","Еврейская АО|17|3|ЕАО","Сахалинская область|18|3|Сах",
"Калининградская область|0|4|Клгд","Смоленская область|1|4|Смол","Москва|2|4|Мск","Московская область|3|4|МО","Ивановская область|4|4|Иван","Владимирская область|5|4|Влд","Нижегородская область|6|4|НН","Республика Марий Эл|7|4|МЭл","Татарстан|8|4|Тат","Алтайский край|10|4|АлтК","Республика Алтай|11|4|АлтР","Приморский край|17|4|Прим",
"Брянская область|0|5|Брян","Калужская область|1|5|Калу","Тульская область|2|5|Тула","Рязанская область|3|5|Ряз","Республика Мордовия|4|5|Морд","Ульяновская область|5|5|Улья","Чувашия|6|5|Чув","Самарская область|7|5|Сам","Оренбургская область|8|5|Орен",
"Курская область|0|6|Курс","Орловская область|1|6|Орёл","Липецкая область|2|6|Лип","Тамбовская область|3|6|Тамб","Пензенская область|4|6|Пенз","Саратовская область|5|6|Сар",
"Белгородская область|0|7|Белг","Воронежская область|2|7|Вор","Волгоградская область|4|7|Волг","Астраханская область|5|7|Астр",
"Республика Крым|0|8|Крым","Краснодарский край|1|8|Крд","Ростовская область|2|8|Рост","Ставропольский край|3|8|Став","Республика Калмыкия|4|8|Калм",
"Севастополь|0|9|Сев","Республика Адыгея|1|9|Ады","Карачаево-Черкесия|2|9|КЧР","Кабардино-Балкария|3|9|КБР","Северная Осетия|4|9|СОс","Ингушетия|5|9|Инг","Чечня|6|9|Чеч","Дагестан|7|9|Даг"
].map(function(r){var p=r.split("|");return{n:p[0],c:+p[1],r:+p[2],l:p[3]}});

var CATS=[
{k:"d",n:"БПЛА",a:"#f59e0b",c:"#22c55e"},
{k:"m",n:"Ракеты",a:"#ef4444",c:"#38bdf8"},
{k:"h",n:"Hornet",a:"#a855f7",c:"#e9d5ff"},
{k:"t",n:"Darts",a:"#14b8a6",c:"#99f6e4"},
{k:"v",n:"МВШ",a:"#6366f1",c:"#c7d2fe"},
{k:"f",n:"Фламинго",a:"#ec4899",c:"#fbcfe8"},
{k:"s",n:"Storm Shadow",a:"#eab308",c:"#fef08a"}];
var NAMEOF={};CATS.forEach(function(c){NAMEOF[c.k]=c.n});
var MISS=["m","f","s"],S={},E={},sel=null,flt="",sz=38;
var map=document.getElementById("map"),nameEl=document.getElementById("sname"),det=document.getElementById("det"),logEl=document.getElementById("log"),upd=document.getElementById("upd"),chips=document.getElementById("chips"),CH={};
function zero(){var s={};CATS.forEach(function(c){s[c.k]=[0,0]});return s}
var showFlags=localStorage.getItem("regionFlags")==="1";
var LANGS=["ru","uk","en","be"];
var lang=(function(){var l=localStorage.getItem("lang");return LANGS.indexOf(l)>=0?l:"ru"})();
var mapMode=localStorage.getItem("mapMode")==="svg"?"svg":"grid";
var LOGO_SRC="data:image/png;base64,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";
var _logoImg=null;
function ensureLogo(cb){if(_logoImg&&_logoImg.complete){cb(_logoImg);return}var im=new Image();im.onload=function(){_logoImg=im;cb(im)};im.onerror=function(){cb(null)};im.src=LOGO_SRC}
/* краткие подписи + i18n */
var SHORT_OVR={"Ханты-Мансийский АО":"ХМАО","Ямало-Ненецкий АО":"ЯНАО","Ненецкий АО":"НАО","Чукотский АО":"Чук","Еврейская АО":"ЕАО","Республика Саха (Якутия)":"Якут","Санкт-Петербург":"СПб","Московская область":"МО","Ленинградская область":"ЛО"};
var REG_I18N={
uk:{"Москва":"Москва","Санкт-Петербург":"Санкт-Петербург","Московская область":"Московська область","Курская область":"Курська область","Белгородская область":"Білгородська область","Брянская область":"Брянська область","Краснодарский край":"Краснодарський край","Ростовская область":"Ростовська область","Республика Крым":"АР Крим","Севастополь":"Севастополь"},
en:{"Москва":"Moscow","Санкт-Петербург":"St. Petersburg","Московская область":"Moscow Oblast","Курская область":"Kursk Oblast","Белгородская область":"Belgorod Oblast","Брянская область":"Bryansk Oblast","Краснодарский край":"Krasnodar Krai","Ростовская область":"Rostov Oblast","Республика Крым":"Crimea","Севастополь":"Sevastopol"},
be:{"Москва":"Масква","Санкт-Петербург":"Санкт-Пецярбург","Московская область":"Маскоўская вобласць","Курская область":"Курская вобласць","Белгородская область":"Белгародская вобласць"}
};
var I18N={
ru:{title:"Радар опасности",danger:"Опасность · регионов",clear:"Отбой · регионов",click:"Нажмите на регион",cells:"ячейки",map:"карта",flags:"флаги"},
uk:{title:"Радар небезпеки",danger:"Небезпека · регіонів",clear:"Відбій · регіонів",click:"Натисніть на регіон",cells:"комірки",map:"карта",flags:"прапори"},
en:{title:"Threat radar",danger:"Alert · regions",clear:"All-clear · regions",click:"Tap a region",cells:"grid",map:"map",flags:"flags"},
be:{title:"Радар небяспекі",danger:"Небяспека · рэгіёнаў",clear:"Адбой · рэгіёнаў",click:"Націсніце на рэгіён",cells:"клеткі",map:"карта",flags:"сцягі"}
};
function t(k){return (I18N[lang]||I18N.ru)[k]||k}
function regionLabel(n){var m=REG_I18N[lang];return (m&&m[n])||n}
function cellLabel(n){
  if(lang==="ru"){if(SHORT_OVR[n])return SHORT_OVR[n];var x=D.find(function(d){return d.n===n});return x?x.l:n}
  var full=regionLabel(n);return full.length>10?full.slice(0,9)+"…":full;
}
var FLAGS={
"Москва":"Flag of Moscow, Russia.svg","Санкт-Петербург":"Flag of Saint Petersburg.svg","Севастополь":"Flag of Ukraine.svg",
"Московская область":"Flag of Moscow oblast.svg","Ленинградская область":"Flag of Leningrad Oblast.svg",
"Краснодарский край":"Flag of Krasnodar Krai.svg","Красноярский край":"Flag of Krasnoyarsk Krai.svg",
"Приморский край":"Flag of Primorsky Krai.svg","Хабаровский край":"Flag of Khabarovsk Krai.svg",
"Ставропольский край":"Flag of Stavropol Krai.svg","Пермский край":"Flag of Perm Krai.svg",
"Алтайский край":"Flag of Altai Krai.svg","Камчатский край":"Flag of Kamchatka Krai.svg",
"Забайкальский край":"Flag of Zabaykalsky Krai.svg","Республика Крым":"Flag of Ukraine.svg",
"Республика Адыгея":"Flag of Adygea.svg","Республика Калмыкия":"Flag of Kalmykia.svg",
"Кабардино-Балкария":"Flag of Kabardino-Balkaria.svg","Карачаево-Черкесия":"Flag of Karachay-Cherkessia.svg",
"Северная Осетия":"Flag of North Ossetia.svg","Ингушетия":"Flag of Ingushetia.svg","Чечня":"Flag of Chechnya.svg",
"Дагестан":"Flag of Dagestan.svg","Татарстан":"Flag of Tatarstan.svg","Башкортостан":"Flag of Bashkortostan.svg",
"Чувашия":"Flag of Chuvashia.svg","Удмуртия":"Flag of Udmurtia.svg","Республика Мордовия":"Flag of Mordovia.svg",
"Республика Марий Эл":"Flag of Mari El.svg","Республика Коми":"Flag of Komi.svg","Республика Карелия":"Flag of Karelia.svg",
"Хакасия":"Flag of Khakassia.svg","Республика Тыва":"Flag of Tuva.svg","Республика Бурятия":"Flag of Buryatia.svg",
"Республика Алтай":"Flag of Altai Republic.svg","Республика Саха (Якутия)":"Flag of Sakha.svg",
"Белгородская область":"Flag of Belgorod Oblast.svg","Курская область":"Flag of Kursk Oblast.svg",
"Брянская область":"Flag of Bryansk Oblast.svg","Воронежская область":"Flag of Voronezh Oblast.svg",
"Ростовская область":"Flag of Rostov Oblast.svg","Свердловская область":"Flag of Sverdlovsk Oblast.svg",
"Челябинская область":"Flag of Chelyabinsk Oblast.svg","Нижегородская область":"Flag of Nizhny Novgorod Oblast.svg",
"Самарская область":"Flag of Samara Oblast.svg","Саратовская область":"Flag of Saratov Oblast.svg",
"Волгоградская область":"Flag of Volgograd Oblast.svg","Астраханская область":"Flag of Astrakhan Oblast.svg",
"Калининградская область":"Flag of Kaliningrad Oblast.svg","Тульская область":"Flag of Tula Oblast.svg",
"Орловская область":"Flag of Oryol Oblast.svg","Липецкая область":"Flag of Lipetsk Oblast.svg",
"Тамбовская область":"Flag of Tambov Oblast.svg","Рязанская область":"Flag of Ryazan Oblast.svg",
"Калужская область":"Flag of Kaluga Oblast.svg","Смоленская область":"Flag of Smolensk Oblast.svg",
"Тверская область":"Flag of Tver Oblast.svg","Ярославская область":"Flag of Yaroslavl Oblast.svg",
"Владимирская область":"Flag of Vladimir Oblast.svg","Ивановская область":"Flag of Ivanovo Oblast.svg",
"Костромская область":"Flag of Kostroma Oblast.svg","Вологодская область":"Flag of Vologda Oblast.svg",
"Архангельская область":"Flag of Arkhangelsk Oblast.svg","Мурманская область":"Flag of Murmansk Oblast.svg",
"Псковская область":"Flag of Pskov Oblast.svg","Новгородская область":"Flag of Novgorod Oblast.svg",
"Кировская область":"Flag of Kirov Oblast.svg","Оренбургская область":"Flag of Orenburg Oblast.svg",
"Пензенская область":"Flag of Penza Oblast.svg","Ульяновская область":"Flag of Ulyanovsk Oblast.svg",
"Курганская область":"Flag of Kurgan Oblast.svg","Тюменская область":"Flag of Tyumen Oblast.svg",
"Омская область":"Flag of Omsk Oblast.svg","Новосибирская область":"Flag of Novosibirsk Oblast.svg",
"Томская область":"Flag of Tomsk Oblast.svg","Кемеровская область":"Flag of Kemerovo Oblast.svg",
"Иркутская область":"Flag of Irkutsk Oblast.svg","Амурская область":"Flag of Amur Oblast.svg",
"Магаданская область":"Flag of Magadan Oblast.svg","Сахалинская область":"Flag of Sakhalin Oblast.svg",
"Ненецкий АО":"Flag of Nenets Autonomous Okrug.svg","Ямало-Ненецкий АО":"Flag of Yamalo-Nenets Autonomous Okrug.svg",
"Ханты-Мансийский АО":"Flag of Khanty-Mansiysk.svg","Чукотский АО":"Flag of Chukotka.svg","Еврейская АО":"Flag of Jewish AO.svg"
};
function flagUrl(n){
  var f=FLAGS[n];if(!f)return "";
  return "https://commons.wikimedia.org/wiki/Special:FilePath/"+encodeURIComponent(f)+"?width=48";
}
function fillCell(b,n){
  var lab=cellLabel(n),url=showFlags?flagUrl(n):"";
  if(url){
    b.innerHTML="";
    var img=document.createElement("img");img.className="fg";img.alt="";img.loading="lazy";img.src=url;
    img.onerror=function(){this.style.display="none"};
    b.appendChild(img);
    var sp=document.createElement("span");sp.className="cl";sp.textContent=lab;b.appendChild(sp);
  }else b.textContent=lab;
  b.title=regionLabel(n);
}
function clearRegionFlags(){
  if(!svg)return;
  var old=svg.querySelectorAll("image.rflag");
  for(var i=0;i<old.length;i++)old[i].parentNode.removeChild(old[i]);
}
function placeRegionFlags(){
  clearRegionFlags();
  if(!showFlags||!svg||!G||!G.regions)return;
  G.regions.forEach(function(r){
    var url=flagUrl(r.n);if(!url||!r.c)return;
    var img=document.createElementNS(NS,"image");
    img.setAttribute("class","rflag");
    img.setAttributeNS("http://www.w3.org/1999/xlink","href",url);
    img.setAttribute("href",url);
    var w=14,h=9;
    img.setAttribute("x",(r.c[0]-w/2).toFixed(1));
    img.setAttribute("y",(r.c[1]-h/2).toFixed(1));
    img.setAttribute("width",String(w));img.setAttribute("height",String(h));
    img.setAttribute("preserveAspectRatio","xMidYMid slice");
    img.style.pointerEvents="none";img.style.opacity="0.9";
    svg.appendChild(img);
  });
}
function refreshCells(){
  D.forEach(function(x){var b=E[x.n];if(b){fillCell(b,x.n);paint(x.n)}});
}
function buildGrid(){
  SVGM=false;M={};svg=null;G=null;
  var wrap=document.getElementById("map-wrap");wrap.className="";map.className="";map.textContent="";E={};
  D.forEach(function(x){
    var b=document.createElement("button");
    b.className="t";
    b.style.gridColumn=x.c+1;b.style.gridRow=x.r+1;
    b.onclick=function(){select(x.n)};
    fillCell(b,x.n);
    map.appendChild(b);E[x.n]=b;if(!S[x.n])S[x.n]=zero();
  });
  D.forEach(function(x){paint(x.n)});count();updateModeBtn();
}
D.forEach(function(x){
  var b=document.createElement("button");
  b.className="t";
  b.style.gridColumn=x.c+1;b.style.gridRow=x.r+1;
  b.onclick=function(){select(x.n)};
  fillCell(b,x.n);
  map.appendChild(b);E[x.n]=b;S[x.n]=zero();
});

function vis(k){return !flt||flt===k}
var SVGM=false,NS="http://www.w3.org/2000/svg",defs=null,svg=null,M={},V={x:0,y:0,w:1000,h:500},G=null,moved=0,P={};
function grad(cols){
  var id="g"+cols.map(function(c){return c.replace("#","")}).join("_");
  if(!document.getElementById(id)){
    var g=document.createElementNS(NS,"linearGradient"),m=cols.length;
    g.id=id;g.setAttribute("x1","0");g.setAttribute("y1","0");g.setAttribute("x2","1");g.setAttribute("y2","1");
    cols.forEach(function(c,i){[i/m,(i+1)/m].forEach(function(o){var t=document.createElementNS(NS,"stop");t.setAttribute("offset",o);t.setAttribute("stop-color",c);g.appendChild(t)})});
    defs.appendChild(g);
  }
  return "url(#"+id+")";
}
function paint(n){
  var s=S[n],cols=[],al=false;
  CATS.forEach(function(c){
    var v=s[c.k][0];if(!v||!vis(c.k))return;
    cols.push(v===1?c.a:c.c);if(v===1)al=true;
  });
  var m=cols.length,els=[E[n]];if(M[n])els.push(M[n]);
  els.forEach(function(b){
    if(!b)return;
    b.style.setProperty("--g1",m?cols[0]:"transparent");b.style.setProperty("--g2",m?cols[m-1]:"transparent");
    if(b instanceof SVGElement){
      b.setAttribute("class","r"+(b.__mk?" mk":"")+(m?" on":"")+(al?" al":"")+(sel===n?" sel":""));
      b.style.fill=!m?"":m===1?cols[0]:grad(cols);
    }else{
      b.className="t"+(m?" on":"")+(m&&!al?" lt":"")+(al?" al":"")+(sel===n?" sel":"");
      b.style.background=!m?"":m===1?cols[0]:"linear-gradient(135deg,"+cols.map(function(c,i){return c+" "+(i*100/m)+"% "+((i+1)*100/m)+"%"}).join(",")+")";
    }
  });
}
function ago(sec){var m=Math.round(sec/60);return m<1?"только что":m<60?m+" мин назад":Math.floor(m/60)+" ч "+(m%60)+" мин назад"}
function detail(){
  if(!sel)return;var s=S[sel],any=false;det.textContent="";
  CATS.forEach(function(c){
    var v=s[c.k][0];if(!v)return;any=true;
    var d=document.createElement("div"),dot=document.createElement("i");
    dot.className="dot";dot.style.background=v===1?c.a:c.c;d.appendChild(dot);
    d.appendChild(document.createTextNode((v===1?"опасность ":"отбой ")+c.n+" · "+ago(s[c.k][1])));det.appendChild(d);
  });
  if(!any)det.textContent="Нет данных";
}
function select(n){var o=sel;sel=n;if(o)paint(o);paint(n);nameEl.textContent=n;detail()}
function chip(k,label,a,c){
  var b=document.createElement("button"),sp=document.createElement("span");
  b.className="chip";
  if(a){[a,c].forEach(function(col){var i=document.createElement("i");i.style.background=col;b.appendChild(i)})}
  b.appendChild(sp);b.onclick=function(){flt=(flt===k?"":k);D.forEach(function(x){paint(x.n)});count()};
  chips.appendChild(b);CH[k]={b:b,sp:sp,label:label};
}
chip("","Все");CATS.forEach(function(c){chip(c.k,c.n,c.a,c.c)});
function count(){
  var A=0,C=0,cnt={};CATS.forEach(function(c){cnt[c.k]=0});
  D.forEach(function(x){
    var a=false,cl=false;
    CATS.forEach(function(c){var v=S[x.n][c.k][0];if(v===1){cnt[c.k]++;if(vis(c.k))a=true}else if(v===2&&vis(c.k))cl=true});
    if(a)A++;else if(cl)C++;
  });
  document.getElementById("cA").textContent=A;document.getElementById("cC").textContent=C;
  Object.keys(CH).forEach(function(k){var h=CH[k];
    h.sp.textContent=h.label+(k&&cnt[k]?" · "+cnt[k]:"");h.b.classList.toggle("on",flt===k)});
}
async function load(){
  try{
    var j=await (await fetch("/api/state",{cache:"no-store"})).json();
    D.forEach(function(x){var r=j.regions[x.n]||{},s=zero();CATS.forEach(function(c){if(r[c.k])s[c.k]=r[c.k]});S[x.n]=s;paint(x.n)});
    count();detail();logEl.textContent="";
    j.events.forEach(function(e){var d=document.createElement("div");
      d.textContent=new Date(e.t*1000).toTimeString().slice(0,5)+" · "+e.r+" — "+(e.v===1?"опасность ":"отбой ")+(NAMEOF[e.k]||e.k);logEl.appendChild(d)});
    var bad=Object.keys(j.channels).filter(function(c){return !j.channels[c].ok}).length;
    upd.textContent=j.configured?("Обновлено "+new Date().toTimeString().slice(0,5)+" · каналов: "+j.configured+(bad?" · с ошибкой: "+bad:"")):"Каналы не заданы (переменная CHANNELS на сервере)";
  }catch(e){upd.textContent="Нет связи с сервером"}
}
function scl(){var r=svg.getBoundingClientRect();return Math.min(r.width/V.w,r.height/V.h)}
function setV(){
  svg.setAttribute("viewBox",V.x+" "+V.y+" "+V.w+" "+V.h);
  var r=6.5/scl();Object.keys(M).forEach(function(n){M[n].setAttribute("r",r)});
}
function toU(px,py){var r=svg.getBoundingClientRect(),k=scl();return [V.x+(px-r.left-(r.width-V.w*k)/2)/k,V.y+(py-r.top-(r.height-V.h*k)/2)/k]}
function zoomAt(f,cx,cy){
  var nw=Math.max(G.w/25,Math.min(G.w*1.1,V.w*f)),q=nw/V.w;
  V.x=cx-(cx-V.x)*q;V.y=cy-(cy-V.y)*q;V.w=nw;V.h*=q;setV();
}
function zoom(d){
  if(SVGM){zoomAt(d>0?1/1.5:1.5,V.x+V.w/2,V.y+V.h/2);return}
  sz=Math.max(24,Math.min(64,sz+d));document.documentElement.style.setProperty("--s",sz+"px");
}
function useSvg(g){
  G=g;SVGM=true;E={};
  var wrap=document.getElementById("map-wrap");wrap.className="svg";map.className="sv";map.textContent="";
  svg=document.createElementNS(NS,"svg");
  defs=document.createElementNS(NS,"defs");svg.appendChild(defs);
  var gp=document.createElementNS(NS,"g"),gm=document.createElementNS(NS,"g");svg.appendChild(gp);svg.appendChild(gm);
  function mk(tag,n){var e=document.createElementNS(NS,tag);e.__n=n;e.onclick=function(){if(moved<8)select(n)};return e}
  g.regions.forEach(function(r){
    if(r.d){var p=mk("path",r.n);p.setAttribute("d",r.d);p.setAttribute("class","r");gp.appendChild(p);E[r.n]=p}
    if(r.s||!r.d){
      var c=mk("circle",r.n);c.__mk=true;c.setAttribute("cx",r.c[0]);c.setAttribute("cy",r.c[1]);c.setAttribute("class","r mk");gm.appendChild(c);M[r.n]=c;
      if(!E[r.n])E[r.n]=c;
    }
  });
  map.appendChild(svg);
  var mo=g.regions.filter(function(r){return r.n==="Москва"})[0],r0=svg.getBoundingClientRect();
  V={x:0,y:0,w:g.w*0.5,h:g.w*0.5*r0.height/Math.max(r0.width,1)};
  if(mo){V.x=Math.max(0,mo.c[0]-V.w*0.33);V.y=mo.c[1]-V.h/2}
  setV();
  svg.addEventListener("pointerdown",function(e){P[e.pointerId]={x:e.clientX,y:e.clientY};moved=0});
  svg.addEventListener("pointermove",function(e){
    var p=P[e.pointerId];if(!p)return;
    var ids=Object.keys(P);
    if(ids.length===1){var dx=e.clientX-p.x,dy=e.clientY-p.y,k=scl();moved+=Math.abs(dx)+Math.abs(dy);V.x-=dx/k;V.y-=dy/k;setV()}
    else if(ids.length===2){
      var o=P[ids[0]==e.pointerId?ids[1]:ids[0]],d0=Math.hypot(p.x-o.x,p.y-o.y),d1=Math.hypot(e.clientX-o.x,e.clientY-o.y);
      if(d1>0){var u=toU((e.clientX+o.x)/2,(e.clientY+o.y)/2);zoomAt(d0/d1,u[0],u[1])}moved+=10;
    }
    p.x=e.clientX;p.y=e.clientY;
  });
  ["pointerup","pointercancel","pointerleave"].forEach(function(t){svg.addEventListener(t,function(e){delete P[e.pointerId]})});
  svg.addEventListener("wheel",function(e){e.preventDefault();var u=toU(e.clientX,e.clientY);zoomAt(e.deltaY>0?1.2:1/1.2,u[0],u[1])},{passive:false});
  D.forEach(function(x){paint(x.n)});count();
  placeRegionFlags();
  updateModeBtn();
}
async function loadGeo(t){
  try{
    var r=await fetch("/api/geo");
    if(r.ok){useSvg(await r.json());return}
  }catch(e){}
  if(t<30)setTimeout(function(){loadGeo(t+1)},20000);
}
function updateModeBtn(){
  var ic=document.getElementById("modeIc"),lb=document.getElementById("modeLb");
  if(mapMode==="svg"){if(ic)ic.textContent="▦";if(lb)lb.textContent=t("cells")}
  else{if(ic)ic.textContent="🗺️";if(lb)lb.textContent=t("map")}
}
function setMapMode(mode){
  mapMode=mode==="svg"?"svg":"grid";
  localStorage.setItem("mapMode",mapMode);
  if(mapMode==="svg")loadGeo(0);else buildGrid();
}
function applyI18n(){
  var h=document.getElementById("title");if(h)h.textContent=t("title");
  var st=document.querySelectorAll(".stat span");
  if(st[0])st[0].textContent=t("danger");if(st[1])st[1].textContent=t("clear");
  var sn=document.getElementById("sname");if(sn&&!sel)sn.textContent=t("click");
  var lb=document.getElementById("langLb");if(lb)lb.textContent=lang.toUpperCase();
  var fl=document.getElementById("flgLb");if(fl)fl.textContent=t("flags");
  updateModeBtn();
  if(!SVGM)refreshCells();
  else if(sel)try{select(sel)}catch(e){}
}
function takeShot(){
  var btn=document.getElementById("shot");if(btn){btn.disabled=true}
  ensureLogo(function(logo){
    try{
      var mapEl=document.getElementById("map-wrap");
      var origin=mapEl.getBoundingClientRect();
      var w=Math.floor(origin.width),h=Math.floor(origin.height);
      var dpr=Math.min(2,window.devicePixelRatio||1);
      var c=document.createElement("canvas");c.width=w*dpr;c.height=h*dpr;
      var ctx=c.getContext("2d");ctx.setTransform(dpr,0,0,dpr,0,0);
      var bg=getComputedStyle(document.documentElement).getPropertyValue("--bg").trim()||"#0b1117";
      ctx.fillStyle=bg;ctx.fillRect(0,0,w,h);
      if(SVGM){
        var svgEl=document.querySelector("#map svg");
        if(svgEl){
          var xml=new XMLSerializer().serializeToString(svgEl);
          if(xml.indexOf("xmlns")<0)xml=xml.replace("<svg",'<svg xmlns="http://www.w3.org/2000/svg"');
          var url=URL.createObjectURL(new Blob([xml],{type:"image/svg+xml;charset=utf-8"}));
          var im=new Image();
          im.onload=function(){
            ctx.drawImage(im,0,0,w,h);
            URL.revokeObjectURL(url);
            stamp(ctx,w,h,logo);save(c,btn);
          };
          im.onerror=function(){URL.revokeObjectURL(url);stamp(ctx,w,h,logo);save(c,btn)};
          im.src=url;return;
        }
      }else{
        mapEl.querySelectorAll(".t").forEach(function(el){
          var r=el.getBoundingClientRect();
          var x=r.left-origin.left,y=r.top-origin.top;
          var st=getComputedStyle(el);
          ctx.fillStyle=st.backgroundColor||"#17212b";
          ctx.strokeStyle="#64748b";ctx.lineWidth=1;
          ctx.fillRect(x,y,r.width,r.height);ctx.strokeRect(x+.5,y+.5,r.width-1,r.height-1);
          ctx.fillStyle=st.color||"#e6edf3";ctx.font="600 10px system-ui,sans-serif";
          ctx.textAlign="center";ctx.textBaseline="middle";
          ctx.fillText((el.textContent||"").slice(0,8),x+r.width/2,y+r.height/2);
        });
      }
      stamp(ctx,w,h,logo);save(c,btn);
    }catch(e){console.warn(e);if(btn)btn.disabled=false;alert("Скрин не удался")}
  });
}
function stamp(ctx,w,h,logo){
  var L=Math.min(64,Math.floor(w*0.14)),lx=w-L-12,ly=h-L-12;
  ctx.save();
  ctx.beginPath();ctx.arc(lx+L/2,ly+L/2,L/2,0,Math.PI*2);ctx.closePath();
  ctx.fillStyle="rgba(0,0,0,.35)";ctx.fill();
  if(logo){ctx.save();ctx.clip();ctx.drawImage(logo,lx,ly,L,L);ctx.restore()}
  ctx.beginPath();ctx.arc(lx+L/2,ly+L/2,L/2,0,Math.PI*2);
  ctx.strokeStyle="rgba(180,200,80,.6)";ctx.lineWidth=2;ctx.stroke();
  ctx.fillStyle="rgba(255,255,255,.9)";ctx.font="600 11px system-ui,sans-serif";
  ctx.textAlign="right";ctx.textBaseline="bottom";
  ctx.fillText("t.me/peace_ptica",w-12,ly-6);
  ctx.restore();
}
function save(c,btn){
  c.toBlob(function(blob){
    if(btn)btn.disabled=false;
    if(!blob)return;
    var a=document.createElement("a");
    a.href=URL.createObjectURL(blob);
    a.download="radar-"+new Date().toISOString().slice(0,19).replace(/[:T]/g,"-")+".png";
    a.click();setTimeout(function(){URL.revokeObjectURL(a.href)},2000);
  },"image/png");
}
document.getElementById("zp").onclick=function(){zoom(6)};
document.getElementById("zm").onclick=function(){zoom(-6)};
document.getElementById("mode").onclick=function(){setMapMode(mapMode==="svg"?"grid":"svg")};
document.getElementById("flg").onclick=function(){
  showFlags=!showFlags;localStorage.setItem("regionFlags",showFlags?"1":"0");
  if(SVGM)placeRegionFlags();else refreshCells();
};
document.getElementById("lang").onclick=function(){
  lang=LANGS[(LANGS.indexOf(lang)+1)%LANGS.length];
  localStorage.setItem("lang",lang);applyI18n();
};
document.getElementById("thm").onclick=function(){
  var cur=document.documentElement.getAttribute("data-theme")==="light"?"light":"dark";
  var next=cur==="dark"?"light":"dark";
  document.documentElement.setAttribute("data-theme",next);
  localStorage.setItem("theme",next);
};
document.getElementById("shot").onclick=function(){takeShot()};
(function(){var th=localStorage.getItem("theme");if(th==="light"||th==="dark")document.documentElement.setAttribute("data-theme",th)})();
applyI18n();
count();load();setInterval(load,10000);
if(mapMode==="svg")loadGeo(0);

</script>
</body>
</html>
'''

# ---------- ГРАНИЦЫ РЕГИОНОВ ----------
import math
GEO_URL = os.getenv("GEO_URL", "https://raw.githubusercontent.com/codeforgermany/click_that_hood/main/public/data/russia.geojson")
# Опорная точка (широта, долгота) внутри каждого региона: по ней контур из файла сопоставляется с регионом (названия не нужны)
REF = {
"Мурманская область":(68.97,33.08),"Ненецкий АО":(67.64,53.0),"Ямало-Ненецкий АО":(66.53,66.6),"Чукотский АО":(64.73,177.5),
"Республика Карелия":(61.79,34.37),"Архангельская область":(61.07,42.1),"Республика Коми":(61.67,50.8),"Ханты-Мансийский АО":(61.0,69.0),
"Красноярский край":(56.01,92.85),"Республика Саха (Якутия)":(62.03,129.73),"Магаданская область":(62.9,152.4),
"Санкт-Петербург":(59.93,30.32),"Ленинградская область":(59.64,33.5),"Вологодская область":(59.22,39.9),"Кировская область":(58.6,49.66),
"Пермский край":(58.0,56.25),"Свердловская область":(56.84,60.6),"Тюменская область":(57.15,65.53),"Томская область":(56.5,84.97),
"Иркутская область":(52.29,104.3),"Хабаровский край":(48.48,135.08),"Камчатский край":(56.3,160.85),
"Псковская область":(57.82,28.33),"Новгородская область":(58.52,31.27),"Тверская область":(56.86,35.9),"Ярославская область":(57.63,39.87),
"Костромская область":(57.77,40.93),"Удмуртия":(56.85,53.2),"Башкортостан":(54.74,55.97),"Челябинская область":(55.16,61.4),
"Курганская область":(55.44,65.34),"Омская область":(54.99,73.37),"Новосибирская область":(55.03,82.92),"Кемеровская область":(55.35,86.09),
"Хакасия":(53.72,91.44),"Республика Тыва":(51.72,94.45),"Республика Бурятия":(51.83,107.6),"Забайкальский край":(52.03,113.5),
"Амурская область":(51.38,128.13),"Еврейская АО":(48.8,132.9),"Сахалинская область":(46.96,142.73),
"Калининградская область":(54.71,20.5),"Смоленская область":(54.78,32.05),"Москва":(55.75,37.62),"Московская область":(56.3,38.13),
"Владимирская область":(56.13,40.4),"Ивановская область":(57.0,40.97),"Нижегородская область":(56.33,44.0),"Республика Марий Эл":(56.63,47.88),
"Чувашия":(56.13,47.25),"Татарстан":(55.8,49.1),"Алтайский край":(53.35,83.77),"Республика Алтай":(51.96,85.96),"Приморский край":(43.8,131.95),
"Брянская область":(53.25,34.37),"Калужская область":(54.51,36.26),"Тульская область":(54.2,37.62),"Рязанская область":(54.63,39.74),
"Республика Мордовия":(54.18,45.18),"Ульяновская область":(54.32,48.4),"Самарская область":(53.2,50.15),"Оренбургская область":(51.77,55.1),
"Курская область":(51.73,36.19),"Орловская область":(52.97,36.07),"Липецкая область":(52.6,39.57),"Тамбовская область":(52.72,41.45),
"Пензенская область":(53.2,45.0),"Саратовская область":(51.53,46.03),"Белгородская область":(50.6,36.59),"Воронежская область":(51.67,39.18),
"Волгоградская область":(48.7,44.5),"Астраханская область":(46.35,48.04),"Республика Крым":(44.95,34.1),"Краснодарский край":(45.04,38.98),
"Ростовская область":(47.23,39.7),"Ставропольский край":(45.04,41.97),"Республика Калмыкия":(46.31,44.27),"Севастополь":(44.6,33.52),
"Республика Адыгея":(44.6,40.1),"Карачаево-Черкесия":(44.22,42.06),"Кабардино-Балкария":(43.5,43.6),"Северная Осетия":(43.03,44.68),
"Ингушетия":(43.2,44.85),"Чечня":(43.32,45.69),"Дагестан":(42.82,47.12),
}
# запасной контур Крыма (грубый), если в файле его нет
CRIMEA = [(46.12,33.6),(45.95,34.5),(45.6,35.0),(45.4,35.7),(45.4,36.5),(45.3,36.65),(45.05,36.3),(45.0,35.4),(44.85,35.0),(44.65,34.4),
          (44.5,34.15),(44.4,33.75),(44.65,33.5),(45.0,33.45),(45.2,33.35),(45.35,32.5),(45.7,32.9),(46.05,33.3)]
# запасной контур Севастополя (грубый)
SEVASTOPOL = [(44.72,33.35),(44.70,33.55),(44.65,33.70),(44.58,33.68),(44.52,33.55),(44.50,33.40),(44.55,33.28),(44.65,33.25),(44.72,33.35)]
GEO, GEO_INFO = None, {"status": "loading"}

def _inring(x, y, ring):
    c, j = False, len(ring) - 1
    for i in range(len(ring)):
        xi, yi, xj, yj = ring[i][0], ring[i][1], ring[j][0], ring[j][1]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi) + xi:
            c = not c
        j = i
    return c

def _inpoly(x, y, poly):
    return _inring(x, y, poly[0]) and not any(_inring(x, y, h) for h in poly[1:])

def _polys(g):
    t = g.get("type")
    return [g["coordinates"]] if t == "Polygon" else (g["coordinates"] if t == "MultiPolygon" else [])

def _dp(pts, eps):
    if len(pts) < 4:
        return pts
    keep, stack = [False] * len(pts), [(0, len(pts) - 1)]
    keep[0] = keep[-1] = True
    while stack:
        a, b = stack.pop()
        (x1, y1), (x2, y2) = pts[a], pts[b]
        dx, dy = x2 - x1, y2 - y1
        L = math.hypot(dx, dy) or 1e-9
        md, mi = 0, -1
        for i in range(a + 1, b):
            d = abs(dy * (pts[i][0] - x1) - dx * (pts[i][1] - y1)) / L
            if d > md:
                md, mi = d, i
        if md > eps:
            keep[mi] = True
            stack += [(a, mi), (mi, b)]
    return [p for p, k in zip(pts, keep) if k]

def _dpring(pts, eps):
    """Упрощение замкнутого контура: делим по самой дальней точке (иначе первая и последняя совпадают и всё схлопывается)."""
    if len(pts) > 1 and pts[0] == pts[-1]:
        pts = pts[:-1]
    if len(pts) < 4:
        return pts
    m = max(range(1, len(pts)), key=lambda i: (pts[i][0] - pts[0][0]) ** 2 + (pts[i][1] - pts[0][1]) ** 2)
    return _dp(pts[:m + 1], eps)[:-1] + _dp(pts[m:] + [pts[0]], eps)[:-1]

def _albers(lon, lat):
    if lon < 0:
        lon += 360
    p1, p2, p0, l0 = map(math.radians, (52, 68, 45, 100))
    n = (math.sin(p1) + math.sin(p2)) / 2
    C = math.cos(p1) ** 2 + 2 * n * math.sin(p1)
    r0 = math.sqrt(C - 2 * n * math.sin(p0)) / n
    r = math.sqrt(C - 2 * n * math.sin(math.radians(lat))) / n
    th = n * (math.radians(lon) - l0)
    return r * math.sin(th), -(r0 - r * math.cos(th))   # y вниз

def _area(r):
    return abs(sum(r[i][0] * r[i - 1][1] - r[i - 1][0] * r[i][1] for i in range(len(r)))) / 2

def build_geo(data):
    feats = []
    for f in data.get("features", []):
        polys = _polys(f.get("geometry") or {})
        if polys:
            boxes = [(min(p[0] for p in pl[0]), min(p[1] for p in pl[0]), max(p[0] for p in pl[0]), max(p[1] for p in pl[0])) for pl in polys]
            feats.append((f.get("properties") or {}, polys, boxes))
    owner = {}
    for name, (lat, lon) in REF.items():
        best = None
        for i, (_, polys, boxes) in enumerate(feats):
            for pl, b in zip(polys, boxes):
                if b[0] <= lon <= b[2] and b[1] <= lat <= b[3] and _inpoly(lon, lat, pl):
                    a = (b[2] - b[0]) * (b[3] - b[1])
                    if best is None or a < best[0]:
                        best = (a, i)
                    break
        if best:
            owner[name] = best[1]
    if len(owner) < 40:      # файл не подошёл: оставляем плитки и показываем, что внутри
        allc = [p for _, polys, _ in feats for pl in polys for p in pl[0][:50]]
        types = {}
        for f in data.get("features", []):
            t = (f.get("geometry") or {}).get("type")
            types[t] = types.get(t, 0) + 1
        return None, {"status": "bad_match", "features": len(feats), "matched": len(owner), "geometry_types": types,
                      "names": [str(list((f.get("properties") or {}).values())[:3]) for f in data.get("features", [])[:8]],
                      "lon_range": [min(p[0] for p in allc), max(p[0] for p in allc)] if allc else None,
                      "lat_range": [min(p[1] for p in allc), max(p[1] for p in allc)] if allc else None}
    raw = {}   # регион -> список колец [(x, y)]
    for name, i in owner.items():
        raw[name] = [[_albers(x, y) for x, y in pl[0]] for pl in feats[i][1]]
    fallback = [n for n in REF if n not in owner]
    crim = [_albers(lo, la) for la, lo in CRIMEA] if "Республика Крым" in fallback else None
    sev  = [_albers(lo, la) for la, lo in SEVASTOPOL] if "Севастополь" in fallback else None
    pts = {n: _albers(lo, la) for n, (la, lo) in REF.items() if n in fallback}
    allp = [p for rs in raw.values() for r in rs for p in r] + (crim or []) + (sev or []) + list(pts.values())
    x0, x1, y0, y1 = min(p[0] for p in allp), max(p[0] for p in allp), min(p[1] for p in allp), max(p[1] for p in allp)
    k = 1000 / (x1 - x0)
    tr = lambda p: ((p[0] - x0) * k, (p[1] - y0) * k)
    def path(rings):
        sc = [[tr(p) for p in r] for r in rings]
        big = max(_area(r) for r in sc)
        out = []
        for r in sc:
            a = _area(r)
            if a < 0.15 and a < big:
                continue
            q = _dpring(r, 0.25 if a < 30 else 0.4)
            if len(q) < 3:
                q = r
            out.append("M" + "L".join("%.1f %.1f" % p for p in q) + "Z")
        return "".join(out)
    regs = []
    for name, rings in raw.items():
        tp = [tr(p) for r in rings for p in r]
        bx = (min(p[0] for p in tp), min(p[1] for p in tp), max(p[0] for p in tp), max(p[1] for p in tp))
        big = max(rings, key=_area)
        cx = sum(tr(p)[0] for p in big) / len(big); cy = sum(tr(p)[1] for p in big) / len(big)
        regs.append({"n": name, "d": path(rings), "c": [round(cx, 1), round(cy, 1)], "s": (bx[2] - bx[0]) * (bx[3] - bx[1]) < 120, "a": (bx[2] - bx[0]) * (bx[3] - bx[1])})
    if crim:
        t = [tr(p) for p in crim]
        regs.append({"n": "Республика Крым", "d": "M" + "L".join("%.1f %.1f" % p for p in t) + "Z",
                     "c": [round(sum(p[0] for p in t) / len(t), 1), round(sum(p[1] for p in t) / len(t), 1)], "s": True, "a": 50, "approx": True})
        fallback.remove("Республика Крым")
    if sev:
        t = [tr(p) for p in sev]
        regs.append({"n": "Севастополь", "d": "M" + "L".join("%.1f %.1f" % p for p in t) + "Z",
                     "c": [round(sum(p[0] for p in t) / len(t), 1), round(sum(p[1] for p in t) / len(t), 1)], "s": True, "a": 5, "approx": True})
        fallback.remove("Севастополь")
    for n in fallback:       # нет контура -> маленькая точка на карте
        x, y = tr(pts[n]) if n in pts else (0, 0)
        regs.append({"n": n, "d": "", "c": [round(x, 1), round(y, 1)], "s": True, "a": 1, "dot": True})
    regs.sort(key=lambda r: -r["a"])
    for r in regs:
        r.pop("a")
    h = max(tr(p)[1] for p in allp)
    shared = sorted(set(n for n, i in owner.items() if list(owner.values()).count(i) > 1))
    info = {"status": "ok", "features": len(feats), "matched": len(owner), "no_outline": fallback, "shared_outline": shared,
            "props_example": list((feats[0][0] if feats else {}).keys())[:10]}
    return {"w": 1000, "h": round(h + 5, 1), "regions": regs}, info

async def load_geo():
    global GEO, GEO_INFO
    while GEO is None:
        try:
            async with httpx.AsyncClient(timeout=90, follow_redirects=True) as c:
                r = await c.get(GEO_URL)
                r.raise_for_status()
            GEO, GEO_INFO = await asyncio.to_thread(build_geo, r.json())
            if GEO is None:
                await asyncio.sleep(1800)
        except Exception as e:
            GEO_INFO = {"status": "error", "err": str(e)[:200]}
            await asyncio.sleep(120)

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

STATE = {n: {k: [0, 0] for k in ALL} for n in NAMES}  # [статус, время]; 1=опасность, 2=отбой
EVENTS, SEEN, HEALTH = deque(maxlen=40), set(), {}

def apply(post_id, ts, text, default):
    if post_id in SEEN:
        return
    SEEN.add(post_id)
    if time.time() - ts > ALERT_TTL:
        return
    for region, k, v in parse(text, default):
        if region not in STATE:
            continue
        targets = [k]
        if v == 2 and k in FAMILY:   # общий отбой снимает активные конкретные типы
            targets += [x for x in FAMILY[k] if STATE[region][x][0] == 1]
        for kk in targets:
            if ts >= STATE[region][kk][1]:
                STATE[region][kk] = [v, ts]
                EVENTS.appendleft({"t": ts, "r": region, "k": kk, "v": v})

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
    tasks = [asyncio.create_task(poller()), asyncio.create_task(load_geo())]
    yield
    for t in tasks:
        t.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1000)

def view():
    now, out = time.time(), {}
    for n, s in STATE.items():
        row = {}
        for k in ALL:
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

@app.get("/api/geo")
def geo():
    return JSONResponse(GEO if GEO else {"ok": False}, status_code=200 if GEO else 503,
                        headers={"Cache-Control": "no-store"})

@app.get("/api/geo-debug")
def geo_debug():
    """Диагностика границ: какие регионы не нашли контур и т.п."""
    return GEO_INFO

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
