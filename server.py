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
html,body{margin:0;height:100%;overflow:hidden}
body{background:var(--bg);color:var(--fg);font:300 14px/1.3 "Comic Sans MS","Comic Sans",cursive;display:flex;flex-direction:column;overflow:hidden}
header{padding:10px 12px 6px;position:relative;z-index:5;background:var(--bg);flex:none}
.top{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px}
h1{font-size:17px;font-weight:300;margin:0}
.z{display:flex;flex-direction:row;flex-wrap:nowrap;gap:4px;margin-top:8px;margin-bottom:2px;padding:6px;border-radius:14px;background:var(--tile);border:1px solid var(--bd);box-shadow:0 4px 14px rgba(0,0,0,.3);width:max-content;max-width:100%;overflow:hidden;isolation:isolate}
.z button{width:auto;min-width:56px;height:auto;min-height:40px;border-radius:10px;border:1px solid var(--bd);background:var(--bg);color:var(--fg);font-size:14px;margin:0;padding:6px 8px;line-height:1.1;cursor:pointer;flex:none}
.z button .ic{display:block;font-size:15px}
.z button .lb{display:block;font-size:9px;color:var(--mut);margin-top:2px}
.stats{display:flex;gap:6px}
.stat{flex:1;border:1px solid var(--bd);border-radius:10px;padding:5px 2px;text-align:center}
.stat b{display:block;font-size:19px}
.stat span{font-size:10px;color:var(--mut)}
#map-wrap{flex:1;min-height:0;overflow:hidden;padding:8px 12px;overscroll-behavior:contain;position:relative;z-index:1}
#map-wrap:not(.svg){overflow:auto;-webkit-overflow-scrolling:touch}
#map{display:grid;grid-template-columns:repeat(21,var(--s));grid-auto-rows:var(--s);gap:3px;width:max-content}
.t{border:0;border-radius:8px;background:var(--tile);color:var(--mut);font-size:calc(var(--s)*.26);font-weight:600;padding:0;cursor:pointer;-webkit-tap-highlight-color:transparent;overflow:hidden;transition:background .55s ease,color .4s ease,box-shadow .55s ease,filter .55s ease,transform .15s ease}
.t.on{color:#fff;text-shadow:0 1px 2px rgba(0,0,0,.55);position:relative;z-index:1;animation:neon 1.8s ease-in-out infinite}
.t.al{animation-duration:1s}
.t.sel{z-index:2;outline:2px solid var(--fg);outline-offset:1px}
@keyframes neon{
0%,100%{filter:brightness(1);box-shadow:inset 0 0 0 1px rgba(255,255,255,.45),0 0 4px 1px var(--g1),0 0 10px 2px var(--g2)}
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
#log{max-height:60px;overflow:auto;font-size:12px;color:var(--mut);margin-top:6px;min-height:0}
#log.hide{visibility:hidden;max-height:0;margin:0;pointer-events:none}
.chips{display:flex;gap:6px;overflow-x:auto;padding:8px 0 2px}
.chip{flex:none;display:flex;align-items:center;gap:4px;padding:6px 10px;border-radius:16px;border:1px solid var(--bd);background:var(--tile);color:var(--fg);font-size:12px;white-space:nowrap}
.chip.on{border-color:var(--fg);font-weight:600}
.chip i{width:9px;height:9px;border-radius:50%;display:inline-block}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px}
.t.on.lt{color:#14191f;text-shadow:none}
#map-wrap{position:relative;min-height:0}
#map-wrap.svg{overflow:hidden;padding:0}
#map.sv{position:absolute;inset:0;display:block;width:auto}
#tip{display:none;position:absolute;z-index:8;left:8px;bottom:8px;top:auto;right:auto;min-width:160px;max-width:min(260px,80vw);padding:10px 12px;border-radius:12px;background:var(--tile);border:1px solid var(--bd);box-shadow:0 8px 28px rgba(0,0,0,.45);pointer-events:none;color:var(--fg);font-size:13px;line-height:1.35}
#tip.on{display:block}
#tip .th{display:flex;align-items:center;gap:8px;font-weight:600;margin-bottom:6px}
#tip .fl{width:36px;height:24px;border-radius:3px;background:var(--bd);display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:var(--fg);flex:none;overflow:hidden;border:1px solid var(--bd)}
#tip .fl img{width:100%;height:100%;object-fit:cover;display:block}
#tip .st{font-size:12px;color:var(--mut)}
#tip .st b{color:var(--fg);font-weight:600}
#tip .hi{margin-top:6px;padding-top:6px;border-top:1px solid var(--bd);font-size:11px;color:var(--mut);max-height:72px;overflow:hidden}
#tip .hi div{margin:2px 0}
#map svg{width:100%;height:100%;display:block;touch-action:none;user-select:none}
.r{fill:var(--tile);stroke:var(--bd);stroke-width:1.4;vector-effect:non-scaling-stroke;cursor:pointer;stroke-linejoin:round;stroke-linecap:round;transition:fill .55s ease,stroke .4s ease,stroke-width .3s ease,filter .55s ease}
.r.mk{stroke:var(--mut);stroke-width:1.6}
.r.on{stroke:rgba(255,255,255,.85)}
.r.al{}
.r.sel{stroke:var(--fg);stroke-width:1.8}
/* свечение под заливкой: видно на пустых соседях, перекрывается соседними "опасными" */
.r.glow{fill:var(--g1,#f59e0b);stroke:none;pointer-events:none;opacity:.9;animation:gneon 1.8s ease-in-out infinite}
.r.glow.al{animation-duration:1s}
@keyframes gneon{
0%,100%{filter:brightness(1) blur(.4px) drop-shadow(0 0 4px var(--g1)) drop-shadow(0 0 10px var(--g2))}
50%{filter:brightness(1.25) blur(.6px) drop-shadow(0 0 8px var(--g1)) drop-shadow(0 0 18px var(--g2))}}
@media (prefers-reduced-motion:reduce){.r.glow{animation:none;filter:brightness(1.1) drop-shadow(0 0 6px var(--g1)) drop-shadow(0 0 12px var(--g2))}}
#map-wrap.gl{overflow:hidden;padding:0}
#map.gl{position:absolute;inset:0;display:block;width:auto}
#map canvas{width:100%;height:100%;display:block;touch-action:none;user-select:none}
#gmsg{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:var(--mut);font-size:13px;text-align:center;padding:20px}
.lbl{fill:var(--fg);paint-order:stroke;stroke:var(--bg);stroke-width:.3em;stroke-linejoin:round;pointer-events:none;font-weight:600;opacity:.92;text-anchor:middle;dominant-baseline:central;font-family:"Comic Sans MS","Comic Sans",cursive;font-weight:300}
</style>
</head>
<body>
<header>
<div class="top"><h1>Радар опасности</h1></div>
<div class="stats">
<div class="stat"><b id="cA" style="color:var(--ma)">0</b><span>Опасность · регионов</span></div>
<div class="stat"><b id="cC" style="color:var(--dc)">0</b><span>Отбой · регионов</span></div>
</div>
<div class="chips" id="chips"></div>
<div class="z">
<button id="mode" title="Вид карты"><span class="ic" id="modeIc">🗺️</span><span class="lb" id="modeLb">карта</span></button>
<button id="thm" title="Тема"><span class="ic">◐</span><span class="lb">тема</span></button>
<button id="zp" title="Крупнее"><span class="ic">+</span><span class="lb">крупнее</span></button>
<button id="zm" title="Мельче"><span class="ic">−</span><span class="lb">мельче</span></button>
</div>
</header>
<div id="map-wrap"><div id="map"></div><div id="tip"></div></div>
<footer>
<div id="sname">Нажмите на регион</div>
<div id="det" style="font-size:13px;color:var(--mut);min-height:34px;margin-bottom:4px"></div>
<div id="log"></div>
<div id="upd" style="font-size:11px;color:var(--mut);margin-top:6px">Загрузка…</div>
</footer>
<script>
(function(){
  var t=localStorage.getItem("theme");
  if(t!=="light"&&t!=="dark")t="dark";
  document.documentElement.setAttribute("data-theme",t);
  var m=document.querySelector('meta[name="theme-color"]');
  if(m)m.content=t==="light"?"#f3f5f7":"#0b1117";
})();
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
var mapMode=(function(){var m=localStorage.getItem("mapMode");return m==="grid"||m==="globe"?m:"svg"})();
var map=document.getElementById("map"),nameEl=document.getElementById("sname"),det=document.getElementById("det"),logEl=document.getElementById("log"),upd=document.getElementById("upd"),chips=document.getElementById("chips"),CH={};
function zero(){var s={};CATS.forEach(function(c){s[c.k]=[0,0]});return s}
D.forEach(function(x){S[x.n]=zero()});
function updateModeBtn(){
  var ic=document.getElementById("modeIc"),lb=document.getElementById("modeLb");
  var nx=mapMode==="svg"?"grid":mapMode==="grid"?"globe":"svg";
  var T={grid:["▦","ячейки"],globe:["🌐","глобус"],svg:["🗺️","карта"]}[nx];
  if(ic)ic.textContent=T[0];if(lb)lb.textContent=T[1];
}
function buildGrid(){
  stopGlobe();gt=null;SVGM=false;E={};M={};GL={};gg=null;gBot=null;gTop=null;svg=null;hideTip();
  var wrap=document.getElementById("map-wrap");
  wrap.className="";map.className="";map.textContent="";
  D.forEach(function(x){
    var b=document.createElement("button");
    b.className="t";b.textContent=x.l;b.title=x.n;
    b.style.gridColumn=x.c+1;b.style.gridRow=x.r+1;
    b.onclick=function(ev){ev.preventDefault();select(x.n)};
    map.appendChild(b);E[x.n]=b;
  });
  D.forEach(function(x){paint(x.n)});count();
  updateModeBtn();
}
/* БПЛА (d) показывает также Hornet/Darts/МВШ; Ракеты (m) — Фламинго/Storm Shadow */
function vis(k){
  if(!flt)return true;
  if(flt===k)return true;
  if(flt==="d"&&(k==="h"||k==="t"||k==="v"))return true;
  if(flt==="m"&&(k==="f"||k==="s"))return true;
  return false;
}
var SVGM=false,NS="http://www.w3.org/2000/svg",defs=null,svg=null,M={},GL={},gg=null,gBot=null,gTop=null,V={x:0,y:0,w:1000,h:500},G=null,moved=0,P={},gt=null,LBL=[],GLB=null,GTOK=0,WD=null;
var HIST={},LABEL={};D.forEach(function(x){LABEL[x.n]=x.l});
var SHORT_OVR={"Ханты-Мансийский АО":"ХМАО","Ямало-Ненецкий АО":"ЯНАО","Ненецкий АО":"НАО","Чукотский АО":"Чукотка","Еврейская АО":"ЕАО","Республика Саха (Якутия)":"Якутия","Санкт-Петербург":"СПб","Республика Алтай":"Респ. Алтай","Северная Осетия":"Сев. Осетия","Карачаево-Черкесия":"Карачаево-Черк.","Кабардино-Балкария":"Кабард.-Балк."};
function shortName(n){return SHORT_OVR[n]||n.replace(/ область$| край$/,"").replace(/^Республика /,"")}
function stopGlobe(){if(GLB){GLB.stop();GLB=null}}
var tip=document.getElementById("tip");
/* флаги субъектов с Wikimedia Commons */
var FLAGS={
"Москва":"Flag of Moscow, Russia.svg",
"Санкт-Петербург":"Flag of Saint Petersburg.svg",
"Севастополь":"Flag of Sevastopol.svg",
"Московская область":"Flag of Moscow oblast.svg",
"Ленинградская область":"Flag of Leningrad Oblast.svg",
"Краснодарский край":"Flag of Krasnodar Krai.svg",
"Красноярский край":"Flag of Krasnoyarsk Krai.svg",
"Приморский край":"Flag of Primorsky Krai.svg",
"Хабаровский край":"Flag of Khabarovsk Krai.svg",
"Ставропольский край":"Flag of Stavropol Krai.svg",
"Пермский край":"Flag of Perm Krai.svg",
"Алтайский край":"Flag of Altai Krai.svg",
"Камчатский край":"Flag of Kamchatka Krai.svg",
"Забайкальский край":"Flag of Zabaykalsky Krai.svg",
"Республика Крым":"Flag of Crimea.svg",
"Республика Адыгея":"Flag of Adygea.svg",
"Республика Калмыкия":"Flag of Kalmykia.svg",
"Республика Карелия":"Flag of Karelia.svg",
"Республика Коми":"Flag of Komi.svg",
"Республика Марий Эл":"Flag of Mari El.svg",
"Республика Мордовия":"Flag of Mordovia.svg",
"Республика Саха (Якутия)":"Flag of Sakha.svg",
"Республика Тыва":"Flag of Tuva.svg",
"Республика Алтай":"Flag of Altai Republic.svg",
"Республика Бурятия":"Flag of Buryatia.svg",
"Башкортостан":"Flag of Bashkortostan.svg",
"Татарстан":"Flag of Tatarstan.svg",
"Удмуртия":"Flag of Udmurtia.svg",
"Чувашия":"Flag of Chuvashia.svg",
"Хакасия":"Flag of Khakassia.svg",
"Дагестан":"Flag of Dagestan.svg",
"Чечня":"Flag of the Chechen Republic.svg",
"Ингушетия":"Flag of Ingushetia.svg",
"Кабардино-Балкария":"Flag of Kabardino-Balkaria.svg",
"Карачаево-Черкесия":"Flag of Karachay-Cherkessia.svg",
"Северная Осетия":"Flag of North Ossetia.svg",
"Ненецкий АО":"Flag of Nenets Autonomous District.svg",
"Ямало-Ненецкий АО":"Flag of Yamalo-Nenets Autonomous District.svg",
"Ханты-Мансийский АО":"Flag of Yugra.svg",
"Чукотский АО":"Flag of Chukotka.svg",
"Еврейская АО":"Flag of the Jewish Autonomous Oblast.svg",
"Белгородская область":"Flag of Belgorod Oblast.svg",
"Брянская область":"Flag of Bryansk Oblast.svg",
"Владимирская область":"Flag of Vladimir Oblast.svg",
"Волгоградская область":"Flag of Volgograd Oblast.svg",
"Вологодская область":"Flag of Vologda oblast.svg",
"Воронежская область":"Flag of Voronezh Oblast.svg",
"Ивановская область":"Flag of Ivanovo Oblast.svg",
"Иркутская область":"Flag of Irkutsk Oblast.svg",
"Калининградская область":"Flag of Kaliningrad Oblast.svg",
"Калужская область":"Flag of Kaluga Oblast.svg",
"Кемеровская область":"Flag of Kemerovo Oblast.svg",
"Кировская область":"Flag of Kirov Oblast.svg",
"Костромская область":"Flag of Kostroma Oblast.svg",
"Курганская область":"Flag of Kurgan Oblast.svg",
"Курская область":"Flag of Kursk Oblast.svg",
"Липецкая область":"Flag of Lipetsk Oblast.svg",
"Магаданская область":"Flag of Magadan Oblast.svg",
"Мурманская область":"Flag of Murmansk Oblast.svg",
"Нижегородская область":"Flag of Nizhny Novgorod Region.svg",
"Новгородская область":"Flag of Novgorod Oblast.svg",
"Новосибирская область":"Flag of Novosibirsk Oblast.svg",
"Омская область":"Flag of Omsk Oblast.svg",
"Оренбургская область":"Flag of Orenburg Oblast.svg",
"Орловская область":"Flag of Oryol Oblast.svg",
"Пензенская область":"Flag of Penza Oblast.svg",
"Псковская область":"Flag of Pskov Oblast.svg",
"Ростовская область":"Flag of Rostov Oblast.svg",
"Рязанская область":"Flag of Ryazan Oblast.svg",
"Самарская область":"Flag of Samara Oblast.svg",
"Саратовская область":"Flag of Saratov Oblast.svg",
"Сахалинская область":"Flag of Sakhalin Oblast.svg",
"Свердловская область":"Flag of Sverdlovsk Oblast.svg",
"Смоленская область":"Flag of Smolensk Oblast.svg",
"Тамбовская область":"Flag of Tambov Oblast.svg",
"Тверская область":"Flag of Tver Oblast.svg",
"Томская область":"Flag of Tomsk Oblast.svg",
"Тульская область":"Flag of Tula Oblast.svg",
"Тюменская область":"Flag of Tyumen Oblast.svg",
"Ульяновская область":"Flag of Ulyanovsk Oblast.svg",
"Челябинская область":"Flag of Chelyabinsk Oblast.svg",
"Ярославская область":"Flag of Yaroslavl Oblast.svg",
"Амурская область":"Flag of Amur Oblast.svg",
"Архангельская область":"Flag of Arkhangelsk Oblast.svg",
"Астраханская область":"Flag of Astrakhan Oblast.svg"
};
var WORLD_ISO={};
/* Бело-сине-белый флаг (оппозиция) + украинские флаги областей */
var FLAG_OVR={
  "Russia":"White-blue-white flag.svg",
  "Донецкая область":"Flag of Donetsk Oblast.svg",
  "Луганская область":"Flag of Luhansk Oblast.svg",
  "Херсонская область":"Flag of Kherson Oblast.svg",
  "Запорожская область":"Flag of Zaporizhia Oblast.svg",
  "Республика Крым":"Flag of Ukraine.svg"
};
function flagUrl(n){
  var ovr=FLAG_OVR[n];
  if(ovr)return "https://commons.wikimedia.org/wiki/Special:FilePath/"+encodeURIComponent(ovr)+"?width=64";
  var f=FLAGS[n];
  if(f)return "https://commons.wikimedia.org/wiki/Special:FilePath/"+encodeURIComponent(f)+"?width=64";
  var iso=WORLD_ISO[n];
  if(iso==="RU")return "https://commons.wikimedia.org/wiki/Special:FilePath/"+encodeURIComponent("White-blue-white flag.svg")+"?width=64";
  if(iso)return "https://flagcdn.com/w40/"+iso.toLowerCase()+".png";
  return "https://commons.wikimedia.org/wiki/Special:FilePath/"+encodeURIComponent("Flag of "+n+".svg")+"?width=64";
}
function grad(cols){
  var id="g"+cols.map(function(c){return c.replace("#","")}).join("_");
  if(!document.getElementById(id)){
    var g=document.createElementNS(NS,"linearGradient"),m=cols.length;
    g.id=id;g.setAttribute("x1","0");g.setAttribute("y1","0");g.setAttribute("x2","1");g.setAttribute("y2","1");
    cols.forEach(function(c,i){
      var t=document.createElementNS(NS,"stop");
      t.setAttribute("offset", m<=1 ? "0" : (i/(m-1)).toFixed(3));
      t.setAttribute("stop-color",c);
      g.appendChild(t);
    });
    defs.appendChild(g);
  }
  return "url(#"+id+")";
}
function restackLayer(layer){
  if(!layer)return;
  var kids=Array.prototype.slice.call(layer.children);
  kids.sort(function(a,b){return (b.__area||0)-(a.__area||0)});
  kids.forEach(function(k){layer.appendChild(k)});
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
      /* градиент при нескольких режимах */
      b.style.fill=!m?"":m===1?cols[0]:grad(cols);
      /* пустые снизу, активные сверху; мелкие регионы всегда поверх крупных (Адыгея > Краснодар) */
      if(!b.__mk){
        var layer=(al||m)?gTop:gBot;
        if(layer&&b.parentNode!==layer)layer.appendChild(b);
      }
    }else{
      b.className="t"+(m?" on":"")+(m&&!al?" lt":"")+(al?" al":"")+(sel===n?" sel":"");
      b.style.background=!m?"":m===1?cols[0]:"linear-gradient(135deg,"+cols.map(function(c,i){return c+" "+(m<=1?0:(i*100/(m-1))).toFixed(1)+"%"}).join(",")+")";
    }
  });
  if(SVGM&&gg){
    if(al&&cols.length){
      var src=E[n],g=GL[n];
      if(src&&src.getAttribute("d")){
        if(!g){g=src.cloneNode(true);g.onclick=null;g.__n=null;g.__area=src.__area||0;gg.appendChild(g);GL[n]=g}
        g.setAttribute("d",src.getAttribute("d"));
        g.setAttribute("class","r glow");
        g.style.setProperty("--g1",cols[0]);g.style.setProperty("--g2",cols[cols.length-1]);
        g.style.fill=cols[0];
      }
    }else if(GL[n]){GL[n].remove();delete GL[n]}
  }
  if(SVGM){restackLayer(gBot);restackLayer(gTop);restackLayer(gg)}
  if(GLB)GLB.dirty();
}
function ago(sec){var m=Math.round(sec/60);return m<1?"только что":m<60?m+" мин назад":Math.floor(m/60)+" ч "+(m%60)+" мин назад"}
function dangerName(k){
  if(k==="d")return "опасность птичек";
  if(k==="m")return "ракетная опасность";
  return NAMEOF[k]||k;
}
function statusPhrase(k,v){
  if(v===1){
    if(k==="d")return "Опасность птичек";
    if(k==="m")return "Ракетная опасность";
    return "Опасность "+(NAMEOF[k]||k);
  }
  if(k==="d")return "Отбой опасности птичек";
  if(k==="m")return "Отбой ракетной опасности";
  return "Отбой: "+(NAMEOF[k]||k);
}
function histPhrase(e){
  var t=new Date(e.t*1000).toTimeString().slice(0,5);
  if(e.v===1)return t+" · введена "+dangerName(e.k);
  return t+" · отбой: "+dangerName(e.k);
}
function detail(){
  if(!sel){hideTip();if(logEl)logEl.classList.remove("hide");return}
  if(logEl)logEl.classList.add("hide");
  var s=S[sel],any=false;det.textContent="";
  if(s)CATS.forEach(function(c){
    var v=s[c.k][0];if(!v)return;any=true;
    var d=document.createElement("div"),dot=document.createElement("i");
    dot.className="dot";dot.style.background=v===1?c.a:c.c;d.appendChild(dot);
    d.appendChild(document.createTextNode(statusPhrase(c.k,v)+" · "+ago(s[c.k][1])));det.appendChild(d);
  });
  if(!any)det.textContent=S[sel]?"Сейчас спокойно":"";
  var hist=(HIST[sel]||[]).slice(0,6);
  if(hist.length){
    hist.forEach(function(e){
      var d=document.createElement("div");
      d.style.cssText="font-size:11px;color:var(--mut);margin-top:2px";
      d.textContent=histPhrase(e);
      det.appendChild(d);
    });
  }
  showTip(sel,any);
}
function statusLine(n){
  var s=S[n],parts=[];
  if(!s)return ""; /* страна — без «Сейчас спокойно» */
  CATS.forEach(function(c){
    var v=s[c.k][0];if(!v)return;
    parts.push(statusPhrase(c.k,v));
  });
  return parts.length?parts.join(", "):"Сейчас спокойно";
}
function hideTip(){if(tip)tip.className=""}
function showTip(n,any){
  if(!tip)return;
  var lab=LABEL[n]||n.slice(0,3);
  var st=statusLine(n);
  var hist=(HIST[n]||[]).slice(0,4);
  var hi=hist.map(function(e){return "<div>"+histPhrase(e)+"</div>"}).join("");
  var fu=flagUrl(n);
  var fl=fu
    ?'<span class="fl"><img src="'+fu+'" alt="" loading="lazy" referrerpolicy="no-referrer" onerror="this.remove()"></span>'
    :'<span class="fl">'+lab+'</span>';
  tip.innerHTML='<div class="th">'+fl+'<span>'+n+'</span></div>'
    +(st?'<div class="st"><b>'+st+'</b></div>':'')
    +(hi?'<div class="hi">'+hi+'</div>':'');
  tip.className="on";
  tip.style.left="8px";tip.style.bottom="8px";tip.style.top="auto";tip.style.right="auto";
}
function select(n){
  var o=sel;
  if(!n){
    sel=null;if(o)paint(o);nameEl.textContent="Нажмите на регион";det.textContent="";hideTip();
    if(logEl)logEl.classList.remove("hide");
    return;
  }
  sel=n;if(o)paint(o);paint(n);nameEl.textContent=n;detail();
  /* не сдвигать экран при выборе */
  try{if(document.activeElement&&document.activeElement.blur)document.activeElement.blur()}catch(e){}
}
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
    count();if(SVGM)updLabels();logEl.textContent="";
    j.events.forEach(function(e){
      if(!HIST[e.r])HIST[e.r]=[];
      if(!HIST[e.r].some(function(x){return x.t===e.t&&x.k===e.k&&x.v===e.v})){
        HIST[e.r].push({t:e.t,k:e.k,v:e.v});
        HIST[e.r].sort(function(a,b){return b.t-a.t});
        if(HIST[e.r].length>20)HIST[e.r].length=20;
      }
      var d=document.createElement("div");
      d.textContent=new Date(e.t*1000).toTimeString().slice(0,5)+" · "+e.r+" — "+(e.v===1?"введена "+dangerName(e.k):"отбой: "+dangerName(e.k));logEl.appendChild(d);
    });
    detail();
    var bad=Object.keys(j.channels).filter(function(c){return !j.channels[c].ok}).length;
    upd.textContent=j.configured?("Обновлено "+new Date().toTimeString().slice(0,5)+" · каналов: "+j.configured+(bad?" · с ошибкой: "+bad:"")):"Каналы не заданы (переменная CHANNELS на сервере)";
  }catch(e){upd.textContent="Нет связи с сервером"}
}
function scl(){var r=svg.getBoundingClientRect();return Math.min(r.width/V.w,r.height/V.h)}
function setV(){
  svg.setAttribute("viewBox",V.x+" "+V.y+" "+V.w+" "+V.h);
  var r=6.5/scl();Object.keys(M).forEach(function(n){M[n].setAttribute("r",r);if(GL[n]&&GL[n].tagName==="circle")GL[n].setAttribute("r",r)});
  updLabels();
}
function toU(px,py){var r=svg.getBoundingClientRect(),k=scl();return [V.x+(px-r.left-(r.width-V.w*k)/2)/k,V.y+(py-r.top-(r.height-V.h*k)/2)/k]}
function zoomAt(f,cx,cy){
  var nw=Math.max(G.w/25,Math.min(G.w*1.1,V.w*f)),q=nw/V.w;
  V.x=cx-(cx-V.x)*q;V.y=cy-(cy-V.y)*q;V.w=nw;V.h*=q;setV();
}
function buildLabels(){LBL=[]}
function isAct(n){var s=S[n];if(!s)return false;for(var i=0;i<CATS.length;i++){if(s[CATS[i].k][0])return true}return false}
function updLabels(){}
function zoom(d){
  if(GLB){GLB.zoom(d>0?1.4:1/1.4);return}
  if(SVGM){zoomAt(d>0?1/1.5:1.5,V.x+V.w/2,V.y+V.h/2);return}
  sz=Math.max(24,Math.min(64,sz+d));document.documentElement.style.setProperty("--s",sz+"px");
}
function useSvg(g){
  if(g)G=g;
  if(!G||!G.regions)return;
  stopGlobe();SVGM=true;E={};M={};GL={};
  var wrap=document.getElementById("map-wrap");wrap.className="svg";map.className="sv";map.textContent="";
  svg=document.createElementNS(NS,"svg");
  defs=document.createElementNS(NS,"defs");svg.appendChild(defs);
  /* слои: пустые → свечение → опасные → маркеры */
  gBot=document.createElementNS(NS,"g");
  gg=document.createElementNS(NS,"g");
  gTop=document.createElementNS(NS,"g");
  var gm=document.createElementNS(NS,"g");
  svg.appendChild(gBot);svg.appendChild(gg);svg.appendChild(gTop);svg.appendChild(gm);
  gt=document.createElementNS(NS,"g");svg.appendChild(gt);
  function mk(tag,n){var e=document.createElementNS(NS,tag);e.__n=n;e.onclick=function(ev){ev.preventDefault();ev.stopPropagation();if(moved<8)select(n)};return e}
  G.regions.forEach(function(r){
    var area=1;
    if(r.d){
      var p=mk("path",r.n);p.setAttribute("d",r.d);p.setAttribute("class","r");
      try{area=p.getTotalLength?Math.max(1,p.getTotalLength()):1}catch(e){area=r.s?10:100}
      if(r.s)area=Math.min(area,20);
      p.__area=area;
      gBot.appendChild(p);E[r.n]=p;
    }
    if(r.s||!r.d){
      var c=mk("circle",r.n);c.__mk=true;c.__area=5;c.setAttribute("cx",r.c[0]);c.setAttribute("cy",r.c[1]);c.setAttribute("class","r mk");gm.appendChild(c);M[r.n]=c;
      if(!E[r.n])E[r.n]=c;
    }
  });
  restackLayer(gBot);
  map.appendChild(svg);
  buildLabels();
  /* вся Россия при загрузке (без зума на центр) */
  var r0=svg.getBoundingClientRect(),aspect=r0.height/Math.max(r0.width,1);
  V={x:0,y:0,w:G.w*1.02,h:G.w*1.02*aspect};
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
  /* клик в пустое место — сброс выбора региона */
  svg.addEventListener("click",function(e){if(moved<8)select(null)});
  D.forEach(function(x){paint(x.n)});count();
  updateModeBtn();
}
function setMapMode(mode){
  mapMode=mode==="grid"||mode==="globe"?mode:"svg";
  localStorage.setItem("mapMode",mapMode);
  GTOK++;
  if(mapMode==="globe")buildGlobe();
  else if(mapMode==="svg"){
    if(G)useSvg();
    else buildGrid();
  }else buildGrid();
}
async function loadGeo(t){
  try{
    var r=await fetch("/api/geo");
    if(r.ok){
      G=await r.json();
      if(mapMode==="svg")useSvg();
      return;
    }
  }catch(e){}
  if(t<30)setTimeout(function(){loadGeo(t+1)},20000);
}

/* ---------- ГЛОБУС (d3-geo с CDN, данные с /api/globe) ---------- */
var D3URLS=["https://cdn.jsdelivr.net/npm/d3-array@3.2.4/dist/d3-array.min.js","https://cdn.jsdelivr.net/npm/d3-geo@3.1.1/dist/d3-geo.min.js"];
function loadScript(src){return new Promise(function(res,rej){var s=document.createElement("script");s.src=src;s.onload=res;s.onerror=function(){rej(new Error("script"))};document.head.appendChild(s)})}
async function loadD3(){if(window.d3&&d3.geoOrthographic)return;for(var i=0;i<D3URLS.length;i++)await loadScript(D3URLS[i])}
function regionCols(n){
  var s=S[n],cols=[],al=false;if(!s)return{cols:cols,al:al};
  CATS.forEach(function(c){var v=s[c.k][0];if(!v||!vis(c.k))return;cols.push(v===1?c.a:c.c);if(v===1)al=true});
  return{cols:cols,al:al};
}
/* Центры субъектов РФ — запасной слой регионов на глобусе */
var REFLL={"Мурманская область":[68.97,33.08],"Ненецкий АО":[67.64,53.0],"Ямало-Ненецкий АО":[66.53,66.6],"Чукотский АО":[64.73,177.5],"Республика Карелия":[61.79,34.37],"Архангельская область":[61.07,42.1],"Республика Коми":[61.67,50.8],"Ханты-Мансийский АО":[61.0,69.0],"Красноярский край":[56.01,92.85],"Республика Саха (Якутия)":[62.03,129.73],"Магаданская область":[62.9,152.4],"Санкт-Петербург":[59.93,30.32],"Ленинградская область":[59.64,33.5],"Вологодская область":[59.22,39.9],"Кировская область":[58.6,49.66],"Пермский край":[58.0,56.25],"Свердловская область":[56.84,60.6],"Тюменская область":[57.15,65.53],"Томская область":[56.5,84.97],"Иркутская область":[52.29,104.3],"Хабаровский край":[48.48,135.08],"Камчатский край":[56.3,160.85],"Псковская область":[57.82,28.33],"Новгородская область":[58.52,31.27],"Тверская область":[56.86,35.9],"Ярославская область":[57.63,39.87],"Костромская область":[57.77,40.93],"Удмуртия":[56.85,53.2],"Башкортостан":[54.74,55.97],"Челябинская область":[55.16,61.4],"Курганская область":[55.44,65.34],"Омская область":[54.99,73.37],"Новосибирская область":[55.03,82.92],"Кемеровская область":[55.35,86.09],"Хакасия":[53.72,91.44],"Республика Тыва":[51.72,94.45],"Республика Бурятия":[51.83,107.6],"Забайкальский край":[52.03,113.5],"Амурская область":[51.38,128.13],"Еврейская АО":[48.8,132.9],"Сахалинская область":[46.96,142.73],"Калининградская область":[54.71,20.5],"Смоленская область":[54.78,32.05],"Москва":[55.75,37.62],"Московская область":[56.3,38.13],"Владимирская область":[56.13,40.4],"Ивановская область":[57.0,40.97],"Нижегородская область":[56.33,44.0],"Республика Марий Эл":[56.63,47.88],"Чувашия":[56.13,47.25],"Татарстан":[55.8,49.1],"Алтайский край":[53.35,83.77],"Республика Алтай":[51.96,85.96],"Приморский край":[43.8,131.95],"Брянская область":[53.25,34.37],"Калужская область":[54.51,36.26],"Тульская область":[54.2,37.62],"Рязанская область":[54.63,39.74],"Республика Мордовия":[54.18,45.18],"Ульяновская область":[54.32,48.4],"Самарская область":[53.2,50.15],"Оренбургская область":[51.77,55.1],"Курская область":[51.73,36.19],"Орловская область":[52.97,36.07],"Липецкая область":[52.6,39.57],"Тамбовская область":[52.72,41.45],"Пензенская область":[53.2,45.0],"Саратовская область":[51.53,46.03],"Белгородская область":[50.6,36.59],"Воронежская область":[51.67,39.18],"Волгоградская область":[48.7,44.5],"Астраханская область":[46.35,48.04],"Республика Крым":[44.95,34.1],"Краснодарский край":[45.04,38.98],"Ростовская область":[47.23,39.7],"Ставропольский край":[45.04,41.97],"Республика Калмыкия":[46.31,44.27],"Севастополь":[44.6,33.52],"Республика Адыгея":[44.6,40.1],"Карачаево-Черкесия":[44.22,42.06],"Кабардино-Балкария":[43.5,43.6],"Северная Осетия":[43.03,44.68],"Ингушетия":[43.2,44.85],"Чечня":[43.32,45.69],"Дагестан":[42.82,47.12]};
/* Украинские области + Крым — границы по реальной форме (lat/lon), UA-флаги */
var UA_OBLASTS=[
{n:"Донецкая область",ring:[
[49.26,36.58],[49.32,36.9],[49.35,37.25],[49.33,37.65],[49.28,38.05],[49.18,38.45],[49.05,38.85],[48.9,39.15],[48.7,39.35],[48.45,39.48],[48.2,39.5],[47.95,39.4],[47.75,39.15],[47.6,38.8],[47.52,38.4],[47.5,37.95],[47.55,37.5],[47.65,37.1],[47.85,36.75],[48.1,36.5],[48.4,36.35],[48.7,36.32],[49.0,36.4],[49.26,36.58]
]},
{n:"Луганская область",ring:[
[50.08,38.15],[50.05,38.55],[49.95,39.0],[49.8,39.4],[49.55,39.75],[49.25,40.0],[48.95,40.12],[48.7,40.05],[48.5,39.8],[48.42,39.45],[48.48,39.05],[48.6,38.7],[48.8,38.4],[49.05,38.15],[49.35,37.98],[49.65,37.95],[49.9,38.0],[50.08,38.15]
]},
{n:"Запорожская область",ring:[
[47.92,34.55],[47.95,35.0],[47.92,35.45],[47.82,35.9],[47.65,36.25],[47.4,36.5],[47.1,36.65],[46.8,36.6],[46.55,36.35],[46.4,35.95],[46.35,35.5],[46.4,35.05],[46.55,34.65],[46.8,34.35],[47.1,34.2],[47.45,34.25],[47.7,34.4],[47.92,34.55]
]},
{n:"Херсонская область",ring:[
[47.05,32.55],[47.08,33.0],[47.02,33.5],[46.9,33.95],[46.7,34.3],[46.4,34.55],[46.1,34.65],[45.85,34.5],[45.7,34.15],[45.65,33.7],[45.7,33.25],[45.85,32.85],[46.1,32.55],[46.4,32.4],[46.7,32.4],[47.05,32.55]
]},
{n:"Республика Крым",ring:[
[46.15,33.55],[46.10,33.90],[46.00,34.30],[45.90,34.70],[45.75,35.10],[45.55,35.40],[45.45,35.80],[45.40,36.20],[45.35,36.50],[45.25,36.65],[45.10,36.55],[45.00,36.20],[44.95,35.70],[44.90,35.20],[44.80,34.80],[44.70,34.40],[44.55,34.20],[44.45,33.90],[44.40,33.60],[44.50,33.40],[44.70,33.35],[44.90,33.40],[45.15,33.30],[45.30,32.90],[45.40,32.55],[45.55,32.50],[45.70,32.70],[45.90,33.00],[46.05,33.30],[46.15,33.55]
]}
];
async function buildGlobe(){
  var tok=++GTOK;WD=null;
  stopGlobe();SVGM=false;E={};M={};GL={};gg=null;gBot=null;gTop=null;gt=null;svg=null;hideTip();
  var wrap=document.getElementById("map-wrap");wrap.className="gl";map.className="gl";map.textContent="";
  var msg=document.createElement("div");msg.id="gmsg";msg.textContent="Загрузка глобуса…";map.appendChild(msg);
  updateModeBtn();
  try{
    await loadD3();
    for(var i=0;i<25&&!WD;i++){
      if(tok!==GTOK)return;
      var r=await fetch("/api/globe",{cache:"no-store"});
      if(r.ok){WD=await r.json();if(WD&&WD.c&&WD.c.length)break;WD=null}
      await new Promise(function(ok){setTimeout(ok,3000)});
    }
    if(!WD||!WD.c)throw new Error("data");
  }catch(e){
    if(tok===GTOK){msg.textContent="Глобус недоступен (нет данных или библиотеки). Возвращаю карту…";setTimeout(function(){if(mapMode==="globe"&&tok===GTOK)setMapMode("svg")},2500)}
    return;
  }
  if(tok!==GTOK||mapMode!=="globe")return;
  map.textContent="";
  GLB=makeGlobe(WD);
}
function makeGlobe(data){
  var cv=document.createElement("canvas"),ctx=cv.getContext("2d");map.appendChild(cv);
  function flatToRing(f){var r=[];for(var i=0;i<f.length;i+=2)r.push([+f[i],+f[i+1]]);if(r.length&&(r[0][0]!==r[r.length-1][0]||r[0][1]!==r[r.length-1][1]))r.push(r[0].slice());return r}
  function feat(p){
    /* p: [polygon...] polygon: [ringFlat...] ringFlat: [lon,lat,...] */
    try{
      return{type:"Feature",geometry:{type:"MultiPolygon",coordinates:p.map(function(poly){
        return (Array.isArray(poly[0])?poly:[poly]).map(function(f){
          return Array.isArray(f[0])?f:flatToRing(f);
        });
      })}};
    }catch(e){return{type:"Feature",geometry:{type:"MultiPolygon",coordinates:[]}}}
  }
  function ringFeat(latlon){
    var ring=latlon.map(function(p){return[p[1],p[0]]});
    ring.push(ring[0].slice());
    return{type:"Feature",geometry:{type:"Polygon",coordinates:[ring]}};
  }
  function dotFeat(lat,lon,d){
    d=d||0.6;
    return{type:"Feature",geometry:{type:"Polygon",coordinates:[[
      [lon-d,lat-d],[lon+d,lat-d],[lon+d,lat+d],[lon-d,lat+d],[lon-d,lat-d]
    ]]}};
  }
  var C=(data.c||[]).map(function(c){return{n:c.n,i:c.i||"",f:feat(c.p)}}).filter(function(c){return c.f.geometry.coordinates.length});
  var RG=[],RGI={};
  (data.r||[]).forEach(function(r){
    try{
      var f=feat(r.p);
      if(f.geometry.coordinates.length){var o={n:r.n,f:f};RG.push(o);RGI[r.n]=o}
    }catch(e){}
  });
  /* гарантируем все субъекты РФ */
  Object.keys(REFLL).forEach(function(n){
    if(RGI[n])return;
    var ll=REFLL[n],o={n:n,f:dotFeat(ll[0],ll[1],0.7),dot:true};
    RG.push(o);RGI[n]=o;
  });
  var CGI={};C.forEach(function(c){CGI[c.n]=c;if(c.i)WORLD_ISO[c.n]=c.i});
  /* украинские области — выбираемые регионы с UA-флагами */
  var UA=[];UA_OBLASTS.forEach(function(u){
    var o={n:u.n,f:ringFeat(u.ring),ua:true};
    UA.push(o);CGI[u.n]=o;
  });
  var proj=d3.geoOrthographic().clipAngle(90).precision(0.5),path=d3.geoPath(proj,ctx),grat=d3.geoGraticule().step([30,30]);
  var st={lon:62,lat:54,k:1.35},W=0,H=0,dpr=1,raf=0,stopped=false,lastT=0,pts={},mv=0,needAnim=false;
  var baseCv=document.createElement("canvas"),baseCtx=baseCv.getContext("2d"),baseKey="";
  function okGeom(f,R){
    try{
      var b=path.bounds(f);
      if(!isFinite(b[0][0]))return false;
      var w=b[1][0]-b[0][0],h=b[1][1]-b[0][1];
      if(w*h>Math.PI*R*R*1.3)return false;
      return true;
    }catch(e){return false}
  }
  function baseR(){return Math.min(W,H)*0.46}
  function clamp(v,a,b){return Math.max(a,Math.min(b,v))}
  function resize(){
    W=cv.clientWidth;H=cv.clientHeight;dpr=Math.min(window.devicePixelRatio||1,1.75);
    cv.width=Math.max(1,Math.round(W*dpr));cv.height=Math.max(1,Math.round(H*dpr));
    baseCv.width=cv.width;baseCv.height=cv.height;baseKey="";dirty();
  }
  function dirty(){if(!raf&&!stopped)raf=requestAnimationFrame(draw)}
  function drawBase(R,light){
    var key=st.lon.toFixed(2)+"|"+st.lat.toFixed(2)+"|"+st.k.toFixed(3)+"|"+W+"x"+H+"|"+(light?1:0);
    if(key===baseKey)return;
    baseKey=key;
    var bctx=baseCtx;
    bctx.setTransform(dpr,0,0,dpr,0,0);bctx.clearRect(0,0,W,H);
    path.context(bctx);
    /* океан */
    var hg=bctx.createRadialGradient(W/2,H/2,R*.98,W/2,H/2,R*1.1);
    hg.addColorStop(0,light?"rgba(90,140,200,.30)":"rgba(90,150,230,.28)");hg.addColorStop(1,"rgba(90,150,230,0)");
    bctx.fillStyle=hg;bctx.beginPath();bctx.arc(W/2,H/2,R*1.1,0,6.2832);bctx.fill();
    bctx.beginPath();path({type:"Sphere"});
    var og=bctx.createRadialGradient(W/2-R*.35,H/2-R*.35,R*.1,W/2,H/2,R*1.05);
    og.addColorStop(0,light?"#e6f2fb":"#14212e");og.addColorStop(1,light?"#bcd6ea":"#060c12");
    bctx.fillStyle=og;bctx.fill();
    bctx.beginPath();path(grat());bctx.strokeStyle=light?"rgba(0,0,0,.06)":"rgba(255,255,255,.04)";bctx.lineWidth=.5;bctx.stroke();
    bctx.lineJoin="round";bctx.lineCap="round";
    C.forEach(function(c){bctx.beginPath();path(c.f);bctx.fillStyle=light?"#f0f4f7":"#2a3d52";bctx.fill("evenodd")});
    /* WBW ярче */
    var rus=null;for(var ri=0;ri<C.length;ri++){if(C[ri].n==="Russia"||C[ri].i==="RU"){rus=C[ri];break}}
    if(rus){
      bctx.save();bctx.beginPath();path(rus.f);bctx.clip("evenodd");
      var rb=path.bounds(rus.f);
      if(isFinite(rb[0][0])){
        var rx=rb[0][0],ry=rb[0][1],rw=Math.max(1,rb[1][0]-rb[0][0]),rh=Math.max(1,rb[1][1]-rb[0][1]);
        bctx.globalAlpha=light?0.72:0.68;
        bctx.fillStyle="#ffffff";bctx.fillRect(rx,ry,rw,rh/3);
        bctx.fillStyle="#0084D7";bctx.fillRect(rx,ry+rh/3,rw,rh/3);
        bctx.fillStyle="#ffffff";bctx.fillRect(rx,ry+2*rh/3,rw,rh/3+1);
        bctx.globalAlpha=1;
      }
      bctx.restore();
    }
    bctx.save();bctx.beginPath();path({type:"Sphere"});bctx.clip();
    var relief=bctx.createRadialGradient(W*.34,H*.30,R*.03,W*.52,H*.52,R*1.08);
    relief.addColorStop(0,light?"rgba(255,255,255,.22)":"rgba(255,255,255,.10)");
    relief.addColorStop(1,light?"rgba(20,45,65,.18)":"rgba(0,0,0,.35)");
    bctx.fillStyle=relief;bctx.fillRect(W/2-R,H/2-R,R*2,R*2);bctx.restore();
    RG.forEach(function(r){
      if(!okGeom(r.f,R))return;
      bctx.beginPath();path(r.f);
      bctx.fillStyle=light?"rgba(244,247,250,.70)":"rgba(51,77,102,.62)";
      bctx.fill("evenodd");
      bctx.strokeStyle=light?"#4a5d70":"#8fa3b8";bctx.lineWidth=0.85;bctx.stroke();
    });
    UA.forEach(function(u){
      bctx.beginPath();path(u.f);
      bctx.fillStyle=light?"rgba(0,87,183,.28)":"rgba(0,87,183,.40)";
      bctx.fill("evenodd");
      bctx.strokeStyle=light?"#0057b7":"#ffd700";bctx.lineWidth=1.15;bctx.stroke();
    });
    C.forEach(function(c){bctx.beginPath();path(c.f);bctx.strokeStyle=light?"#5a7085":"#9aafc2";bctx.lineWidth=1.0;bctx.stroke()});
    bctx.beginPath();path({type:"Sphere"});bctx.strokeStyle=light?"rgba(60,100,150,.4)":"rgba(140,190,240,.3)";bctx.lineWidth=1.1;bctx.stroke();
    path.context(ctx);
  }
  function draw(ts){
    raf=0;if(stopped||!W)return;
    var minGap=needAnim?40:80;
    if(ts-lastT<minGap){raf=requestAnimationFrame(draw);return}
    lastT=ts;
    var R=baseR()*st.k,light=document.documentElement.getAttribute("data-theme")==="light";
    var cs=getComputedStyle(document.documentElement),fg=cs.getPropertyValue("--fg").trim()||"#fff",bg=cs.getPropertyValue("--bg").trim()||"#000";
    proj.translate([W/2,H/2]).scale(R).rotate([-st.lon,-st.lat,0]);
    drawBase(R,light);
    ctx.setTransform(1,0,0,1,0,0);
    ctx.clearRect(0,0,cv.width,cv.height);
    ctx.drawImage(baseCv,0,0);
    ctx.setTransform(dpr,0,0,dpr,0,0);
    /* только анимация опасностей + выделение поверх кэша */
    var t=ts/1000,anim=false;
    RG.forEach(function(r){
      var q=regionCols(r.n),cols=q.cols;if(!cols.length)return;
      if(!okGeom(r.f,R))return;anim=true;
      var per=q.al?1:1.8,p=.5+.5*Math.sin(t*6.2832/per);
      ctx.save();ctx.beginPath();path(r.f);
      var b=path.bounds(r.f),w=b[1][0]-b[0][0],h=b[1][1]-b[0][1],fin=isFinite(w)&&isFinite(h),fill=cols[0];
      if(fin&&cols.length>1){var g=ctx.createLinearGradient(b[0][0],b[0][1],b[1][0],b[1][1]);cols.forEach(function(c,i){g.addColorStop(i/(cols.length-1),c)});fill=g}
      ctx.shadowColor=cols[cols.length-1];ctx.shadowBlur=4+10*p;ctx.globalAlpha=.75+.25*p;ctx.fillStyle=fill;ctx.fill("evenodd");
      ctx.shadowBlur=0;ctx.globalAlpha=1;ctx.strokeStyle="rgba(255,255,255,.85)";ctx.lineWidth=1;ctx.stroke();
      if(fin&&w<10&&h<10){var c=path.centroid(r.f);if(isFinite(c[0])){ctx.beginPath();ctx.arc(c[0],c[1],6,0,6.2832);ctx.shadowColor=cols[0];ctx.shadowBlur=6+6*p;ctx.fillStyle=cols[0];ctx.fill();ctx.shadowBlur=0;ctx.lineWidth=1.5;ctx.strokeStyle="#fff";ctx.stroke()}}
      ctx.restore();
    });
    if(sel&&RGI[sel]&&okGeom(RGI[sel].f,R)){ctx.beginPath();path(RGI[sel].f);ctx.lineWidth=2.2;ctx.strokeStyle=fg;ctx.stroke()}
    else if(sel&&CGI[sel]){ctx.beginPath();path(CGI[sel].f);ctx.lineWidth=2.0;ctx.strokeStyle=fg;ctx.stroke()}
    needAnim=anim;
    if(anim)raf=requestAnimationFrame(draw);
  }
  function zoomBy(f){st.k=clamp(st.k*f,1,45);dirty()}
  function tap(e){
    var rc=cv.getBoundingClientRect(),x=e.clientX-rc.left,y=e.clientY-rc.top,best=null;
    var R=baseR()*st.k,dx=x-W/2,dy=y-H/2;
    /* Клик вне диска глобуса — сброс (окружность сферы не выбирается). */
    if(dx*dx+dy*dy<=R*R*0.998){
      var ll=proj.invert([x,y]);
      if(ll && d3.geoDistance(ll,[st.lon,st.lat])<=Math.PI/2+0.0005){
        var matches=[];
        /* сначала регионы РФ (только валидные, самые мелкие приоритетнее) */
        RG.forEach(function(g){
          if(!okGeom(g.f,R))return;
          var b=path.bounds(g.f);
          if(!isFinite(b[0][0])||x<b[0][0]-1||x>b[1][0]+1||y<b[0][1]-1||y>b[1][1]+1)return;
          if(d3.geoContains(g.f,ll)){
            matches.push({n:g.n,a:Math.max(1,(b[1][0]-b[0][0])*(b[1][1]-b[0][1])),t:"r"});
          }
        });
        if(matches.length){
          matches.sort(function(a,b){return a.a-b.a});
          best=matches[0].n;
        }else{
          /* украинские области — поверх Украины */
          var um=[];
          UA.forEach(function(g){
            var b=path.bounds(g.f);
            if(!isFinite(b[0][0])||x<b[0][0]-1||x>b[1][0]+1||y<b[0][1]-1||y>b[1][1]+1)return;
            if(d3.geoContains(g.f,ll))um.push({n:g.n,a:Math.max(1,(b[1][0]-b[0][0])*(b[1][1]-b[0][1]))});
          });
          if(um.length){
            um.sort(function(a,b){return a.a-b.a});
            best=um[0].n;
          }
        }
        if(!best){
          /* страна: центроид недалеко от клика + крупнейшая */
          var cm=[],MAX_D=28*Math.PI/180;
          C.forEach(function(g){
            var b=path.bounds(g.f);
            if(!isFinite(b[0][0])||x<b[0][0]-1||x>b[1][0]+1||y<b[0][1]-1||y>b[1][1]+1)return;
            if(!d3.geoContains(g.f,ll))return;
            var cen=d3.geoCentroid(g.f);
            if(!cen||!isFinite(cen[0]))return;
            if(d3.geoDistance(ll,cen)>MAX_D)return;
            var area=Math.max(1,(b[1][0]-b[0][0])*(b[1][1]-b[0][1]));
            cm.push({n:g.n,a:area});
          });
          cm.sort(function(a,b){return b.a-a.a});
          if(cm.length)best=cm[0].n;
        }
      }
    }
    select(best);
  }
  cv.addEventListener("pointerdown",function(e){pts[e.pointerId]={x:e.clientX,y:e.clientY};mv=0;try{cv.setPointerCapture(e.pointerId)}catch(x){}});
  cv.addEventListener("pointermove",function(e){
    var p=pts[e.pointerId];if(!p)return;var ids=Object.keys(pts);
    if(ids.length===1){var dx=e.clientX-p.x,dy=e.clientY-p.y,dpp=57.3/(baseR()*st.k);mv+=Math.abs(dx)+Math.abs(dy);st.lon-=dx*dpp;st.lat=clamp(st.lat+dy*dpp,-85,85)}
    else if(ids.length===2){var o=pts[ids[0]==e.pointerId?ids[1]:ids[0]],d0=Math.hypot(p.x-o.x,p.y-o.y),d1=Math.hypot(e.clientX-o.x,e.clientY-o.y);if(d0>0)st.k=clamp(st.k*d1/d0,1,45);mv+=10}
    p.x=e.clientX;p.y=e.clientY;dirty();
  });
  function up(e,isUp){var had=pts[e.pointerId];delete pts[e.pointerId];if(isUp&&had&&mv<8&&!Object.keys(pts).length)tap(e)}
  cv.addEventListener("pointerup",function(e){up(e,true)});
  cv.addEventListener("pointercancel",function(e){up(e,false)});
  cv.addEventListener("wheel",function(e){e.preventDefault();zoomBy(e.deltaY>0?1/1.2:1.2)},{passive:false});
  var ro=window.ResizeObserver?new ResizeObserver(resize):null;if(ro)ro.observe(document.getElementById("map-wrap"));
  window.addEventListener("resize",resize);
  resize();updateModeBtn();
  return{dirty:dirty,zoom:zoomBy,stop:function(){stopped=true;if(raf)cancelAnimationFrame(raf);raf=0;if(ro)ro.disconnect();window.removeEventListener("resize",resize)}};
}
document.getElementById("thm").addEventListener("click",function(){setTimeout(function(){if(GLB)GLB.dirty()},0)});
document.getElementById("zp").onclick=function(){zoom(6)};
document.getElementById("zm").onclick=function(){zoom(-6)};
document.getElementById("thm").onclick=function(){
  var cur=document.documentElement.getAttribute("data-theme")==="light"?"light":"dark";
  var next=cur==="dark"?"light":"dark";
  document.documentElement.setAttribute("data-theme",next);
  localStorage.setItem("theme",next);
  var m=document.querySelector('meta[name="theme-color"]');
  if(m)m.content=next==="light"?"#f3f5f7":"#0b1117";
};
document.getElementById("mode").onclick=function(){setMapMode(mapMode==="svg"?"grid":mapMode==="grid"?"globe":"svg")};
buildGrid();
if(mapMode==="globe")buildGlobe();
count();load();setInterval(load,10000);loadGeo(0);
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
# запасной контур Крыма, если в файле его нет
CRIMEA = [
    (46.15,33.55),(46.10,33.90),(46.00,34.30),(45.90,34.70),(45.75,35.10),(45.55,35.40),(45.45,35.80),(45.40,36.20),
    (45.35,36.50),(45.25,36.65),(45.10,36.55),(45.00,36.20),(44.95,35.70),(44.90,35.20),(44.80,34.80),(44.70,34.40),
    (44.55,34.20),(44.45,33.90),(44.40,33.60),(44.50,33.40),(44.70,33.35),(44.90,33.40),(45.15,33.30),(45.30,32.90),
    (45.40,32.55),(45.55,32.50),(45.70,32.70),(45.90,33.00),(46.05,33.30),(46.15,33.55),
]
# запасной контур Севастополя
SEVASTOPOL = [
    (44.75,33.30),(44.74,33.42),(44.72,33.52),(44.70,33.60),(44.66,33.68),(44.62,33.72),(44.58,33.70),
    (44.54,33.64),(44.52,33.55),(44.50,33.48),(44.50,33.40),(44.52,33.32),(44.56,33.26),(44.62,33.24),
    (44.68,33.25),(44.72,33.28),(44.75,33.30),
]
# Приблизительные контуры оккупированных территорий Украины (для отображения на глобусе)
OCC_UA = [
    # Крым — уже есть в CRIMEA; ДНР/ЛНР/юг Запорожской и Херсонской (грубо)
    [(49.2,36.5),(49.3,37.5),(49.1,38.2),(48.7,39.5),(48.1,39.8),(47.8,39.0),(47.5,38.2),(47.6,37.5),(48.0,36.8),(48.5,36.3),(49.2,36.5)],
    [(48.8,39.0),(49.0,39.8),(48.7,40.0),(48.2,39.9),(47.9,39.5),(47.95,39.1),(48.3,38.9),(48.8,39.0)],
    [(47.5,34.5),(47.6,35.5),(47.3,36.5),(46.8,36.8),(46.3,35.8),(46.2,34.5),(46.5,33.8),(47.0,33.5),(47.4,34.0),(47.5,34.5)],
    [(46.8,32.5),(46.9,33.5),(46.5,34.2),(46.0,34.5),(45.7,33.8),(45.8,32.8),(46.2,32.2),(46.8,32.5)],
]
GEO, GEO_INFO = None, {"status": "loading"}
GLOBE_REG, WORLD, WORLD_INFO = [], None, {"status": "loading"}
WORLD_URLS = [
    os.getenv("WORLD_URL", "https://cdn.jsdelivr.net/npm/world-atlas@2.0.2/countries-50m.json"),
    "https://cdn.jsdelivr.net/npm/world-atlas@2.0.2/countries-110m.json",
]

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

def _wind(ring, exterior):
    """d3-geo ждёт внешние кольца по часовой стрелке, дыры против (иначе полигон «выворачивается»)."""
    if len(ring) < 4 or max(p[0] for p in ring) - min(p[0] for p in ring) > 300:
        return ring
    a = sum(ring[i - 1][0] * ring[i][1] - ring[i][0] * ring[i - 1][1] for i in range(len(ring))) / 2
    return ring if (a < 0) == exterior else ring[::-1]

def _glob_poly(poly, eps, minsz, holes=True):
    out = []
    for j, ring in enumerate(poly):
        if j and not holes:
            break
        ring = [(p[0], p[1]) for p in ring]
        xs, ys = [p[0] for p in ring], [p[1] for p in ring]
        if j == 0 and max(xs) - min(xs) < minsz and max(ys) - min(ys) < minsz:
            return None
        q = _dpring(ring, eps)
        if len(q) < 3:
            continue
        out.append([round(c, 2) for p in _wind(q, j == 0) for c in p])
    return out or None

def _decode_topo(t):
    sx, sy = t["transform"]["scale"]; tx, ty = t["transform"]["translate"]
    arcs = []
    for a in t["arcs"]:
        x = y = 0
        pts = []
        for dx, dy in a:
            x += dx; y += dy
            pts.append((x * sx + tx, y * sy + ty))
        arcs.append(pts)
    def ring(idx):
        pts = []
        for i in idx:
            seg = arcs[i] if i >= 0 else arcs[~i][::-1]
            pts += seg if not pts else seg[1:]
        return pts
    res = []
    for g in t["objects"]["countries"]["geometries"]:
        polys = [g["arcs"]] if g["type"] == "Polygon" else (g["arcs"] if g["type"] == "MultiPolygon" else [])
        if polys:
            props = g.get("properties") or {}
            res.append((props.get("name", "") or "", str(g.get("id") or ""), [[ring(r) for r in poly] for poly in polys]))
    return res

# ISO 3166-1 numeric → alpha-2 для флагов
ISO_N2A = {"004":"AF","008":"AL","010":"AQ","012":"DZ","016":"AS","020":"AD","024":"AO","028":"AG","031":"AZ","032":"AR","036":"AU","040":"AT","044":"BS","048":"BH","050":"BD","051":"AM","052":"BB","056":"BE","060":"BM","064":"BT","068":"BO","070":"BA","072":"BW","074":"BV","076":"BR","084":"BZ","086":"IO","090":"SB","092":"VG","096":"BN","100":"BG","104":"MM","108":"BI","112":"BY","116":"KH","120":"CM","124":"CA","132":"CV","136":"KY","140":"CF","144":"LK","148":"TD","152":"CL","156":"CN","158":"TW","162":"CX","166":"CC","170":"CO","174":"KM","175":"YT","178":"CG","180":"CD","184":"CK","188":"CR","191":"HR","192":"CU","196":"CY","203":"CZ","204":"BJ","208":"DK","212":"DM","214":"DO","218":"EC","222":"SV","226":"GQ","231":"ET","232":"ER","233":"EE","234":"FO","238":"FK","239":"GS","242":"FJ","246":"FI","248":"AX","250":"FR","254":"GF","258":"PF","260":"TF","262":"DJ","266":"GA","268":"GE","270":"GM","275":"PS","276":"DE","288":"GH","292":"GI","296":"KI","300":"GR","304":"GL","308":"GD","312":"GP","316":"GU","320":"GT","324":"GN","328":"GY","332":"HT","334":"HM","336":"VA","340":"HN","344":"HK","348":"HU","352":"IS","356":"IN","360":"ID","364":"IR","368":"IQ","372":"IE","376":"IL","380":"IT","384":"CI","388":"JM","392":"JP","398":"KZ","400":"JO","404":"KE","408":"KP","410":"KR","414":"KW","417":"KG","418":"LA","422":"LB","426":"LS","428":"LV","430":"LR","434":"LY","438":"LI","440":"LT","442":"LU","446":"MO","450":"MG","454":"MW","458":"MY","462":"MV","466":"ML","470":"MT","474":"MQ","478":"MR","480":"MU","484":"MX","492":"MC","496":"MN","498":"MD","499":"ME","500":"MS","504":"MA","508":"MZ","512":"OM","516":"NA","520":"NR","524":"NP","528":"NL","531":"CW","533":"AW","534":"SX","535":"BQ","540":"NC","548":"VU","554":"NZ","558":"NI","562":"NE","566":"NG","570":"NU","574":"NF","578":"NO","580":"MP","581":"UM","583":"FM","584":"MH","585":"PW","586":"PK","591":"PA","598":"PG","600":"PY","604":"PE","608":"PH","612":"PN","616":"PL","620":"PT","624":"GW","626":"TL","630":"PR","634":"QA","638":"RE","642":"RO","643":"RU","646":"RW","652":"BL","654":"SH","659":"KN","660":"AI","662":"LC","663":"MF","666":"PM","670":"VC","674":"SM","678":"ST","682":"SA","686":"SN","688":"RS","690":"SC","694":"SL","702":"SG","703":"SK","704":"VN","705":"SI","706":"SO","710":"ZA","716":"ZW","724":"ES","728":"SS","729":"SD","732":"EH","740":"SR","744":"SJ","748":"SZ","752":"SE","756":"CH","760":"SY","762":"TJ","764":"TH","768":"TG","772":"TK","776":"TO","780":"TT","784":"AE","788":"TN","792":"TR","795":"TM","796":"TC","798":"TV","800":"UG","804":"UA","807":"MK","818":"EG","826":"GB","831":"GG","832":"JE","833":"IM","834":"TZ","840":"US","850":"VI","854":"BF","858":"UY","860":"UZ","862":"VE","876":"WF","882":"WS","887":"YE","894":"ZM"}

def build_world(topo):
    out = []
    for name, iso, polys in _decode_topo(topo):
        if name == "Antarctica":
            continue
        # меньше упрощение → более реалистичные границы стран
        ps = [g for g in (_glob_poly(poly, 0.02, 0.04) for poly in polys) if g]
        if ps:
            a2 = ISO_N2A.get(str(iso).zfill(3), "")
            out.append({"n": name, "i": a2, "p": ps})
    return out

def build_geo(data):
    global GLOBE_REG
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
    glob = []      # контуры в градусах для глобуса
    # Для глобуса: берём полигоны с опорной точкой; режем только явный «весь мир» (dx>100°)
    for name, i in owner.items():
        lat0, lon0 = REF[name]
        good = []
        for pl in feats[i][1]:
            if not _inpoly(lon0, lat0, pl):
                continue
            # мягкое упрощение, чтобы не терять регионы
            g = _glob_poly([pl[0]], 0.03, 0.03, holes=False)
            if not g:
                # fallback без упрощения (усечённый контур)
                ring = [(round(p[0], 2), round(p[1], 2)) for p in pl[0][::max(1, len(pl[0]) // 80)]]
                if len(ring) < 3:
                    continue
                flat = [c for p in _wind(ring, True) for c in p]
                g = [flat]
            xs = [g[0][k] for k in range(0, len(g[0]), 2)]
            ys = [g[0][k] for k in range(1, len(g[0]), 2)]
            if not xs:
                continue
            dx, dy = max(xs) - min(xs), max(ys) - min(ys)
            # только явный артефакт «на весь шар»
            if dx > 100 or dy > 50:
                continue
            good.append(g)
        if good:
            glob.append({"n": name, "p": good})
    have = {g["n"] for g in glob}
    fallback = [n for n in REF if n not in owner]
    for n_, pts_ in (("Республика Крым", CRIMEA), ("Севастополь", SEVASTOPOL)):
        if n_ not in have:
            glob.append({"n": n_, "p": [[[round(c, 2) for p in _wind([(lo, la) for la, lo in pts_], True) for c in p]]]})
            have.add(n_)
    # регионы без контура — маленькая «площадка» вокруг REF, чтобы на глобусе были все субъекты
    for name, (lat0, lon0) in REF.items():
        if name in have:
            continue
        d = 0.55
        flat = [round(c, 2) for c in (
            lon0 - d, lat0 - d, lon0 + d, lat0 - d, lon0 + d, lat0 + d, lon0 - d, lat0 + d, lon0 - d, lat0 - d
        )]
        glob.append({"n": name, "p": [[flat]], "dot": True})
    GLOBE_REG = glob
    crim = [_albers(lo, la) for la, lo in CRIMEA] if "Республика Крым" in fallback else None
    sev  = [_albers(lo, la) for la, lo in SEVASTOPOL] if "Севастополь" in fallback else None
    pts = {n: _albers(lo, la) for n, (la, lo) in REF.items() if n in fallback}
    allp = [p for rs in raw.values() for r in rs for p in r] + (crim or []) + (sev or []) + list(pts.values())
    x0, x1, y0, y1 = min(p[0] for p in allp), max(p[0] for p in allp), min(p[1] for p in allp), max(p[1] for p in allp)
    k = 1000 / (x1 - x0)
    tr = lambda p: ((p[0] - x0) * k, (p[1] - y0) * k)
    def path(rings):
        sc = [[tr(p) for p in r] for r in rings]
        big = max(_area(r) for r in sc) if sc else 0
        out = []
        for r in sc:
            a = _area(r)
            if a < 0.04 and a < big:
                continue
            # меньше упрощение → более реалистичные границы
            q = _dpring(r, 0.06 if a < 40 else 0.12)
            if len(q) < 3:
                q = r
            out.append("M" + "L".join("%.2f %.2f" % p for p in q) + "Z")
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
        regs.append({"n": "Республика Крым", "d": "M" + "L".join("%.2f %.2f" % p for p in t) + "Z",
                     "c": [round(sum(p[0] for p in t) / len(t), 1), round(sum(p[1] for p in t) / len(t), 1)], "s": True, "a": 50, "approx": True})
        fallback.remove("Республика Крым")
    if sev:
        t = [tr(p) for p in sev]
        regs.append({"n": "Севастополь", "d": "M" + "L".join("%.2f %.2f" % p for p in t) + "Z",
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
            "props_example": list((feats[0][0] if feats else {}).keys())[:10], "globe_regions": len(glob)}
    return {"w": 1000, "h": round(h + 5, 1), "regions": regs}, info

def _seed_globe_from_ref():
    """Минимальные контуры всех субъектов, если geo ещё не готов."""
    out = []
    for name, (lat0, lon0) in REF.items():
        d = 0.55
        flat = [round(c, 2) for c in (
            lon0 - d, lat0 - d, lon0 + d, lat0 - d, lon0 + d, lat0 + d, lon0 - d, lat0 + d, lon0 - d, lat0 - d
        )]
        out.append({"n": name, "p": [[flat]], "dot": True})
    return out

async def load_world():
    global WORLD, WORLD_INFO, GLOBE_REG
    while WORLD is None:
        for url in WORLD_URLS:
            try:
                async with httpx.AsyncClient(timeout=90, follow_redirects=True) as c:
                    r = await c.get(url)
                    r.raise_for_status()
                WORLD = await asyncio.to_thread(build_world, r.json())
                WORLD_INFO = {"status": "ok", "url": url, "countries": len(WORLD)}
                if not GLOBE_REG:
                    GLOBE_REG = _seed_globe_from_ref()
                break
            except Exception as e:
                WORLD_INFO = {"status": "error", "url": url, "err": str(e)[:200]}
        if WORLD is None:
            await asyncio.sleep(120)

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
    tasks = [asyncio.create_task(poller()), asyncio.create_task(load_geo()), asyncio.create_task(load_world())]
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
    return JSONResponse({"regions": view(), "events": list(EVENTS)[:40], "channels": HEALTH, "configured": len(CHANNELS)},
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
    return {**GEO_INFO, "world": WORLD_INFO}

@app.get("/api/globe")
def globe():
    if WORLD is None:
        return JSONResponse({"ok": False}, status_code=503)
    regs = GLOBE_REG if GLOBE_REG else _seed_globe_from_ref()
    occ = []
    for ring in OCC_UA:
        flat = [round(c, 2) for la, lo in ring for c in (lo, la)]
        occ.append([flat])
    cr = [round(c, 2) for la, lo in CRIMEA for c in (lo, la)]
    occ.append([cr])
    return JSONResponse(
        {"c": WORLD, "r": regs, "o": occ, "n_regions": len(regs), "n_countries": len(WORLD)},
        headers={"Cache-Control": "no-store"},
    )

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
