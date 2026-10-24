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
.t{border:0;border-radius:8px;background:var(--tile);color:var(--mut);font-size:calc(var(--s)*.26);font-weight:600;padding:0;cursor:pointer;-webkit-tap-highlight-color:transparent;overflow:hidden;transition:transform .15s ease,filter .15s ease,box-shadow .15s ease}
.t.on{color:#fff;text-shadow:0 1px 2px rgba(0,0,0,.55)}
.t.on{position:relative;z-index:1;animation:neon 1.8s ease-in-out infinite}
.t.al{animation-duration:1s}
.t.sel{z-index:2}
.t.sel{outline:2px solid var(--fg);outline-offset:1px}
@media (hover:hover){
.t:hover{filter:brightness(1.14);transform:scale(1.08);z-index:3}
.t.on:hover{filter:brightness(1.22)}
}
.t:active{transform:scale(.94)}
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
.r{fill:var(--tile);stroke:var(--bd);stroke-width:.7;vector-effect:non-scaling-stroke;cursor:pointer;paint-order:stroke fill;transition:stroke-width .15s ease,filter .15s ease,opacity .15s ease}
.r.mk{stroke:var(--mut);stroke-width:1}
.r.on{stroke-width:2.2}
.r.al{stroke-width:2.8}
.r.sel{stroke:var(--fg)!important;stroke-width:2.6}
/* glow только как бонус для Android/Chrome; на iOS основной эффект — цветной stroke + fill */
.r.on{animation:sneon 1.8s ease-in-out infinite}
.r.al{animation-duration:1s}
@keyframes sneon{
0%,100%{filter:brightness(1) drop-shadow(0 0 3px var(--g1)) drop-shadow(0 0 8px var(--g2))}
50%{filter:brightness(1.3) drop-shadow(0 0 6px var(--g1)) drop-shadow(0 0 14px var(--g2))}}
@media (prefers-reduced-motion:reduce){.r.on{animation:none}}
@media (hover:hover){
#map svg .r:hover{filter:brightness(1.2);stroke-width:2.4;opacity:1}
#map svg .r.on:hover{filter:brightness(1.35) drop-shadow(0 0 6px var(--g1)) drop-shadow(0 0 12px var(--g2));stroke-width:3}
#map svg .r.mk:hover{stroke-width:2.8}
}
#map svg .r:active{filter:brightness(1.25)}
</style>
</head>
<body>
<header>
<div class="top"><h1>Радар опасности</h1><div class="z"><button id="zm">−</button><button id="zp">+</button></div></div>
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
D.forEach(function(x){
  var b=document.createElement("button");
  b.className="t";b.textContent=x.l;b.title=x.n;
  b.style.gridColumn=x.c+1;b.style.gridRow=x.r+1;
  b.onclick=function(){select(x.n)};
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
      /* iOS Safari плохо рисует CSS filter на SVG — подсветка через цветной stroke */
      if(m){
        b.style.stroke=cols[0];
        b.style.strokeWidth=al?"2.8":(b.__mk?"2.4":"2.0");
      }else{
        b.style.stroke="";
        b.style.strokeWidth="";
      }
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
}
async function loadGeo(t){
  try{
    var r=await fetch("/api/geo");
    if(r.ok){useSvg(await r.json());return}
  }catch(e){}
  if(t<30)setTimeout(function(){loadGeo(t+1)},20000);
}
document.getElementById("zp").onclick=function(){zoom(6)};
document.getElementById("zm").onclick=function(){zoom(-6)};
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
