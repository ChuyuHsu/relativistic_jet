"""人看的進度頁（單檔 HTML，不載任何外部資源）。

設計給手機直式看：大字、高對比、單欄、卡片式、自動重抓（15 秒）。
頁面本身不含任何資料；資料用讀鑰匙從 /status /branches /todo /metrics 抓。
鑰匙從網址的 #k=… 帶入（片段不會送到伺服器、也不進紀錄），帶入後立刻從網址列清掉，
存在這個分頁的 sessionStorage（關分頁就沒了）。動態內容一律用 textContent 寫入，不拼 HTML。
"""

PAGE_HTML = r"""<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="robots" content="noindex">
<title>合併佇列進度</title>
<style>
:root{--bg:#f4f5f7;--card:#fff;--ink:#14171c;--sub:#4b5563;--line:#d5d9e0;--ok:#0b7a3b;--warn:#a15c00;--bad:#b3261e;--run:#1d4ed8;--idle:#5b6472}
@media (prefers-color-scheme:dark){:root{--bg:#0f1115;--card:#1a1d24;--ink:#f3f4f6;--sub:#aab2bf;--line:#2d333d;--ok:#4ade80;--warn:#fbbf24;--bad:#f87171;--run:#7aa2ff;--idle:#9aa3b2}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:17px/1.5 -apple-system,system-ui,"PingFang TC","Noto Sans TC",sans-serif;padding:0 16px calc(24px + env(safe-area-inset-bottom))}
header{position:sticky;top:0;background:var(--bg);padding:14px 0 10px;z-index:2;border-bottom:1px solid var(--line)}
h1{font-size:20px;margin:0 0 6px}
.pill{display:inline-block;padding:2px 12px;border-radius:999px;font-weight:700;font-size:15px;border:2px solid currentColor}
.pill.ok{color:var(--ok)}.pill.warn{color:var(--warn)}.pill.bad{color:var(--bad)}
#upd{color:var(--sub);font-size:14px;margin-left:8px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:14px 0}
.stat{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:10px 12px}
.stat b{display:block;font-size:28px;line-height:1.1}.stat span{color:var(--sub);font-size:14px}
h2{font-size:18px;margin:22px 0 8px}
.card{background:var(--card);border:1px solid var(--line);border-left:6px solid var(--idle);border-radius:10px;padding:10px 12px;margin:8px 0;word-break:break-word}
.card .t{font-weight:700}.card .s{color:var(--sub);font-size:15px}
.card.bad{border-left-color:var(--bad)}.card.warn{border-left-color:var(--warn)}.card.ok{border-left-color:var(--ok)}.card.run{border-left-color:var(--run)}
.empty{color:var(--sub);padding:6px 2px}
#login{display:none;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px;margin:16px 0}
#login input{width:100%;font-size:17px;padding:10px;border-radius:8px;border:1px solid var(--line);background:var(--bg);color:var(--ink)}
#login button{margin-top:10px;width:100%;font-size:17px;padding:12px;border-radius:8px;border:0;background:var(--run);color:#fff;font-weight:700}
#err{color:var(--bad);margin:10px 0;font-weight:700}
</style>
</head>
<body>
<header>
  <h1>合併佇列進度</h1>
  <span id="state" class="pill ok">讀取中</span><span id="upd"></span>
</header>
<div id="login">
  <div>請貼上讀取鑰匙（只存在這個分頁）：</div>
  <input id="key" type="password" autocomplete="off" autocapitalize="off" spellcheck="false">
  <button id="go">確定</button>
</div>
<div id="err"></div>
<div id="app" style="display:none">
  <div class="grid" id="stats"></div>
  <h2>需要人處理</h2><div id="todo"></div>
  <h2>正在跑</h2><div id="running"></div>
  <h2>排隊中</h2><div id="queue"></div>
  <h2>其他分支</h2><div id="others"></div>
  <h2>最近合入</h2><div id="merged"></div>
</div>
<script>
(function(){
  var KEY_NAME="jet_read_key", key=null;
  try{ key=sessionStorage.getItem(KEY_NAME); }catch(e){}
  var m=/[#&]k=([^&]+)/.exec(location.hash);
  if(m){ key=decodeURIComponent(m[1]); try{sessionStorage.setItem(KEY_NAME,key);}catch(e){}
         history.replaceState(null,"",location.pathname+location.search); }
  var $=function(id){return document.getElementById(id)};
  var LABEL={pending:"待自測",selftesting:"自測中",selftest_green:"自測綠（等放行）",queued:"排隊",batching:"批測中",merged:"已合入",rejected:"退回",halted:"暫停"};
  function ago(t){ if(!t) return "-"; var s=Math.max(0,Date.now()/1000-t);
    if(s<90) return Math.round(s)+" 秒前"; if(s<5400) return Math.round(s/60)+" 分鐘前";
    if(s<172800) return Math.round(s/3600)+" 小時前"; return Math.round(s/86400)+" 天前"; }
  function el(tag,cls,text){ var e=document.createElement(tag); if(cls) e.className=cls; if(text!=null) e.textContent=text; return e; }
  function card(cls,title,sub){ var c=el("div","card "+cls); c.appendChild(el("div","t",title)); if(sub) c.appendChild(el("div","s",sub)); return c; }
  function fill(id,nodes,emptyText){ var box=$(id); box.textContent=""; if(!nodes.length){ box.appendChild(el("div","empty",emptyText)); return; } nodes.forEach(function(n){box.appendChild(n)}); }
  function get(p){ return fetch(p,{headers:{"X-Jet-Read-Key":key},cache:"no-store"}).then(function(r){
    if(r.status===401){ throw new Error("auth"); } if(!r.ok){ throw new Error("http "+r.status); } return r.json(); }); }
  function stat(n,label){ var d=el("div","stat"); d.appendChild(el("b",null,String(n))); d.appendChild(el("span",null,label)); return d; }
  function render(st,br,todo,me){
    var pill=$("state");
    if(st.paused){ pill.className="pill warn"; pill.textContent="已暫停"; }
    else if((todo.waiting||[]).length||(todo.events||[]).length){ pill.className="pill bad"; pill.textContent="有事待辦"; }
    else { pill.className="pill ok"; pill.textContent="運作中"; }
    $("upd").textContent="更新 "+new Date().toLocaleTimeString();
    var c=st.counts, s=$("stats"); s.textContent="";
    [[st.queue_len,"排隊"],[(c.selftesting||0)+(c.batching||0),"測試中"],[(c.rejected||0)+(c.halted||0),"退回／暫停"],[me.merged_24h,"24 小時合入"]]
      .forEach(function(x){ s.appendChild(stat(x[0],x[1])); });
    fill("todo",(todo.events||[]).map(function(e){ return card("bad",(e.branch||"系統")+"：待辦 "+e.kind,(e.detail||"")+"（"+ago(e.at)+"）"); })
      .concat((todo.waiting||[]).map(function(b){ return card(b.state==="halted"?"warn":"bad",b.name+"："+LABEL[b.state],(b.reject_reason||"")+"（"+ago(b.updated_at)+"）"); })),"沒有，很好。");
    fill("running",(st.running||[]).map(function(r){ return card("run",(r.branch||"批測")+"　"+r.kind+"／"+r.layer,"開始 "+ago(r.started_at)); }),"目前沒有工作在跑。");
    var q=br.filter(function(b){return b.state==="queued"||b.state==="batching";});
    fill("queue",q.map(function(b,i){ return card(b.state==="batching"?"run":"ok",(i+1)+". "+b.name+(b.parent?"（疊在 "+b.parent+"）":""),LABEL[b.state]+"　優先 "+b.prio_override); }),"隊伍是空的。");
    var o=br.filter(function(b){return ["queued","batching","merged","rejected","halted"].indexOf(b.state)<0;});
    fill("others",o.map(function(b){ return card("",b.name+(b.parent?"（疊在 "+b.parent+"）":""),LABEL[b.state]+(b.state==="selftest_green"&&!b.approved?"　未放行":"")+"　"+ago(b.updated_at)); }),"沒有。");
  }
  function mergedRender(list){ fill("merged",list.slice(0,10).map(function(b){ return card("ok",b.name,ago(b.updated_at)); }),"還沒有。"); }
  var timer=null;
  function load(){
    if(!key){ $("login").style.display="block"; $("app").style.display="none"; return; }
    $("login").style.display="none";
    Promise.all([get("/status"),get("/branches"),get("/todo"),get("/metrics"),get("/merged?limit=10")]).then(function(r){
      $("err").textContent=""; $("app").style.display="block"; render(r[0],r[1],r[2],r[3]); mergedRender(r[4]);
    }).catch(function(e){
      if(String(e.message)==="auth"){ key=null; try{sessionStorage.removeItem(KEY_NAME);}catch(x){} $("err").textContent="鑰匙不對，請重新輸入。"; $("login").style.display="block"; $("app").style.display="none"; }
      else{ $("err").textContent="讀取失敗（"+e.message+"），稍後自動重試。"; var p=$("state"); p.className="pill bad"; p.textContent="連不上"; }
    });
  }
  $("go").onclick=function(){ var v=$("key").value.trim(); if(!v) return; key=v; $("key").value=""; try{sessionStorage.setItem(KEY_NAME,key);}catch(e){} load(); };
  load(); timer=setInterval(load,15000);
})();
</script>
</body>
</html>
"""
