import os, datetime
from concurrent import futures
from typing import List
from fastapi import Query

import grpc
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
import uvicorn
import time
import requests

import intel_pb2
import intel_pb2_grpc

NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
BUILTWITH_API_KEY = os.getenv("BUILTWITH_API_KEY")

app = FastAPI(
    title="DNet Python Worker API",
    version="1.0.0",
    description="Public endpoints + gRPC",
)

# --- CORS: hızlı test için tüm originlere izin ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # prod'da kısıtlayabilirsin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}

def fetch_ssl_grade(domain: str | None) -> str:
    """
    SSL Labs API'den (fromCache=on) grade okumayı dener.
    Cache yoksa UNKNOWN döner; (tam akışta polling yapılabilir).
    """
    if not domain:
        return "UNKNOWN"
    try:
        url = f"https://api.ssllabs.com/api/v3/analyze?host={requests.utils.quote(domain)}&fromCache=on&all=done"
        r = requests.get(url, timeout=15)
        data = r.json()
        eps = data.get("endpoints") or []
        if eps and isinstance(eps, list):
            g = eps[0].get("grade")
            return g or "UNKNOWN"
        return "UNKNOWN"
    except Exception as e:
        return f"ERR:{e}"

def _http_get_with_backoff(url, tries=3, base_sleep=1.5, timeout=20):
    last_exc = None
    for i in range(tries):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 429:
                time.sleep(base_sleep * (i+1))
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_exc = e
            time.sleep(base_sleep * (i+1))
    if last_exc: raise last_exc

def fetch_tech_count(domain: str | None) -> int:
    if not domain:
        return 0
    if not BUILTWITH_API_KEY:
        # Demo / mock
        return 42
    try:
        url = f"https://api.builtwith.com/v21/api.json?KEY={requests.utils.quote(BUILTWITH_API_KEY)}&LOOKUP={requests.utils.quote(domain)}"
        r = _http_get_with_backoff(url)
        data = r.json()
        results = data.get("Results") or []
        count = 0
        for res in results:
            result = res.get("Result") or {}
            for p in (result.get("Paths") or []):
                techs = p.get("Technologies") or []
                count += len(techs)
        return count
    except Exception:
        return 0

def fetch_news(company_name: str, domain: str | None):
    """
    Dönen yapı:
    {
      "total": int,
      "articles": [
        {"title": str, "url": str, "source": str, "published_at": "YYYY-MM-DDTHH:MM:SSZ"}
      ]
    }
    """
    if not NEWSAPI_KEY:
        return {
            "total": 3,
            "articles": [
                {"title": f"{company_name} raises new funding", "url": "", "source": "Mock", "published_at": None},
                {"title": f"{company_name} launches product",    "url": "", "source": "Mock", "published_at": None},
                {"title": f"{company_name} security update",     "url": "", "source": "Mock", "published_at": None},
            ]
        }
    try:
        q = f'"{company_name}"'  # basit arama; dilersen domain ile zenginleştir
        url = f"https://newsapi.org/v2/everything?q={q}&sortBy=publishedAt&pageSize=5&apiKey={NEWSAPI_KEY}"
        r = requests.get(url, timeout=12)
        data = r.json()
        arts = []
        for a in data.get("articles", [])[:5]:
            arts.append({
                "title": a.get("title") or "",
                "url": a.get("url") or "",
                "source": (a.get("source") or {}).get("name") or "",
                "published_at": a.get("publishedAt") or None
            })
        return {"total": int(data.get("totalResults", 0)), "articles": arts}
    except Exception as e:
        return {"total": 0, "articles": [], "error": str(e)}


def _to_iso(ts):
    if not ts:
        return None
    # BuiltWith FirstDetected/LastDetected genelde epoch seconds
    try:
        return datetime.datetime.utcfromtimestamp(int(ts)).isoformat() + "Z"
    except Exception:
        return None

def _mock_breakdown():
    # Key yoksa veya API boş/hatalıysa en azından 3–4 item dönsün
    return [
        {"category": "Analytics", "name": "Google Analytics", "first_seen": "2022-01-10", "last_seen": "2025-08-01"},
        {"category": "CDN",       "name": "Cloudflare",       "first_seen": "2021-06-02", "last_seen": "2025-07-30"},
        {"category": "Payments",  "name": "Stripe",           "first_seen": "2023-02-15", "last_seen": "2025-08-25"},
    ]


def fetch_news_rich(company_name: str, domain: str | None):
    if not NEWSAPI_KEY:
        # mock rich
        return {
            "total": 3,
            "articles": [
                {"title": f"{company_name} raises new funding", "url": None, "published_at": None, "source": "mock"},
                {"title": f"{company_name} launches product",   "url": None, "published_at": None, "source": "mock"},
                {"title": f"{company_name} security update",    "url": None, "published_at": None, "source": "mock"},
            ],
        }
    try:
        q = f'"{company_name}"'
        url = f"https://newsapi.org/v2/everything?q={q}&sortBy=publishedAt&pageSize=10&apiKey={NEWSAPI_KEY}"
        r = requests.get(url, timeout=10)
        data = r.json()
        arts = []
        for a in (data.get("articles") or [])[:10]:
            arts.append({
                "title": a.get("title"),
                "url": (a.get("url") or None),
                "published_at": (a.get("publishedAt") or None),
                "source": ((a.get("source") or {}).get("name") or "NewsAPI"),
            })
        return {"total": int(data.get("totalResults", 0)), "articles": arts}
    except Exception as e:
        return {"total": 0, "articles": [{"title": f"error: {e}", "url": None, "published_at": None, "source": "error"}]}


def fetch_tech_breakdown(domain: str | None):
    if not domain:
        print("[tech-breakdown] empty domain -> []")
        return []

    force_mock = os.getenv("BUILTWITH_FORCE_MOCK", "").lower() == "true"
    key = os.getenv("BUILTWITH_API_KEY")

    if force_mock or not key:
        items = _mock_breakdown()
        print(f"[tech-breakdown] using MOCK (force={force_mock}, has_key={bool(key)}), count={len(items)}")
        return items

    try:
        url = (
            "https://api.builtwith.com/v21/api.json"
            f"?KEY={requests.utils.quote(key)}&LOOKUP={requests.utils.quote(domain)}"
        )
        r = _http_get_with_backoff(url)
        data = r.json()

        items = []
        for res in data.get("Results", []):
            result = res.get("Result", {})
            for p in result.get("Paths", []):
                for t in p.get("Technologies", []):
                    items.append({
                        "category":   t.get("Tag") or t.get("ParentCategory") or None,
                        "name":       t.get("Name", ""),
                        "first_seen": t.get("FirstDetected") or t.get("FirstSeen") or None,
                        "last_seen":  t.get("LastDetected")  or t.get("LastSeen")  or None,
                    })

        if not items:
            # API cevap verdi ama 0 çıktı -> mock’a düş
            items = _mock_breakdown()
            print(f"[tech-breakdown] API empty -> MOCK fallback, count={len(items)}")
        else:
            print(f"[tech-breakdown] API success, count={len(items)}")

        return items

    except Exception as e:
        # Ağ hatası, rate-limit vb. -> mock’a düş
        items = _mock_breakdown()
        print(f"[tech-breakdown] API error -> MOCK fallback: {e}, count={len(items)}")
        return items

class IntelWorkerServicer(intel_pb2_grpc.IntelWorkerServicer):
    def GetNewsCount(self, request, context):
        total, titles = fetch_news(request.company_name, request.domain)
        return intel_pb2.GetNewsReply(total=total, sample_titles=titles)
    def GetSslGrade(self, request, context):   # <= YENİ
        grade = fetch_ssl_grade(request.domain)
        return intel_pb2.SslReply(grade=grade)
    def GetTechCount(self, request, context):
        return intel_pb2.TechReply(count=fetch_tech_count(request.domain))
    def GetTechBreakdown(self, request, context):
        items = fetch_tech_breakdown(request.domain)
        return intel_pb2.TechBreakdownReply(
            items=[intel_pb2.TechnologyItem(
                category=i.get("category") or "",
                name=i.get("name") or "",
                first_seen=i.get("first_seen") or "",
                last_seen=i.get("last_seen") or "",
            ) for i in items]
        )                

app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "service": "python-worker", "version": "1.0.0"}

@app.get("/news")
def news(company_name: str, domain: str = ""):
    return JSONResponse(fetch_news(company_name, domain))

@app.get("/news-count")
def news_count(company_name: str, domain: str = ""):
    d = fetch_news(company_name, domain)
    return JSONResponse({"total": d.get("total", 0),
                         "sample_titles": [a["title"] for a in d.get("articles", [])[:3]]})

@app.get("/news-search")
def news_search(company_name: str = Query(..., alias="company_name"), domain: str = ""):
    data = fetch_news_rich(company_name, domain)
    return JSONResponse(data)

@app.get("/ssl-grade")
def ssl_grade(domain: str = ""):
    return JSONResponse({"grade": fetch_ssl_grade(domain)})

def serve_grpc():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    intel_pb2_grpc.add_IntelWorkerServicer_to_server(IntelWorkerServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("gRPC server listening on :50051")
    return server

@app.get("/tech-count")
def tech_count(domain: str = ""):
    return JSONResponse({"count": fetch_tech_count(domain)})

@app.get("/tech-breakdown")
def tech_breakdown(domain: str = ""):
    items = fetch_tech_breakdown(domain)
    return JSONResponse({"items": items})

@app.get("/breach-count")
async def breach_count(domain: str):
    # DEMO: ücretli API yerine mock veri
    demo_data = {
        "atlassian.com": 5,
        "microsoft.com": 12,
        "google.com": 20
    }
    total = demo_data.get(domain.lower(), 0)
    return {"total": total, "source": "mock"}

if __name__ == "__main__":
    # gRPC + HTTP birlikte
    server = serve_grpc()
    port = int(os.getenv("PORT", "8081"))  # Render 'Web Service' için şart: PORT'u dinle
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
    server.wait_for_termination()
