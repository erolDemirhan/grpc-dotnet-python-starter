
using Intel.Proto;
using Grpc.Net.Client;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text;
using System.Net.Http.Headers;
using System.Linq;
using System.Text.Json.Serialization;

var builder = WebApplication.CreateBuilder(args);

var port = Environment.GetEnvironmentVariable("PORT") ?? "8080";
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
var app = builder.Build();

// Enable plaintext gRPC (h2c) for local docker network
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

app.MapGet("/", () => Results.Ok(new { ok = true, service = "orchestrator", version = "1.0.0" }));

app.MapPost("/ingest/news", async (WriteNewsReq req) =>
{
    if (string.IsNullOrWhiteSpace(req.CompanyName))
        return Results.BadRequest(new { ok = false, message = "CompanyName is required" });

    try
    {
        var companyId = await Supa.GetOrCreateCompanyByDomainAsync(req.CompanyName, req.Domain);

        // News upsert — aynı haberi mükerrer eklememek için title+url kombosunu kullanıyoruz.
        // (DB tarafında unique index önerisi aşağıda)
        foreach (var a in req.Articles ?? new())
        {
            await Supa.UpsertAsync("news_article", new {
                company_id   = companyId,
                title        = a.Title,
                url          = a.Url,
                source       = a.Source ?? "external",
                published_at = Helpers.ParseIso(a.PublishedAt),
                captured_at  = DateTime.UtcNow
            }, "company_id,title,url");
        }

        // İsteğe bağlı: toplam sayıyı datapoint olarak güncelle
        // (burada tek seferde saymak istemezsen atlayabilirsin)
        return Results.Ok(new { ok = true, companyId, written = req.Articles?.Count ?? 0 });
    }
    catch (Exception ex)
    {
        return Results.Problem(detail: ex.Message, statusCode: 500);
    }
});


app.MapPost("/ingest/preview", async (PreviewReq req) =>
{
    var jobId   = Guid.NewGuid().ToString("N");
    var useHttp = Environment.GetEnvironmentVariable("USE_HTTP_WORKER")?.ToLowerInvariant() == "true";
    var resp    = new PreviewResp { JobId = jobId, CompanyName = req.CompanyName, Domain = req.Domain };

    try
    {
        if (useHttp)
        {
            using var http = new HttpClient();

            // ana şirket
            var mainTask = SnapshotHelpers.GetSnapshotAsync(true, req.CompanyName, req.Domain, sharedHttp: http);

            // rakipler
            var compTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => SnapshotHelpers.GetSnapshotAsync(true, c.Name, c.Domain, sharedHttp: http))
                .ToArray();

            await Task.WhenAll(new Task[] { mainTask }.Concat(compTasks));

            var main = await mainTask;
            resp.NewsCount    = main.NewsCount;
            resp.SampleTitles = main.SampleTitles;
            resp.SslGrade     = main.SslGrade;
            resp.TechCount    = main.TechCount;

            resp.Competitors = compTasks.Select(t => t.Result).ToList();
        }
        else
        {
            var target  = Environment.GetEnvironmentVariable("PY_WORKER_GRPC_URL") ?? "http://python-worker:50051";
            var channel = GrpcChannel.ForAddress(target);
            var client  = new IntelWorker.IntelWorkerClient(channel);

            var mainTask = SnapshotHelpers.GetSnapshotAsync(false, req.CompanyName, req.Domain, sharedClient: client);
            var compTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => SnapshotHelpers.GetSnapshotAsync(false, c.Name, c.Domain, sharedClient: client))
                .ToArray();

            await Task.WhenAll(new Task[] { mainTask }.Concat(compTasks));

            var main = await mainTask;
            resp.NewsCount    = main.NewsCount;
            resp.SampleTitles = main.SampleTitles;
            resp.SslGrade     = main.SslGrade;
            resp.TechCount    = main.TechCount;

            resp.Competitors = compTasks.Select(t => t.Result).ToList();
        }
    }
    catch (Exception ex)
    {
        resp.Errors.Add($"preview error: {ex.Message}");
    }

    return Results.Ok(resp);
});


app.MapPost("/ingest/confirm", async (ConfirmReq req) =>
{
    var useHttp = Environment.GetEnvironmentVariable("USE_HTTP_WORKER")?.ToLowerInvariant() == "true";
    var resp = new ConfirmResp {
        JobId = Guid.NewGuid().ToString("N"),
        CompanyName = req.CompanyName,
        Domain = req.Domain
    };

    // 1) Orkestrasyon: ana + competitors snapshot'ları paralel
    CompanySnapshot main;
    List<CompanySnapshot> comps;
    List<TechItem> mainTechs = new();
    List<List<TechItem>> compTechs = new();
    NewsSearchDto? mainNews = null;
    List<NewsSearchDto> compNews = new();
    List<NewsArticleDto> mainArticles = new();
    List<List<NewsArticleDto>> compArticlesList = new();
    BreachDto? mainBreach = null;
    List<int> compBreachTotals = new();    
    

    // ---- NEW: aggregate holders (try'ın ÜSTÜNDE!) ----
    JobsSummaryDto? jobsMain = null;
    List<JobsSummaryDto> jobsComps = new();

    GithubStatsDto? ghMain = null;
    List<GithubStatsDto> ghComps = new();

    PrivacyDto? privMain = null;
    List<PrivacyDto> privComps = new();

    HnMentionsDto? hnMain = null;
    List<HnMentionsDto> hnComps = new();

    EdgarSummaryDto? edgarMain = null;
    List<EdgarSummaryDto> edgarComps = new();

    RevenueEstimateDto? revMain = null;
    List<RevenueEstimateDto> revComps = new();
    try
    {
        if (useHttp)
        {
            var httpUrl = Environment.GetEnvironmentVariable("PY_WORKER_HTTP_URL") ?? "http://python-worker:8081";
            using var http = new HttpClient();

            // --- SNAPSHOT PARALLEL ---
            var mainTask = SnapshotHelpers.GetSnapshotAsync(true, req.CompanyName, req.Domain, sharedHttp: http);
            var compTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => SnapshotHelpers.GetSnapshotAsync(true, c.Name, c.Domain, sharedHttp: http))
                .ToArray();

            await Task.WhenAll(new Task[] { mainTask }.Concat(compTasks));

            main = await mainTask;
            comps = compTasks.Select(t => t.Result).ToList();

            // --- TECH BREAKDOWN PARALLEL (HTTP) ---
            var mainBreakdownTask = http.GetFromJsonAsync<TechBreakdownDto>(
                $"{httpUrl}/tech-breakdown?domain={Uri.EscapeDataString(req.Domain ?? "")}");

            var compBreakdownTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => http.GetFromJsonAsync<TechBreakdownDto>(
                    $"{httpUrl}/tech-breakdown?domain={Uri.EscapeDataString(c.Domain ?? "")}"))
                .ToArray();

            // --- NEWS RICH PARALLEL (HTTP) ---  <<<< YENİ
            var mainNewsTask = http.GetFromJsonAsync<NewsSearchDto>(
                $"{httpUrl}/news-search?company_name={Uri.EscapeDataString(req.CompanyName)}&domain={Uri.EscapeDataString(req.Domain ?? "")}");
            var compNewsTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => http.GetFromJsonAsync<NewsSearchDto>(
                    $"{httpUrl}/news-search?company_name={Uri.EscapeDataString(c.Name)}&domain={Uri.EscapeDataString(c.Domain ?? "")}"))
                .ToArray();

            // --- JOBS (Greenhouse/Lever) ---
            var mainJobsTask = http.GetFromJsonAsync<JobsSummaryDto>(
                $"{httpUrl}/jobs-summary?domain={Uri.EscapeDataString(req.Domain ?? "")}");
            var compJobsTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => http.GetFromJsonAsync<JobsSummaryDto>(
                    $"{httpUrl}/jobs-summary?domain={Uri.EscapeDataString(c.Domain ?? "")}"))
                .ToArray();

            // --- GITHUB (unauth public) ---
            // not: org tespitini python-worker üstlenecek (domain->muhtemel org eşleme + fallback)
            var mainGhTask = http.GetFromJsonAsync<GithubStatsDto>(
                $"{httpUrl}/github-stats?domain={Uri.EscapeDataString(req.Domain ?? "")}&company={Uri.EscapeDataString(req.CompanyName)}");
            var compGhTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => http.GetFromJsonAsync<GithubStatsDto>(
                    $"{httpUrl}/github-stats?domain={Uri.EscapeDataString(c.Domain ?? "")}&company={Uri.EscapeDataString(c.Name)}"))
                .ToArray();

            // --- PRIVACY INDICATORS ---
            var mainPrivacyTask = http.GetFromJsonAsync<PrivacyDto>(
                $"{httpUrl}/privacy-scan?domain={Uri.EscapeDataString(req.Domain ?? "")}");
            var compPrivacyTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => http.GetFromJsonAsync<PrivacyDto>(
                    $"{httpUrl}/privacy-scan?domain={Uri.EscapeDataString(c.Domain ?? "")}"))
                .ToArray();

            // --- PUBLIC SENTIMENT (HN mentions 30d) ---
            var mainHnTask = http.GetFromJsonAsync<HnMentionsDto>(
                $"{httpUrl}/hn-mentions?query={Uri.EscapeDataString(req.CompanyName)}");
            var compHnTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => http.GetFromJsonAsync<HnMentionsDto>(
                    $"{httpUrl}/hn-mentions?query={Uri.EscapeDataString(c.Name)}"))
                .ToArray();

            // --- FINANCIAL STATEMENTS (SEC EDGAR — US public) ---
            var mainEdgarTask = http.GetFromJsonAsync<EdgarSummaryDto>(
                $"{httpUrl}/sec-filings?company={Uri.EscapeDataString(req.CompanyName)}&domain={Uri.EscapeDataString(req.Domain ?? "")}");
            var compEdgarTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => http.GetFromJsonAsync<EdgarSummaryDto>(
                    $"{httpUrl}/sec-filings?company={Uri.EscapeDataString(c.Name)}&domain={Uri.EscapeDataString(c.Domain ?? "")}"))
                .ToArray();

            // --- REVENUE ESTIMATE (ücretsiz heuristics) ---
            var mainRevTask = http.GetFromJsonAsync<RevenueEstimateDto>(
                $"{httpUrl}/revenue-estimate?company={Uri.EscapeDataString(req.CompanyName)}&domain={Uri.EscapeDataString(req.Domain ?? "")}");
            var compRevTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => http.GetFromJsonAsync<RevenueEstimateDto>(
                    $"{httpUrl}/revenue-estimate?company={Uri.EscapeDataString(c.Name)}&domain={Uri.EscapeDataString(c.Domain ?? "")}"))
                .ToArray();

            // --- BREACH COUNT PARALLEL (HTTP) ---  <<<< YENİ
            var mainBreachTask = http.GetFromJsonAsync<BreachDto>(
                $"{httpUrl}/breach-count?domain={Uri.EscapeDataString(req.Domain ?? "")}");

            var compBreachTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => http.GetFromJsonAsync<BreachDto>(
                    $"{httpUrl}/breach-count?domain={Uri.EscapeDataString(c.Domain ?? "")}"))
                .ToArray();

            // hepsini aynı anda bekle
            await Task.WhenAll(
                new Task[] { mainBreakdownTask!, mainNewsTask!, mainBreachTask!, mainJobsTask!, mainGhTask!, mainPrivacyTask!, mainHnTask!, mainEdgarTask!, mainRevTask! }
                .Concat(compBreakdownTasks.Select(t => (Task)t!))
                .Concat(compNewsTasks.Select(t => (Task)t!))
                .Concat(compBreachTasks.Select(t => (Task)t!))
                .Concat(compJobsTasks.Select(t => (Task)t!))
                .Concat(compGhTasks.Select(t => (Task)t!))
                .Concat(compPrivacyTasks.Select(t => (Task)t!))
                .Concat(compHnTasks.Select(t => (Task)t!))
                .Concat(compEdgarTasks.Select(t => (Task)t!))
                .Concat(compRevTasks.Select(t => (Task)t!))
            );

            // breakdown sonuçları
            mainTechs = mainBreakdownTask?.Result?.Items ?? new List<TechItem>();
            compTechs = compBreakdownTasks.Select(t => t?.Result?.Items ?? new List<TechItem>()).ToList();



            // breach sonuçları
            mainBreach = mainBreachTask?.Result;
            compBreachTotals = compBreachTasks
                .Select(t => t?.Result?.total ?? 0)
                .ToList();

            // news sonuçlarını alan değişkenlere ata ve sampleTitles’ı zenginleştir
            mainNews = mainNewsTask?.Result;
            if (mainNews is not null && mainNews.Articles?.Count > 0)
            {
                // preview/response için örnek başlıklar
                resp.SampleTitles = mainNews.Articles.Select(a => a.Title).Take(5).ToArray();
                // persist için tam liste
                mainArticles = mainNews.Articles;
            }

            // rakip haberleri
            compArticlesList = compNewsTasks
                .Select(t => (t?.Result?.Articles ?? new List<NewsArticleDto>()))
                .ToList();

            compNews = compNewsTasks.Select(t => t?.Result ?? new NewsSearchDto(0, new())).ToList();

            // ---- AGGREGATES FROM OTHER TASKS (JOBS, GITHUB, PRIVACY, HN, EDGAR, REVENUE) ----
            jobsMain = mainJobsTask?.Result;
            jobsComps = compJobsTasks.Select(t => t?.Result ?? new JobsSummaryDto(0, new())).ToList();

            ghMain = mainGhTask?.Result ?? new GithubStatsDto(null, 0, 0, 0, 0, 0);
            ghComps = compGhTasks.Select(t => t?.Result ?? new GithubStatsDto(null, 0, 0, 0, 0, 0)).ToList();

            privMain = mainPrivacyTask?.Result ?? new PrivacyDto(null, null, false);
            privComps = compPrivacyTasks.Select(t => t?.Result ?? new PrivacyDto(null, null, false)).ToList();

            hnMain = mainHnTask?.Result ?? new HnMentionsDto(0);
            hnComps = compHnTasks.Select(t => t?.Result ?? new HnMentionsDto(0)).ToList();

            edgarMain = mainEdgarTask?.Result ?? new EdgarSummaryDto(0, null);
            edgarComps = compEdgarTasks.Select(t => t?.Result ?? new EdgarSummaryDto(0, null)).ToList();

            revMain = mainRevTask?.Result ?? new RevenueEstimateDto("none", null);
            revComps = compRevTasks.Select(t => t?.Result ?? new RevenueEstimateDto("none", null)).ToList();


        }
        else
        {
            var target = Environment.GetEnvironmentVariable("PY_WORKER_GRPC_URL") ?? "http://python-worker:50051";
            using var channel = GrpcChannel.ForAddress(target);
            var client = new IntelWorker.IntelWorkerClient(channel);

            // --- SNAPSHOT PARALLEL (gRPC) ---
            var mainTask = SnapshotHelpers.GetSnapshotAsync(false, req.CompanyName, req.Domain, sharedClient: client);
            var compTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => SnapshotHelpers.GetSnapshotAsync(false, c.Name, c.Domain, sharedClient: client))
                .ToArray();

            // --- TECH BREAKDOWN PARALLEL (gRPC) ---
            var mainBreakdownTask = client.GetTechBreakdownAsync(new TechRequest { Domain = req.Domain ?? "" }).ResponseAsync;
            var compBreakdownTasks = (req.Competitors ?? Array.Empty<Competitor>())
                .Select(c => client.GetTechBreakdownAsync(new TechRequest { Domain = c.Domain ?? "" }).ResponseAsync)
                .ToArray();

            await Task.WhenAll(
                new Task[] { mainTask, mainBreakdownTask }
                .Concat(compTasks.Select(t => (Task)t))
                .Concat(compBreakdownTasks.Select(t => (Task)t))
            );

            main = await mainTask;
            comps = compTasks.Select(t => t.Result).ToList();

            var mainReply = await mainBreakdownTask;
            mainTechs = mainReply.Items
                .Select(i => new TechItem(i.Category, i.Name, i.FirstSeen, i.LastSeen))
                .ToList();

            compTechs = new List<List<TechItem>>();
            foreach (var t in compBreakdownTasks)
            {
                var r = await t;
                compTechs.Add(r.Items.Select(i => new TechItem(i.Category, i.Name, i.FirstSeen, i.LastSeen)).ToList());
            }
            // gRPC yolunda şimdilik breach verisi yok: 0 ile doldur
            mainBreach = new BreachDto(0);
            compBreachTotals = Enumerable.Repeat(0, comps.Count).ToList();
        }

        resp.NewsCount = main.NewsCount;
        if (resp.SampleTitles is null || resp.SampleTitles.Length == 0)
            resp.SampleTitles = main.SampleTitles;
        resp.SslGrade = main.SslGrade;
        resp.TechCount = (mainTechs != null && mainTechs.Count > 0)
        ? mainTechs.Count
        : (resp.TechCount ?? 0);
        resp.Competitors = comps;

    }
    catch (Exception ex)
    {
        resp.Errors.Add($"orchestration error: {ex.Message}");
        main = new CompanySnapshot { CompanyName = req.CompanyName, Domain = req.Domain };
        comps = new List<CompanySnapshot>();
    }

        // --- ensure aggregates have defaults (so IDE stops greying and gRPC path is safe) ---
        jobsMain  ??= new JobsSummaryDto(0, new());
        if (jobsComps.Count == 0 && resp.Competitors != null)
            jobsComps = Enumerable.Repeat(new JobsSummaryDto(0, new()), resp.Competitors.Count).ToList();

        ghMain  ??= new GithubStatsDto(null, 0, 0, 0, 0, 0);
        if (ghComps.Count == 0 && resp.Competitors != null)
            ghComps = Enumerable.Repeat(new GithubStatsDto(null, 0, 0, 0, 0, 0), resp.Competitors.Count).ToList();

        privMain ??= new PrivacyDto(null, null, false);
        if (privComps.Count == 0 && resp.Competitors != null)
            privComps = Enumerable.Repeat(new PrivacyDto(null, null, false), resp.Competitors.Count).ToList();

        hnMain ??= new HnMentionsDto(0);
        if (hnComps.Count == 0 && resp.Competitors != null)
            hnComps = Enumerable.Repeat(new HnMentionsDto(0), resp.Competitors.Count).ToList();

        edgarMain ??= new EdgarSummaryDto(0, null);
        if (edgarComps.Count == 0 && resp.Competitors != null)
            edgarComps = Enumerable.Repeat(new EdgarSummaryDto(0, null), resp.Competitors.Count).ToList();

        revMain ??= new RevenueEstimateDto("none", null);
        if (revComps.Count == 0 && resp.Competitors != null)
            revComps = Enumerable.Repeat(new RevenueEstimateDto("none", null), resp.Competitors.Count).ToList();

        mainBreach ??= new BreachDto(0);
        if (compBreachTotals.Count == 0 && resp.Competitors != null)
            compBreachTotals = Enumerable.Repeat(0, resp.Competitors.Count).ToList();


    resp.MainTechsCount = mainTechs?.Count ?? 0;
    resp.CompTechsCounts = (compTechs ?? new List<List<TechItem>>())
        .Select(l => l?.Count ?? 0).ToArray();
    // 2) Persist: get-or-create by domain + datapoint/news insert + tech breakdown + raw JSON upload
    try
    {
        // küçük yardımcı

        // ANA ŞİRKET — domain varsa mevcut ID’yi bul, yoksa oluştur
        var companyId = await Supa.GetOrCreateCompanyByDomainAsync(req.CompanyName, req.Domain);
        resp.CompanyId = companyId;

        // aggregate datapoints (main)
        await Supa.InsertAsync("datapoint", new {
            company_id  = companyId,
            label       = "news_count_90d",
            path        = "news.total",
            value_json  = new { total = resp.NewsCount },
            source_api  = "NewsAPI",
            captured_at = DateTime.UtcNow
        });
        await Supa.InsertAsync("datapoint", new {
            company_id  = companyId,
            label       = "ssl_grade",
            path        = "security.ssl_labs.grade",
            value_json  = new { grade = resp.SslGrade },
            source_api  = "SSL Labs",
            captured_at = DateTime.UtcNow
        });
        await Supa.InsertAsync("datapoint", new {
            company_id  = companyId,
            label       = "tech_count_total",
            path        = "builtwith.count",
            value_json  = new { count = mainTechs?.Count ?? 0 },   // <— breakdown’dan
            source_api  = "BuiltWith",
            captured_at = DateTime.UtcNow
        });

        await Supa.InsertAsync("datapoint", new {
            company_id  = companyId,
            label       = "breach_count_total",
            path        = "security.hibp.breaches",
            value_json  = new { total = mainBreach?.total ?? 0 },   // << HTTP mock sonucu
            source_api  = "MockHIBP",
            captured_at = DateTime.UtcNow
        });

        // === (NEW) MAIN AGGREGATES ===

        // JOBS (main)
        await Supa.InsertAsync("datapoint", new {
            company_id  = companyId,
            label       = "jobs_total",
            path        = "human_capital.jobs.total",
            value_json  = new { total = (jobsMain?.Total ?? 0) },
            source_api  = "HTTP:jobs-search",
            captured_at = DateTime.UtcNow
        });

        // GITHUB (main)
        await Supa.InsertAsync("datapoint", new {
            company_id  = companyId,
            label       = "github_repo_count",
            path        = "software.github.repos",
            value_json  = new { count = (ghMain?.Repos ?? 0) },   // eskiden RepoCount
            source_api  = "GitHubAPI",
            captured_at = DateTime.UtcNow
        });
        await Supa.InsertAsync("datapoint", new {
            company_id  = companyId,
            label       = "github_total_stars",
            path        = "software.github.total_stars",
            value_json  = new { total = (ghMain?.Stars ?? 0) },   // eskiden TotalStars
            source_api  = "GitHubAPI",
            captured_at = DateTime.UtcNow
        });

        // PRIVACY (main)
        await Supa.InsertAsync("datapoint", new {
            company_id  = companyId,
            label       = "privacy_maturity",
            path        = "privacy.maturity",
            value_json  = new {
            score = 0, // elinde skor yoksa şimdilik 0 bırak
            has_policy = !string.IsNullOrWhiteSpace(privMain?.PrivacyPolicyUrl),
            dpo_email = (privMain?.DpoEmailFound ?? false)
        },
            source_api  = "HTTP:privacy-scan",
            captured_at = DateTime.UtcNow
        });

        // HACKER NEWS (main)
        await Supa.InsertAsync("datapoint", new {
            company_id  = companyId,
            label       = "hn_mentions_total",
            path        = "public_sentiment.hn.mentions",
            value_json  = new { total = (hnMain?.Last30d ?? 0) }, // eskiden Total
            source_api  = "HNAPI",
            captured_at = DateTime.UtcNow
        });

        // EDGAR / FİLİNGS (main)
        await Supa.InsertAsync("datapoint", new {
            company_id  = companyId,
            label       = "edgar_filings_total",
            path        = "financials.edgar.total_filings",
            value_json  = new { total = (edgarMain?.Filings90d ?? 0) }, // eskiden Total
            source_api  = "SEC-EDGAR",
            captured_at = DateTime.UtcNow
        });

        // REVENUE ESTIMATE (main)
        await Supa.InsertAsync("datapoint", new {
            company_id  = companyId,
            label       = "revenue_estimate",
            path        = "financials.revenue.estimate",
            value_json  = new { method = (revMain?.Method ?? "none"), value = revMain?.EstimateUsd }, // eskiden Value
            source_api  = "Estimator",
            captured_at = DateTime.UtcNow
        });


        // MAIN news: varsa rich veriyle yaz, yoksa fallback başlık listesi
        if (mainNews is not null && mainNews.Articles?.Count > 0)
        {
            foreach (var a in mainNews.Articles)
            {
                await Supa.UpsertAsync("news_article", new
                {
                    company_id = companyId,
                    title = a.Title,
                    url = a.Url,
                    source = a.Source ?? "NewsAPI",
                    published_at = string.IsNullOrWhiteSpace(a.PublishedAt) ? (DateTime?)null : DateTime.Parse(a.PublishedAt),
                    captured_at = DateTime.UtcNow
                }, "company_id,title");
            }
        }
        else
        {
            // küçük yardımcı: ISO tarih parse (zaten var)

            // --- main news persist (zengin alanlar)
            foreach (var a in mainArticles ?? new List<NewsArticleDto>())
            {
                // Upsert öneriyorum: aynı haberi tekrar eklemeyelim.
                await Supa.UpsertAsync("news_article", new
                {
                    company_id = companyId,
                    title = a.Title,
                    url = a.Url,
                    source = a.Source ?? "NewsAPI",
                    published_at = Helpers.ParseIso(a.PublishedAt),
                    captured_at = DateTime.UtcNow
                }, "company_id,title,url"); // on_conflict kolonları (aşağıda not var)
            }
        }


        // --- main breakdown persist ---
        foreach (var t in mainTechs)
        {
            await Supa.UpsertAsync("technology", new {
                company_id  = companyId,
                category    = t.Category ?? "",   // null yerine boş string
                name        = t.Name,
                first_seen  = Helpers.ParseIso(t.FirstSeen),
                last_seen   = Helpers.ParseIso(t.LastSeen),
                source_api  = "BuiltWith",
                captured_at = DateTime.UtcNow
            }, "company_id,name,category");
        }

        // RAKİPLER — her biri için get-or-create + datapoints + news + breakdown
        for (int idx = 0; idx < resp.Competitors.Count; idx++)
        {
            var compSnap = resp.Competitors[idx];
            var compId = await Supa.GetOrCreateCompanyByDomainAsync(compSnap.CompanyName, compSnap.Domain, competitorOf: companyId);
            var compList = (idx < compTechs.Count) ? compTechs[idx] : new List<TechItem>();

            await Supa.InsertAsync("datapoint", new {
                company_id  = compId,
                label       = "news_count_90d",
                path        = "news.total",
                value_json  = new { total = compSnap.NewsCount },
                source_api  = "NewsAPI",
                captured_at = DateTime.UtcNow
            });
            await Supa.InsertAsync("datapoint", new {
                company_id  = compId,
                label       = "ssl_grade",
                path        = "security.ssl_labs.grade",
                value_json  = new { grade = compSnap.SslGrade },
                source_api  = "SSL Labs",
                captured_at = DateTime.UtcNow
            });
            await Supa.InsertAsync("datapoint", new {
                company_id  = compId,
                label       = "tech_count_total",
                path        = "builtwith.count",
                value_json  = new { count = compList?.Count ?? 0 },   // <— breakdown’dan
                source_api  = "BuiltWith",
                captured_at = DateTime.UtcNow
            });

        // === (NEW) COMP AGGREGATES ===

        // JOBS (comp)
        var jobsComp = (idx < jobsComps.Count) ? jobsComps[idx] : new JobsSummaryDto(0, new());
        await Supa.InsertAsync("datapoint", new {
            company_id  = compId,
            label       = "jobs_total",
            path        = "human_capital.jobs.total",
            value_json  = new { total = jobsComp.Total },
            source_api  = "HTTP:jobs-search",
            captured_at = DateTime.UtcNow
        });

        // GITHUB (comp)
        var ghComp = (idx < ghComps.Count) ? ghComps[idx] : new GithubStatsDto(null,0,0,0,0,0);
        await Supa.InsertAsync("datapoint", new {
            company_id  = compId,
            label       = "github_repo_count",
            path        = "software.github.repos",
            value_json  = new { count = ghComp.Repos },
            source_api  = "GitHubAPI",
            captured_at = DateTime.UtcNow
        });
        await Supa.InsertAsync("datapoint", new {
            company_id  = compId,
            label       = "github_total_stars",
            path        = "software.github.total_stars",
            value_json  = new { total = ghComp.Stars },
            source_api  = "GitHubAPI",
            captured_at = DateTime.UtcNow
        });

        // PRIVACY (comp)
        var privComp = (idx < privComps.Count) ? privComps[idx] : new PrivacyDto(null,null,false);
        await Supa.InsertAsync("datapoint", new {
            company_id  = compId,
            label       = "privacy_maturity",
            path        = "privacy.maturity",
            value_json  = new {
                score = 0,
                has_policy = !string.IsNullOrWhiteSpace(privComp.PrivacyPolicyUrl),
                dpo_email = (privComp.DpoEmailFound)
            },
            source_api  = "HTTP:privacy-scan",
            captured_at = DateTime.UtcNow
        });

        // HACKER NEWS (comp)
        var hnComp = (idx < hnComps.Count) ? hnComps[idx] : new HnMentionsDto(0);
        await Supa.InsertAsync("datapoint", new {
            company_id  = compId,
            label       = "hn_mentions_total",
            path        = "public_sentiment.hn.mentions",
            value_json  = new { total = hnComp.Last30d },
            source_api  = "HNAPI",
            captured_at = DateTime.UtcNow
        });

        // EDGAR (comp)
        var edgarComp = (idx < edgarComps.Count) ? edgarComps[idx] : new EdgarSummaryDto(0,null);
        await Supa.InsertAsync("datapoint", new {
            company_id  = compId,
            label       = "edgar_filings_total",
            path        = "financials.edgar.total_filings",
            value_json  = new { total = edgarComp.Filings90d },
            source_api  = "SEC-EDGAR",
            captured_at = DateTime.UtcNow
        });

        // REVENUE (comp)
        var revComp = (idx < revComps.Count) ? revComps[idx] : new RevenueEstimateDto("none", null);
        await Supa.InsertAsync("datapoint", new {
            company_id  = compId,
            label       = "revenue_estimate",
            path        = "financials.revenue.estimate",
            value_json  = new { method = (revComp.Method ?? "none"), value = revComp.EstimateUsd },
            source_api  = "Estimator",
            captured_at = DateTime.UtcNow
        });

            // rakip breach count
            var compBreachTotal = (idx < compBreachTotals.Count) ? compBreachTotals[idx] : 0;
            await Supa.InsertAsync("datapoint", new {
                company_id  = compId,
                label       = "breach_count_total",
                path        = "security.hibp.breaches",
                value_json  = new { total = compBreachTotal },
                source_api  = "MockHIBP",
                captured_at = DateTime.UtcNow
            });

            // competitor news: varsa rich veri, yoksa fallback
            var compNewsDto = (idx < compNews.Count) ? compNews[idx] : new NewsSearchDto(0, new());
            if (compNewsDto.Articles?.Count > 0)
            {
                foreach (var a in compNewsDto.Articles)
                {
                    await Supa.UpsertAsync("news_article", new {
                        company_id   = compId,
                        title        = a.Title,
                        url          = a.Url,
                        source       = a.Source ?? "NewsAPI",
                        published_at = string.IsNullOrWhiteSpace(a.PublishedAt) ? (DateTime?)null : DateTime.Parse(a.PublishedAt),
                        captured_at  = DateTime.UtcNow
                    }, "company_id,title");
                }
            }
            else
            {
                var compArticles = (idx < compArticlesList.Count) ? compArticlesList[idx] : new List<NewsArticleDto>();
                foreach (var a in compArticles)
                {
                    await Supa.UpsertAsync("news_article", new {
                        company_id   = compId,
                        title        = a.Title,
                        url          = a.Url,
                        source       = a.Source ?? "NewsAPI",
                        published_at = Helpers.ParseIso(a.PublishedAt),
                        captured_at  = DateTime.UtcNow
                    }, "company_id,title,url");
                }
            }


            // competitor breakdown
            compSnap.TechCount = compList?.Count ?? compSnap.TechCount;
            foreach (var t in compList)
            {
                await Supa.UpsertAsync("technology", new
                {
                    company_id = compId,
                    category = t.Category ?? "",
                    name = t.Name,
                    first_seen = Helpers.ParseIso(t.FirstSeen),
                    last_seen = Helpers.ParseIso(t.LastSeen),
                    source_api = "BuiltWith",
                    captured_at = DateTime.UtcNow
                }, "company_id,name,category");
            }
        }

        // Raw JSON’u Storage’a yaz
        var bucket = Environment.GetEnvironmentVariable("SUPABASE_BUCKET") ?? "ingest-raw";
        var rawKey = $"runs/{resp.CompanyId}/{resp.JobId}.json";
        await Supa.UploadJsonAsync(bucket, rawKey, resp);
        resp.StorageKey = $"{bucket}/{rawKey}";
    }
    catch (Exception ex)
    {
        resp.Errors.Add($"persist error: {ex.Message}");
    }

    return Results.Ok(resp);
});

// --- READ ENDPOINTS ---

app.MapGet("/company/{domain}", async (string domain) =>
{
    var companyId = await Supa.GetCompanyIdByDomainAsync(domain);
    if (companyId is null)
        return Results.NotFound(new { ok = false, message = "company not found" });
    int? newsCount = null, techCount = null, breachCount = null;   // << eklendi
    string? sslGrade = null;

    int? jobsTotal = null, githubRepos = null, githubStars = null, hnMentions = null, edgarFilings = null;
    int? privacyScore = null; bool? privacyHasPolicy = null, privacyDpoEmail = null;
    string? revenueMethod = null; double? revenueValue = null;
    double? revenueEstimate = null;

    if (companyId is null)
        return Results.NotFound(new { ok = false, message = "company not found" });

    // son datapoint değerlerini çek
    var dp = await Supa.SelectArrayAsync("datapoint",
        $"company_id=eq.{companyId}&order=captured_at.desc&limit=200&select=label,path,value_json,captured_at,source_api");

    foreach (var row in dp)
    {
        var label = row.GetProperty("label").GetString();
        if (!row.TryGetProperty("value_json", out var vj)) continue;

        // helper: int/double/string/bool güvenli getter’lar
        static int? GetInt(JsonElement e, string name)
            => e.TryGetProperty(name, out var x) && x.ValueKind == JsonValueKind.Number ? x.GetInt32() : (int?)null;

        static double? GetDouble(JsonElement e, string name)
            => e.TryGetProperty(name, out var x) && x.ValueKind == JsonValueKind.Number ? x.GetDouble() : (double?)null;

        static string? GetStr(JsonElement e, string name)
            => e.TryGetProperty(name, out var x) && x.ValueKind == JsonValueKind.String ? x.GetString() : null;

        static bool? GetBool(JsonElement e, string name)
            => e.TryGetProperty(name, out var x) && (x.ValueKind == JsonValueKind.True || x.ValueKind == JsonValueKind.False) ? x.GetBoolean() : (bool?)null;



        // mevcut 4 metrik
        if (label == "news_count_90d"   && newsCount    is null) newsCount    = GetInt(vj, "total");
        if (label == "tech_count_total" && techCount    is null) techCount    = GetInt(vj, "count");
        if (label == "ssl_grade"        && sslGrade     is null) sslGrade     = GetStr(vj, "grade");
        if (label == "breach_count_total" && breachCount is null) breachCount = GetInt(vj, "total");

        // yeni metrikler
        if (label == "jobs_total"          && jobsTotal        is null) jobsTotal        = GetInt(vj, "total");

        if (label == "github_repo_count"   && githubRepos      is null) githubRepos      = GetInt(vj, "count");
        if (label == "github_total_stars"  && githubStars      is null) githubStars      = GetInt(vj, "total");

        if (label == "privacy_maturity")
        {
            // en yeni olan ilk sırada geldiği için null olanları doldur
            privacyScore     ??= GetInt (vj, "score");
            privacyHasPolicy ??= GetBool(vj, "has_policy");
            privacyDpoEmail  ??= GetBool(vj, "dpo_email");
        }

        if (label == "hn_mentions_total"   && hnMentions      is null) hnMentions      = GetInt(vj, "total");

        if (label == "edgar_filings_total" && edgarFilings    is null) edgarFilings    = GetInt(vj, "total");

        if (label == "revenue_estimate")
        {
            revenueMethod   ??= GetStr   (vj, "method");
            // value double olarak saklandı (EstimateUsd), adı "value"
            revenueEstimate ??= GetDouble(vj, "value");
        }

    }

    return Results.Ok(new {
        ok = true,
        domain,
        companyId,
        summary = new {
            newsCount,
            techCount,
            sslGrade,
            breachCount,
            jobsTotal,
            githubRepos,
            githubStars,
            privacyScore,
            privacyHasPolicy,
            privacyDpoEmail,
            hnMentions,
            edgarFilings,
            revenueMethod,
            revenueEstimate
        }
    });
});

app.MapGet("/company/{domain}/tech", async (string domain) =>
{
    var companyId = await Supa.GetCompanyIdByDomainAsync(domain);
    if (companyId is null)
        return Results.NotFound(new { ok = false, message = "company not found" });

    // kategori & isim sırasıyla getir
    var rows = await Supa.SelectArrayAsync("technology",
        $"company_id=eq.{companyId}&order=category.asc,name.asc&select=category,name,first_seen,last_seen,source_api,captured_at");

    // JSON Element -> POCO projeksiyon
    var items = rows.Select(r => new {
        category  = r.TryGetProperty("category", out var c) ? c.GetString() : null,
        name      = r.GetProperty("name").GetString(),
        firstSeen = r.TryGetProperty("first_seen", out var fs) && fs.ValueKind != JsonValueKind.Null ? fs.GetString() : null,
        lastSeen  = r.TryGetProperty("last_seen", out var ls) && ls.ValueKind != JsonValueKind.Null ? ls.GetString() : null,
        sourceApi = r.GetProperty("source_api").GetString(),
        capturedAt= r.GetProperty("captured_at").GetString()
    });

    return Results.Ok(new { ok = true, domain, count = items.Count(), items });
});

app.MapGet("/company/{domain}/news", async (string domain, int limit = 20) =>
{
    var companyId = await Supa.GetCompanyIdByDomainAsync(domain);
    if (companyId is null)
        return Results.NotFound(new { ok = false, message = "company not found" });

    var rows = await Supa.SelectArrayAsync("news_article",
        $"company_id=eq.{companyId}&order=captured_at.desc" +
        $"&limit={Math.Clamp(limit,1,100)}" +
        $"&select=title,url,source,published_at,captured_at");

    var items = rows.Select(r => new {
        title       = r.GetProperty("title").GetString(),
        url         = r.TryGetProperty("url", out var u) && u.ValueKind != JsonValueKind.Null ? u.GetString() : null,
        source      = r.TryGetProperty("source", out var s) && s.ValueKind != JsonValueKind.Null ? s.GetString() : null,
        publishedAt = r.TryGetProperty("published_at", out var p) && p.ValueKind != JsonValueKind.Null ? p.GetString() : null,
        capturedAt  = r.GetProperty("captured_at").GetString()
    });

    return Results.Ok(new { ok = true, domain, count = items.Count(), items });
});


app.Run();

record Competitor(string Name, string? Domain);

record PreviewReq(string CompanyName, string? Domain, Competitor[]? Competitors);
record ConfirmReq(string CompanyName, string? Domain, Competitor[]? Competitors);

record NewsCountDto(int total, string[] sample_titles);
record SslDto(string? grade);
record TechDto(int count);
record TechItem(
    [property: JsonPropertyName("category")]  string? Category,
    [property: JsonPropertyName("name")]      string  Name,
    [property: JsonPropertyName("first_seen")]string? FirstSeen,
    [property: JsonPropertyName("last_seen")] string? LastSeen
);record TechBreakdownDto(
    [property: JsonPropertyName("items")] List<TechItem> Items
);
record NewsArticleDto(
    [property: JsonPropertyName("title")]       string Title,
    [property: JsonPropertyName("url")]         string? Url,
    [property: JsonPropertyName("published_at")]string? PublishedAt,
    [property: JsonPropertyName("source")]      string? Source
);

record NewsSearchDto(
    [property: JsonPropertyName("total")]    int Total,
    [property: JsonPropertyName("articles")] List<NewsArticleDto> Articles
);

record BreachDto(int total);

record WriteNewsReq(
    string CompanyName,
    string? Domain,
    List<NewsArticleDto> Articles
);

record JobsSummaryDto(
    [property: JsonPropertyName("total")] int Total,
    [property: JsonPropertyName("by_function")] Dictionary<string,int> ByFunction
);

record GithubStatsDto(
    [property: JsonPropertyName("org")] string? Org,
    [property: JsonPropertyName("repos")] int Repos,
    [property: JsonPropertyName("stars")] int Stars,
    [property: JsonPropertyName("forks")] int Forks,
    [property: JsonPropertyName("open_issues")] int OpenIssues,
    [property: JsonPropertyName("commits_30d")] int Commits30d
);

record PrivacyDto(
    [property: JsonPropertyName("privacy_policy_url")] string? PrivacyPolicyUrl,
    [property: JsonPropertyName("security_txt_url")] string? SecurityTxtUrl,
    [property: JsonPropertyName("dpo_email_found")] bool DpoEmailFound
);

record HnMentionsDto(
    [property: JsonPropertyName("last_30d")] int Last30d
);

record EdgarSummaryDto(
    [property: JsonPropertyName("filings_90d")] int Filings90d,
    [property: JsonPropertyName("cik")] string? Cik
);

record RevenueEstimateDto(
    [property: JsonPropertyName("method")] string Method,
    [property: JsonPropertyName("estimate_usd")] double? EstimateUsd
);


class CompanySnapshot
{
    public string CompanyName { get; set; } = default!;
    public string? Domain { get; set; }
    public int NewsCount { get; set; }
    public string[] SampleTitles { get; set; } = Array.Empty<string>();
    public string? SslGrade { get; set; }
    public int? TechCount { get; set; }
    public List<string> Errors { get; set; } = new();
}

class ConfirmResp
{
    public string JobId { get; set; } = default!;
    public Guid? CompanyId { get; set; }
    public string CompanyName { get; set; } = default!;
    public string? Domain { get; set; }
    public int NewsCount { get; set; }
    public string[]? SampleTitles { get; set; }
    public string? SslGrade { get; set; }
    public int? TechCount { get; set; }
    public string? StorageKey { get; set; }
    public List<CompanySnapshot> Competitors { get; set; } = new();
    public List<string> Errors { get; set; } = new();
    public int MainTechsCount { get; set; }
    public int[] CompTechsCounts { get; set; } = Array.Empty<int>();
}

class PreviewResp
{
    public string JobId { get; set; } = default!;
    public string CompanyName { get; set; } = default!;
    public string? Domain { get; set; }
    public int NewsCount { get; set; }
    public string[] SampleTitles { get; set; } = Array.Empty<string>();
    public string? SslGrade { get; set; }
    public int? TechCount { get; set; }
    public List<CompanySnapshot> Competitors { get; set; } = new();
    public List<string> Errors { get; set; } = new();
}

static class SnapshotHelpers
{
    public static async Task<CompanySnapshot> GetSnapshotAsync(
        bool useHttp, string companyName, string? domain,
        HttpClient? sharedHttp = null, IntelWorker.IntelWorkerClient? sharedClient = null)
    {
        var snap = new CompanySnapshot { CompanyName = companyName, Domain = domain };

        try
        {
            if (useHttp)
            {
                var httpUrl = Environment.GetEnvironmentVariable("PY_WORKER_HTTP_URL") ?? "http://python-worker:8081";
                var http = sharedHttp ?? new HttpClient();

                var newsTask = http.GetFromJsonAsync<NewsCountDto>(
                    $"{httpUrl}/news-count?company_name={Uri.EscapeDataString(companyName)}&domain={Uri.EscapeDataString(domain ?? "")}");
                var sslTask  = http.GetFromJsonAsync<SslDto>(
                    $"{httpUrl}/ssl-grade?domain={Uri.EscapeDataString(domain ?? "")}");
                var techTask = http.GetFromJsonAsync<TechDto>(
                    $"{httpUrl}/tech-count?domain={Uri.EscapeDataString(domain ?? "")}");

                await Task.WhenAll(newsTask!, sslTask!, techTask!);

                var news = newsTask!.Result;
                var ssl  = sslTask!.Result;
                var tech = techTask!.Result;

                snap.NewsCount    = news?.total ?? 0;
                snap.SampleTitles = news?.sample_titles ?? Array.Empty<string>();
                snap.SslGrade     = ssl?.grade;
                snap.TechCount    = tech?.count;
            }
            else
            {
                var client = sharedClient!;
                var newsTask = client.GetNewsCountAsync(new GetNewsRequest { CompanyName = companyName ?? "", Domain = domain ?? "" }).ResponseAsync;
                var sslTask  = client.GetSslGradeAsync (new SslRequest { Domain = domain ?? "" }).ResponseAsync;
                var techTask = client.GetTechCountAsync(new TechRequest { Domain = domain ?? "" }).ResponseAsync;

                await Task.WhenAll(newsTask, sslTask, techTask);

                var news = await newsTask; var ssl = await sslTask; var tech = await techTask;
                snap.NewsCount    = news.Total;
                snap.SampleTitles = news.SampleTitles.ToArray();
                snap.SslGrade     = ssl.Grade;
                snap.TechCount    = tech.Count;
            }
        }
        catch (Exception ex)
        {
            snap.Errors.Add(ex.Message);
        }

        return snap;
    }
}


static class Supa
{
    static readonly HttpClient _http = new HttpClient();
    static string? Url => Environment.GetEnvironmentVariable("SUPABASE_URL");
    static string? Key => Environment.GetEnvironmentVariable("SUPABASE_SERVICE_ROLE_KEY");

    static void Ensure()
    {
        if (string.IsNullOrWhiteSpace(Url) || string.IsNullOrWhiteSpace(Key))
            throw new InvalidOperationException("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is not set.");
    }

    public static async Task InsertAsync(string table, object row)
    {
        Ensure();
        var uri = $"{Url!.TrimEnd('/')}/rest/v1/{table}";
        var json = JsonSerializer.Serialize(row, new JsonSerializerOptions { PropertyNamingPolicy = null });
        using var req = new HttpRequestMessage(HttpMethod.Post, uri);
        req.Headers.TryAddWithoutValidation("apikey", Key);
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", Key);
        req.Headers.TryAddWithoutValidation("Prefer", "return=representation");
        req.Content = new StringContent(json, Encoding.UTF8, "application/json");

        using var res = await _http.SendAsync(req);
        if (!res.IsSuccessStatusCode)
        {
            var body = await res.Content.ReadAsStringAsync();
            throw new Exception($"PostgREST insert {table} failed: {(int)res.StatusCode} {res.ReasonPhrase} - {body}");
        }
    }

    public static async Task UploadJsonAsync(string bucket, string path, object payload)
    {
        Ensure();
        var uri = $"{Url!.TrimEnd('/')}/storage/v1/object/{bucket}/{path}";
        var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        using var req = new HttpRequestMessage(HttpMethod.Post, uri);
        req.Headers.TryAddWithoutValidation("apikey", Key);
        req.Headers.TryAddWithoutValidation("x-upsert", "true");
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", Key);
        req.Content = new StringContent(json, Encoding.UTF8, "application/json");

        using var res = await _http.SendAsync(req);
        if (!res.IsSuccessStatusCode)
        {
            var body = await res.Content.ReadAsStringAsync();
            throw new Exception($"Storage upload failed: {(int)res.StatusCode} {res.ReasonPhrase} - {body}");
        }
    }

    public static async Task<Guid?> GetCompanyIdByDomainAsync(string? domain)
    {
        Ensure();
        if (string.IsNullOrWhiteSpace(domain)) return null;

        var uri = $"{Url!.TrimEnd('/')}/rest/v1/company?domain=eq.{Uri.EscapeDataString(domain)}&select=id&limit=1";
        using var req = new HttpRequestMessage(HttpMethod.Get, uri);
        req.Headers.TryAddWithoutValidation("apikey", Key);
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", Key);

        using var res = await _http.SendAsync(req);
        if (!res.IsSuccessStatusCode) return null;

        var json = await res.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        if (root.ValueKind == JsonValueKind.Array && root.GetArrayLength() > 0)
        {
            var idStr = root[0].GetProperty("id").GetString();
            if (Guid.TryParse(idStr, out var gid)) return gid;
        }
        return null;
    }

    public static async Task<Guid> GetOrCreateCompanyByDomainAsync(string name, string? domain, Guid? competitorOf = null)
    {
        Ensure();
        // 1) Varsa mevcut ID'yi dön
        var existing = await GetCompanyIdByDomainAsync(domain);
        if (existing.HasValue) return existing.Value;

        // 2) Yoksa yeni kaydet
        var newId = Guid.NewGuid();
        await InsertAsync("company", new
        {
            id = newId,
            name = name,
            domain = domain,
            competitor_of = competitorOf
        });
        return newId;
    }

    public static async Task UpsertAsync(string table, object row, string onConflictColumnsCsv)
    {
        Ensure();
        var uri = $"{Url!.TrimEnd('/')}/rest/v1/{table}?on_conflict={Uri.EscapeDataString(onConflictColumnsCsv)}";
        var json = JsonSerializer.Serialize(row, new JsonSerializerOptions { PropertyNamingPolicy = null });

        using var req = new HttpRequestMessage(HttpMethod.Post, uri);
        req.Headers.TryAddWithoutValidation("apikey", Key);
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", Key);
        req.Headers.TryAddWithoutValidation("Prefer", "resolution=merge-duplicates,return=representation");
        req.Content = new StringContent(json, Encoding.UTF8, "application/json");

        using var res = await _http.SendAsync(req);
        if (!res.IsSuccessStatusCode)
        {
            var body = await res.Content.ReadAsStringAsync();
            throw new Exception($"PostgREST upsert {table} failed: {(int)res.StatusCode} {res.ReasonPhrase} - {body}");
        }
    }

    public static async Task<JsonElement[]> SelectArrayAsync(string table, string query)
    {
        Ensure();
        var uri = $"{Url!.TrimEnd('/')}/rest/v1/{table}?{query}";
        using var req = new HttpRequestMessage(HttpMethod.Get, uri);
        req.Headers.TryAddWithoutValidation("apikey", Key);
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", Key);
        // RLS bypass için service role key zaten Authorization'da

        using var res = await _http.SendAsync(req);
        var body = await res.Content.ReadAsStringAsync();

        if (!res.IsSuccessStatusCode)
            throw new Exception($"PostgREST select {table} failed: {(int)res.StatusCode} {res.ReasonPhrase} - {body}");

        using var doc = JsonDocument.Parse(body);
        if (doc.RootElement.ValueKind != JsonValueKind.Array) return Array.Empty<JsonElement>();
        // JsonElement’i kopyalamak için yeni bir diziye yazalım
        return doc.RootElement.EnumerateArray().Select(e => e.Clone()).ToArray();
    }


}

static class Helpers
{
    public static DateTime? ParseIso(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return null;
        return DateTime.TryParse(s, out var dt) ? dt : (DateTime?)null;
    }
}