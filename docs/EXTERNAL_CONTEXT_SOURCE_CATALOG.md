# External Context Source Catalog

This catalog records the external data sources Core should consider for slow-moving macro, news, research, market, and risk context. It is project-specific to `kai-trading-core` and is meant to prevent each provider task from repeating the same source research.

[FinceptTerminal](https://github.com/Fincept-Corporation/FinceptTerminal) is an inspiration/reference for the idea of maintaining a source catalog only. Do not copy Fincept code, documentation text, UI, class names, or branding into Core. Provider implementations must use the underlying providers' public documentation, credentials, and terms directly.

## Scope and Signal Policy

This document does not authorize live trading, order execution, position sizing, or a new buy/sell signal. New external sources default to `context_only`: they may enrich decider prompts, dashboard status, weekly analysis, or operator diagnostics, but they must not directly trigger trades or block/allow trades unless a later issue explicitly approves that use.

Signal status vocabulary:

| Status | Meaning |
| --- | --- |
| `context_only` | Source can provide provenance-rich context only. It cannot directly trigger buy/sell/size decisions or hard gates. |
| `existing_internal_gate_input` | Source is already used by Core's deterministic market/factor/gate paths. This catalog does not expand that authority. |
| `future_signal_or_gate_requires_approval` | Source might become eligible later only after a dedicated issue, tests, validation, docs, and product approval change this status. |

Access vocabulary:

| Access | Meaning |
| --- | --- |
| `no-key` | Public endpoint can be called without an API key, subject to provider terms and rate limits. |
| `key-required` | Core must have an environment-backed credential or app key. Missing keys must produce explicit disabled status. |
| `manual` | Human review/download or non-automated approval is required before use. |
| `unstable` | Endpoint, mirror, scraping path, or public content availability is not dependable enough for automated pipelines. |

## Implementation Priority

| Priority | Work item | Status | Target / reference |
| --- | --- | --- | --- |
| P0 | External context provider registry with TTL, minimum refresh interval, source status, and last-good cache semantics. | Implemented foundation. | [#449](https://github.com/kiseyno-AI/kai-trading-core/issues/449), [PR #456](https://github.com/kiseyno-AI/kai-trading-core/pull/456) |
| P0 | Macro context MVP using no-key World Bank indicators and optional keyed FRED. | Implemented MVP; expand only after the shape stays stable. | [#448](https://github.com/kiseyno-AI/kai-trading-core/issues/448) |
| P0 | External news material/noise/source-status contract. | Implemented contract; keep news contextual and source-backed. | [#447](https://github.com/kiseyno-AI/kai-trading-core/issues/447) |
| P0 | Durable Core-owned source catalog. | This document. | [#450](https://github.com/kiseyno-AI/kai-trading-core/issues/450) |
| P1 | Wrap existing macro/FX adapters in the external context registry where doing so improves common freshness/status semantics. | Candidate after P0 docs settle. | BOK ECOS, Korea Exim, FRED follow-up issue TBD |
| P1 | Add DBnomics as the first no-key macro aggregator follow-up if licensing and rate behavior are acceptable. | Candidate. | Macro provider follow-up issue TBD |
| P2 | Add direct OECD/IMF providers only for series that DBnomics or World Bank cannot cover cleanly. | Candidate. | Macro provider follow-up issue TBD |
| P2 | Normalize DART/OpenDART disclosure/fundamental source status through the same catalog language. | Candidate; existing DART routes remain authoritative. | DART follow-up issue TBD |
| P3 | Evaluate public research report mirrors such as Daishin GitHub mirrors. | Blocked until public content access, provenance, and redistribution limits are proven. | Research provider issue TBD |

## Source Catalog

| Source | Domain | Access | Update frequency / suggested TTL | Intended Core use | Direct trading-signal status | First target / issue | Source URL | Legal / stability notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| World Bank Indicators API | macro | `no-key` | Series are usually annual, quarterly, or monthly; cache for 7 days for annual/monthly context, 24 hours for metadata/status. | Decider macro backdrop, daily pipeline source status, weekly analysis. | `context_only` | MVP implemented under [#448](https://github.com/kiseyno-AI/kai-trading-core/issues/448). | [World Bank developer information](https://datahelpdesk.worldbank.org/knowledgebase/topics/125589-developer-information) | Good default no-key macro source. Store compact observations only; do not pass raw long series to deciders. |
| DBnomics Web API | macro | `no-key` | Follows upstream providers; cache selected monthly/quarterly series for 24 hours to 7 days. | Follow-up no-key macro aggregator for decider context and weekly analysis. | `context_only` | P1 macro provider follow-up; should use registry shape from [#449](https://github.com/kiseyno-AI/kai-trading-core/issues/449). | [DBnomics Web API docs](https://docs.db.nomics.world/web-api/) | Aggregates many providers; verify each dataset's upstream license and provider code before using it. |
| OECD SDMX API | macro | `no-key` | Depends on dataset; daily/monthly/quarterly/annual. Cache monthly/quarterly series for 7 days; metadata for 24 hours. | Macro backdrop and weekly analysis where OECD has cleaner series than World Bank/DBnomics. | `context_only` | P2 after DBnomics unless a needed OECD-only series is identified. | [OECD API overview](https://www.oecd.org/en/data/insights/data-explainers/2024/09/api.html) | OECD API is free subject to OECD terms and rate limits; avoid broad downloads. |
| IMF Data APIs | macro, risk | `no-key` | Mostly monthly/quarterly/annual; cache selected series for 7 days, metadata for 24 hours. | Global risk/macro context and weekly analysis. | `context_only` | P2 after DBnomics/OECD gap analysis. | [IMF Data APIs](https://data.imf.org/en/Resource-Pages/IMF-API) | Prefer targeted series. Treat SDMX schema churn and dataset availability as provider-stability risks. |
| FRED API | macro | `key-required` | Daily to annual by series; cache daily series for 24 hours, slower series for 7 days. | Optional macro context such as USD/KRW proxy; disabled explicitly when `FRED_API_KEY` is missing. | `context_only` | Optional source in [#448](https://github.com/kiseyno-AI/kai-trading-core/issues/448). | [FRED API docs](https://fred.stlouisfed.org/docs/api/fred/) | Key-gated. Missing key must be surfaced as disabled, not as a pipeline failure. |
| BOK ECOS | macro | `key-required` | Depends on statistic and cycle; cache daily series for 24 hours, monthly/quarterly series for 7 days. | Korea-specific macro context, operator diagnostics, and dashboard-only series reads. | `context_only` | Existing `/macro/bok/series`; future registry wrapping TBD. | [BOK ECOS Open API](https://ecos.bok.or.kr/api/) | Requires `BOK_ECOS_API_KEY`. Keep Korea-specific series IDs documented with source status. |
| Korea Exim exchange rates | macro, risk | `key-required` | Business-day FX rates; cache for 24 hours with last-good fallback for weekends/holidays/provider errors. | USD/KRW market snapshot context, risk backdrop, daily pipeline status. | `context_only` | Existing `/fx/koreaexim/rates`; future registry wrapping TBD. | [Korea Exim exchange-rate OpenAPI](https://www.data.go.kr/en/data/3068846/openapi.do) | Requires `KOREAEXIM_API_KEY`. Use the current `oapi.koreaexim.go.kr` endpoint and preserve last-good fallback semantics. |
| Naver News Search API | news | `key-required` | Near-real-time search results; cache collection windows per symbol for 1 to 6 hours, with per-run throttling. | Decider context, bounded material-news summaries, daily pipeline status. | `context_only` | News material/noise contract in [#447](https://github.com/kiseyno-AI/kai-trading-core/issues/447). | [Naver News Search API docs](https://developers.naver.com/docs/serviceapi/search/news/news.md) | Headline/snippet search results are not full article reads. Preserve `headline_only`/paywall/fetch-failed access status and never forecast price from news alone. |
| DART / OpenDART | research, market | `key-required` | Disclosures are event-driven; company metadata can cache for 24 hours, filing/fundamental captures should refresh daily or on event sync. | Disclosure/fundamental context, agent context, daily pipeline status, dashboard inspection. | `context_only` | Existing DART routes; future source-status normalization TBD. | [OpenDART developer guide](https://engopendart.fss.or.kr/guide/main.do?apiGrpCd=DE001) | Requires `DART_API_KEY`. Financial disclosures can inform context and later factors only through separately approved deterministic services. |
| Daishin research report mirror | research | `manual`, `unstable` | No automated TTL until a stable public mirror and official source provenance are proven. | Manual weekly analysis only; dashboard links only after legal/stability review. | `context_only` | P3 research provider issue TBD. | [Daishin official sector research page](https://eng.daishin.com/E5/ResearchCenter/Work/DW_EnglishList.aspx?b_code=272) | Treat GitHub mirrors as unapproved until content access, redistribution rights, original-source URLs, and update reliability are verified. Do not ingest private, paywalled, or redistribution-prohibited reports. |
| KIS Open API | market, risk | `key-required` | Intraday/realtime quotes are seconds-level; daily ranking/risk snapshots are market-day data. Use 5 to 60 seconds for live read models and 1 market day for daily snapshots. | Existing internal market data, quote context, ranking signal features, market-risk snapshots, execution runtime diagnostics. | `existing_internal_gate_input` | Existing KIS modules/routes; no new provider issue from this catalog. | [KIS Developers portal](https://apiportal.koreainvestment.com/), [KIS Open API GitHub](https://github.com/koreainvestment/open-trading-api) | Requires broker credentials. This catalog does not approve new order placement or expanded live trading behavior. |
| pykrx | market, risk | `no-key`, `unstable` | KRX/Naver-backed daily market data; refresh once per market date after close, cache historical rows permanently once persisted. | Existing EOD bars, enrichment, factor/risk context, decider preflight gates, daily pipeline status. | `existing_internal_gate_input` | Existing EOD/pykrx modules/routes; no new provider issue from this catalog. | [pykrx GitHub](https://github.com/sharebook-kr/pykrx) | Scraping-based dependency over KRX/Naver sources. Respect upstream terms, throttle calls, and surface partial/missing coverage instead of fabricating data. |

## Provider Implementation Notes

- Each new automated source should declare provider name, supported key/topic, source URL, upstream endpoint, TTL seconds, minimum refresh interval, and source status in the `app.external_context` registry where that registry fits the provider shape.
- Missing keys, provider errors, empty responses, rate limits, and stale cached values must be explicit source statuses. They must not silently erase the context block.
- Decider-facing context should be compact and provenance-rich. Raw long time series, raw article dumps, or raw research PDFs should not be sent directly to deciders.
- Any change from `context_only` to gate/signal eligibility requires a new issue, tests, historical validation, documentation, and explicit approval.
- Source terms must be checked at implementation time. This catalog is not a license grant.
