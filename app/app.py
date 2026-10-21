"""AirSafe Streamlit app: reads BigQuery tables built by the pipeline.

Env vars:
  GCP_PROJECT     (default airsafe-ia3-48213)
  GEMINI_API_KEY  optional; without it the app shows the guideline text directly
  GEMINI_MODEL    optional; default gemini-2.5-flash
"""
import os
from datetime import datetime, timedelta, timezone

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery

PROJECT = os.environ.get("GCP_PROJECT", "airsafe-ia3-48213")
T = f"{PROJECT}.airsafe"
IST = timezone(timedelta(hours=5, minutes=30))
AUDIENCES = {
    "General public": "general",
    "Children and older adults": "sensitive",
    "People with asthma or heart disease": "sensitive",
    "Outdoor workers": "general",
}

st.set_page_config(page_title="AirSafe", layout="wide")


@st.cache_resource
def client():
    return bigquery.Client(project=PROJECT)


@st.cache_data(ttl=300)
def load(sql: str) -> pd.DataFrame:
    cfg = bigquery.QueryJobConfig(maximum_bytes_billed=100_000_000)
    df = client().query(sql, job_config=cfg, location="US").to_dataframe()
    for c in df.columns:  # timestamps are IST wall-clock; drop the UTC label
        if isinstance(df[c].dtype, pd.DatetimeTZDtype):
            df[c] = df[c].dt.tz_localize(None)
    return df


@st.cache_data(ttl=3600, show_spinner=False)
def gemini_advisory(audience: str, category: str, peak: float, peak_time: str,
                    window: str, impact: str, advice: str) -> str | None:
    """Gemini may only rephrase the facts below (grounded in the guideline table)."""
    key = os.environ.get("GEMINI_API_KEY")
    if not key:
        return None
    prompt = (
        "You write short air-quality advisories. Use ONLY the facts below. "
        "Do not add medical claims, numbers or advice that are not listed. "
        "Maximum 70 words, plain friendly language, no markdown.\n\n"
        f"Audience: {audience}\nRisk category: {category}\n"
        f"Peak forecast PM2.5: {peak:.0f} ug/m3 around {peak_time}\n"
        f"Cleanest outdoor window: {window}\n"
        f"Health impact: {impact}\nGuideline advice: {advice}\n"
    )
    try:
        from google import genai

        resp = genai.Client(api_key=key).models.generate_content(
            model=os.environ.get("GEMINI_MODEL", "gemini-2.5-flash"), contents=prompt
        )
        return resp.text
    except Exception:
        return None


st.title("AirSafe: PM2.5 forecast and health-risk early warning")
st.caption("Manali Village, Chennai (TNPCB sensor via OpenAQ). Informational only; "
           "not a replacement for official CPCB alerts or medical advice.")

fc = load(f"SELECT * FROM `{T}.forecast_latest` ORDER BY ts")
best = load(f"SELECT * FROM `{T}.best_windows` ORDER BY clean_rank LIMIT 3")
health = load(f"SELECT * FROM `{T}.v_pipeline_health`")
guide = load(f"SELECT category, health_impact FROM `{T}.advisory_guidelines`")
latest = load(f"SELECT ts, pm25, category FROM `{T}.v_history` ORDER BY ts DESC LIMIT 1")

last_obs = health["last_observation"].iloc[0]
now_ist = datetime.now(IST).replace(tzinfo=None)
lag_h = (now_ist - last_obs.to_pydatetime()).total_seconds() / 3600

tab0, tab1, tab2, tab3, tab4 = st.tabs(
    ["About the project", "Forecast and advice", "Model results",
     "Spikes and accuracy", "Pipeline health"]
)

# ------------------------------------------------------------------ tab 0
with tab0:
    st.header("Why AirSafe?")
    st.markdown(
        """
**The problem.** PM2.5 (tiny particles that reach deep into the lungs) is the main air pollutant
in Indian cities. Most apps only show the *current* AQI, so people cannot plan ahead, and a
bare number like "AQI 180" does not say what to do. Children, older adults, people with asthma or
heart disease and outdoor workers are affected most.

**Our solution.** AirSafe is a cloud analytics system that collects live air-quality and weather
data, **forecasts PM2.5 for the next 48 hours**, converts each hour into a **health-risk category**
and tells each type of user **what to do and when the cleanest hours are**.

**How air quality is checked.** A government monitoring station (Manali Village, Chennai, TNPCB)
measures PM2.5 in micrograms per cubic metre (ug/m3). The data comes through the OpenAQ API.
We compare values with the CPCB AQI bands: Good (0-30), Satisfactory (31-60), Moderate (61-90),
Poor (91-120), Very Poor (121-250) and Severe (250+).

**What you get (output).** The live forecast with a 90% interval, an early-warning banner,
the cleanest 3-hour windows, audience-specific advice (with an optional Gemini rewrite), model
accuracy results and a data-health report.

**Who benefits.** Citizens planning exercise, commutes or school activities, outdoor workers,
patients with respiratory or heart conditions, and local bodies that need early warning.
"""
    )
    st.subheader("Innovative features")
    st.markdown(
        """
1. **Predictive monitoring:** forecasts future PM2.5, not only current readings.
2. **Weather-aware forecasting:** tests whether weather features improve accuracy (they did not, and we report that honestly).
3. **Health-risk early warnings:** audience-specific, guideline-based advice.
4. **Cleanest-time-window recommendation:** ranks forecast hours for outdoor plans.
5. **Automated cloud analytics:** scheduled ingestion, ETL, managed ML, dashboard, Docker and CI/CD.
6. **Explainable results:** prediction intervals, errors, model comparison and data freshness.
7. **Forecast accuracy tracker:** stores each forecast and compares it with what actually happened.
8. **Spike-alert log:** records forecast pollution episodes as events.
9. **Grounded Gemini advisory:** Gemini may only rephrase a fixed guideline table.
10. **Pipeline health panel:** freshness, missing-hour share and last successful run.
11. **Multi-station comparison:** forecast accuracy across several stations.
"""
    )
    st.subheader("Architecture")
    st.code(
        "OpenAQ + Open-Meteo APIs\n"
        "   -> Cloud Scheduler (hourly) -> Cloud Function (fetch, validate, clean)\n"
        "   -> Cloud Storage (raw JSON) + BigQuery (history table)\n"
        "   -> BigQuery ML (ARIMA_PLUS forecast, retrained when new data arrives)\n"
        "   -> Looker Studio dashboard + Streamlit app (Docker, Cloud Run)\n"
        "   -> GitHub Actions (tests and deployment)",
        language="text",
    )
    st.caption("Limitations: one station, about one year of data, and the public feed can lag by hours "
               "or days. This is an informational early-warning tool, not an official alert or medical advice.")

# ------------------------------------------------------------------ tab 1
with tab1:
    audience = st.sidebar.radio("Advice for", list(AUDIENCES))
    st.sidebar.write("Risk bands follow CPCB PM2.5 AQI categories (applied to hourly values).")

    st.write(f"Data as of **{last_obs:%d %b %Y, %H:%M} IST** "
             f"(latest reading is about {lag_h:.0f} h old). Forecast covers the next 48 hours from there.")
    if lag_h > 24:
        st.warning("The public data feed is more than 24 hours behind; treat the forecast with care.")

    nxt = fc.head(24)
    peak = nxt.loc[nxt["pm25_pred"].idxmax()]
    bad = nxt[nxt["level"] >= 4]
    if not bad.empty:
        st.error(f"Early warning: PM2.5 is forecast to reach **{peak['category']}** "
                 f"(about {peak['pm25_pred']:.0f} ug/m3) around {peak['ts']:%a %H:%M}.")
    elif peak["level"] >= 3:
        st.warning(f"Moderate pollution expected, peaking near {peak['pm25_pred']:.0f} ug/m3 "
                   f"around {peak['ts']:%a %H:%M}.")
    else:
        st.success(f"No high-pollution episode forecast in the next 24 hours "
                   f"(peak {peak['pm25_pred']:.0f} ug/m3, {peak['category']}).")

    lr = latest.iloc[0]
    cw = (f"{best.iloc[0]['window_start']:%a %H:%M} to {best.iloc[0]['window_end']:%H:%M}"
          if not best.empty else "n/a")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Latest reading", f"{lr['pm25']:.0f} ug/m3", lr["category"], delta_color="off")
    m2.metric("Peak, next 24 h", f"{peak['pm25_pred']:.0f} ug/m3", peak["category"], delta_color="off")
    m3.metric("Cleanest window", cw)
    m4.metric("Data age", f"{lag_h:.0f} h")

    base = alt.Chart(fc).encode(x=alt.X("ts:T", title="Hour (IST)"))
    band = base.mark_area(opacity=0.2).encode(
        y=alt.Y("pm25_lo:Q", title="PM2.5 (ug/m3)"), y2="pm25_hi:Q")
    line = base.mark_line().encode(y="pm25_pred:Q", tooltip=["ts:T", "pm25_pred:Q", "category:N"])
    st.altair_chart(band + line, use_container_width=True)
    st.caption("Line = forecast, shaded band = 90% prediction interval.")

    left, right = st.columns(2)
    with left:
        st.subheader("Cleanest 3-hour windows (next 24 h)")
        window_txt = "no suitable daytime window"
        if not best.empty:
            b = best.iloc[0]
            window_txt = f"{b['window_start']:%a %H:%M} to {b['window_end']:%H:%M} (avg {b['avg_pm25']} ug/m3)"
        st.dataframe(best[["window_start", "window_end", "avg_pm25"]], hide_index=True,
                     use_container_width=True)
    with right:
        st.subheader(f"Advice: {audience}")
        advice = peak["sensitive_advice"] if AUDIENCES[audience] == "sensitive" else peak["general_advice"]
        impact = guide.loc[guide["category"] == peak["category"], "health_impact"].iloc[0]
        st.markdown(f"**Peak category: {peak['category']}**\n\n{impact}\n\n{advice}")
        if st.button("Write a plain-language advisory with Gemini"):
            text = gemini_advisory(audience, peak["category"], float(peak["pm25_pred"]),
                                   f"{peak['ts']:%a %H:%M}", window_txt, impact, advice)
            if text:
                st.info(text)
                st.caption("Generated by Gemini using only the guideline table above.")
            else:
                st.info("Gemini is unavailable, so the guideline text above is shown.")

    st.subheader("Hourly forecast")
    st.dataframe(fc[["ts", "pm25_pred", "pm25_lo", "pm25_hi", "category"]].round(1),
                 hide_index=True, use_container_width=True)

# ------------------------------------------------------------------ tab 2
with tab2:
    st.subheader("Chronological holdout (last week): error by model and horizon")
    mm = load(f"SELECT horizon_hours, model, n_hours, mae, rmse FROM `{T}.model_metrics` "
              "ORDER BY horizon_hours, mae")
    st.dataframe(mm, hide_index=True, use_container_width=True)
    st.caption("Lower is better. ARIMA_PLUS was best at 24 h and 48 h. Adding weather (ARIMA_PLUS_XREG) "
               "did not improve accuracy with one station and about a year of data. "
               "XREG used observed weather for the test week, which is slightly optimistic.")
    ev = load(f"SELECT ts, actual, pred_arima, pred_persistence FROM `{T}.forecast_eval` ORDER BY ts")
    st.line_chart(ev.set_index("ts"))

# ------------------------------------------------------------------ tab 3
with tab3:
    st.subheader("Forecast accuracy tracker (past forecasts vs reality)")
    acc = load(f"SELECT * FROM `{T}.v_accuracy_summary` ORDER BY horizon")
    if acc.empty:
        st.info("Collecting data: this fills in as new observations arrive after each forecast.")
    else:
        st.dataframe(acc, hide_index=True, use_container_width=True)
    st.subheader("Most recent flagged spikes")
    sp = load(f"SELECT ts, pm25, upper_bound FROM `{T}.anomaly_history` ORDER BY ts DESC LIMIT 15")
    st.dataframe(sp.round(1), hide_index=True, use_container_width=True)

# ------------------------------------------------------------------ tab 4
with tab4:
    st.subheader("Data quality and freshness")
    st.dataframe(health, hide_index=True, use_container_width=True)
    st.subheader("Recent pipeline runs")
    runs = load(f"SELECT * FROM `{T}.pipeline_runs` ORDER BY run_ts DESC LIMIT 15")
    st.dataframe(runs, hide_index=True, use_container_width=True)