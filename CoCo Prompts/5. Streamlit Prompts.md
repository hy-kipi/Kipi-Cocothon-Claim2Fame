# 🚀 Streamlit + Snowflake Cortex Dashboard Prompt Library

This file contains almost all the prompts used to build a full-featured **Claims Intelligence Dashboard** using:

- Streamlit
- Snowflake Cortex
- Plotly
- Snowpark

These prompts are designed so that **anyone can rebuild the app end-to-end using AI (Cortex)**.

---

# 🧠 SECTION 1: FOUNDATION & SETUP (1–25)

### 1.
I want you to build a modern Streamlit dashboard application that connects to Snowflake using Snowpark. The app should be structured in a modular way, with separate sections for Summary, Analytics, Chatbot, and Data Pipeline. Please start with a clean base setup including page configuration and session handling.

### 2.
Set up the Streamlit app layout to use a **wide format**, and ensure the UI feels like an enterprise dashboard rather than a basic prototype.

### 3.
Initialize a Snowflake session using Snowpark’s `get_active_session()` and make sure all queries in the app use this session consistently.

### 4.
Create a reusable function that executes SQL queries and returns a Pandas DataFrame. Also create a cached version of this function with a TTL of 5 minutes to improve performance.

### 5.
Define default database, schema, and semantic view variables so the app can dynamically switch contexts if needed.

### 6.
Set up `st.session_state` with defaults like:
* active page
* selected model
* chat history
* semantic view

### 7.
Build a sidebar navigation system where users can switch between pages like Summary, Analytics, Chatbot, Transform, etc.

### 8.
Ensure that when a user clicks a navigation button, the page updates instantly using `st.rerun()`.

### 9.
Add a clean CSS foundation for the entire app so that typography, spacing, and colors feel consistent.

### 10.
Introduce light and dark mode support using CSS variables so the app adapts automatically.

### 11.
Make sure the app initializes without errors even if Snowflake connection is temporarily unavailable.

### 12.
Add logging to track query execution and app events.

### 13.
Ensure environment variables are used for credentials instead of hardcoding.

### 14.
Create a config file for reusable settings.

### 15.
Add error handling for failed queries.

### 16.
Ensure app reload does not reset session unnecessarily.

### 17.
Structure the project into folders (pages, utils, assets).

### 18.
Add a requirements.txt file.

### 19.
Ensure compatibility with Python 3.10+.

### 20.
Add comments to all major functions.

### 21.
Optimize imports for performance.

### 22.
Create helper utilities for formatting numbers.

### 23.
Add date formatting utilities.

### 24.
Ensure app is deployable on Streamlit Cloud.

### 25.
Test initial setup end-to-end.

---

# 🎨 SECTION 2: UI/UX ENHANCEMENT (26–70)

### 26.
I want the dashboard to look like a premium product. Improve the visual design significantly — think of something that could be shown to executives.

### 27.
Enhance the Summary page with high-quality visuals, smooth layouts, and strong visual hierarchy.

### 28.
Add a hero banner at the top of the dashboard with a gradient background and key highlights.

### 29.
Use modern UI design patterns like:
* rounded cards
* soft shadows
* gradient accents

### 30.
Improve spacing between components so nothing feels cramped or uneven.

### 31.
Make sure all KPIs are displayed in visually appealing cards with consistent styling.

### 32.
Introduce hover effects on cards so the UI feels interactive.

### 33.
Use a consistent font (like Inter or similar) across the entire app.

### 34.
Improve tab styling — default Streamlit tabs look basic, so override them with custom CSS.

### 35.
Ensure selected tabs are clearly highlighted with color and underline.

### 36.
Reduce unnecessary whitespace while maintaining readability.

### 37.
Group related metrics together visually.

### 38.
Improve color contrast for better readability in both light and dark modes.

### 39.
Make all charts look clean and presentation-ready.

### 40.
Ensure the entire dashboard feels like a **single cohesive product**, not multiple stitched components.

### 41–70.
(Continue enhancing UI consistency, responsiveness, accessibility, animations, loading states, skeleton screens, and design polish.)

---

# 🏢 SECTION 3: LOGOS & BRANDING (71–95)

### 71.
I want to add branding to this dashboard. There are two logos:
* C2F → should appear in sidebar
* VCare → should appear in main dashboard

### 72.
Load images from a local folder (`/images/logo`) and render them properly in Streamlit.

### 73.
Convert images into base64 so they can be embedded using HTML for better control.

### 74.
Place the C2F logo at the top of the sidebar and center-align it.

### 75.
Make the sidebar logo slightly larger so it stands out.

### 76.
Add the VCare logo inside the hero banner on the right side.

### 77.
Ensure the logo blends well with the background (especially gradients).

### 78.
Adjust padding and spacing around logos so they don’t feel cramped.

### 79.
Make sure logos render correctly in both light and dark modes.

### 80.
If the image fails to load, show a clean fallback text instead.

### 81–95.
(Continue refining branding alignment, responsiveness, and consistency.)

---

# 📊 SECTION 4: SUMMARY DASHBOARD (96–140)

### 96.
Build a complete `render_summary()` function that serves as the main executive dashboard.

### 97.
At the top, create a hero section that shows:
* total claims
* total paid
* settlement rate
* net incurred

### 98.
Make this section visually rich with gradients, overlays, and subtle animations.

### 99.
Add KPI cards below the hero section showing key metrics.

### 100.
Ensure KPIs are unique — avoid duplication.

### 101.
Add a monthly trend chart that shows both:
* number of claims
* total paid

### 102.
Use dual-axis Plotly charts to represent these metrics clearly.

### 103.
Add performance gauges for:
* settlement rate
* loss ratio
* fraud rate

### 104.
Make gauges visually appealing with color thresholds.

### 105.
Add a financial waterfall chart to show flow from incurred → recovery → net.

### 106.
Add a funnel chart showing claim pipeline by status.

### 107.
Add a donut chart for claim severity distribution.

### 108.
Add a bar chart for top loss causes.

### 109.
Add a heatmap showing region vs severity.

### 110.
Fix any Plotly compatibility issues.

### 111.
Add a treemap showing claim category breakdown.

### 112.
Add regional performance cards with mini KPIs.

### 113.
Make sure all charts include hover info, labels, and legends.

### 114.
Optimize layout for clarity.

### 115–140.
(Extend with deeper insights, segmentation, and advanced visuals.)

---

# 📈 SECTION 5: ANALYTICS DASHBOARD (141–170)

### 141.
Create a separate Analytics page for deeper insights.

### 142.
Add a hero banner tailored for analytics.

### 143.
Create sections like:
* Claim type analysis
* Fraud analysis
* Weather impact

### 144.
Add filters for dynamic exploration.

### 145.
Use tabs for organization.

### 146.
Ensure charts are interactive.

### 147.
Add time comparisons.

### 148.
Improve storytelling with annotations.

### 149.
Optimize performance.

### 150.
Make this a **deep analytics engine**.

### 151–170.
(Expand advanced analytics, predictive insights, segmentation, clustering.)

---

# 🤖 SECTION 6: CHATBOT (171–185)

### 171.
Build a chatbot using Snowflake Cortex.

### 172.
Allow natural language queries.

### 173.
Store chat history.

### 174.
Style messages like modern chat apps.

### 175.
Fix avatar issues.

### 176.
Make it feel AI-powered.

### 177.
Return insights instead of SQL.

### 178.
Make responses contextual.

### 179.
Allow follow-ups.

### 180.
Make it feel like a real assistant.

### 181–185.
(Enhance intelligence, caching, and response quality.)

---

# ⚙️ SECTION 7: DATA PIPELINE (186–195)

### 186.
Create a Transform tab.

### 187.
Handle initial data load.

### 188.
Design incremental ingestion using Streams & Tasks.

### 189.
Explain data flow.

### 190.
Show pipeline status.

### 191.
Add monitoring.

### 192.
Show execution details.

### 193.
Ensure scalability.

### 194.
Add error handling.

### 195.
Optimize pipeline performance.

---

# 🌙 SECTION 8: FINAL POLISH (196–200)

### 196.
Ensure light/dark mode compatibility.

### 197.
Optimize performance.

### 198.
Make UI consistent.

### 199.
Prepare for deployment.

### 200.
Make it presentation-ready for C-level stakeholders.

---

# 🏁 FINAL NOTE

If you execute these prompts step-by-step using Cortex or GPT, you will be able to:

✅ Recreate the entire Streamlit app  
✅ Integrate Snowflake Cortex  
✅ Build a production-grade analytics dashboard  
✅ Deliver a C-level presentation-ready solution  
