# 🚀 Streamlit + Snowflake Cortex Dashboard Prompt Library

This document contains **200+ prompts** used to build a full-featured **Claims Intelligence Dashboard** using:

- Streamlit
- Snowflake Cortex
- Plotly
- Snowpark

These prompts are designed so that **anyone can rebuild the app end-to-end using AI (Cortex / GPT)**.

---

## 🧠 SECTION 1: INITIAL SETUP (1–20)

1. Create a Streamlit dashboard using Snowflake Snowpark session
2. Setup page config with wide layout
3. Add sidebar navigation with multiple tabs
4. Create a Summary dashboard page
5. Connect Streamlit to Snowflake using `get_active_session()`
6. Create reusable query function using Snowpark
7. Add caching layer for queries (TTL 300)
8. Create base CSS for UI styling
9. Add light and dark theme support
10. Define default database, schema, and semantic view
11. Add session state defaults
12. Build navigation state handling
13. Create modular functions for each page
14. Add Plotly support for charts
15. Setup KPI cards using `st.metric`
16. Add expander sections for insights
17. Create structured layout using columns
18. Build reusable color palette
19. Add typography styling (Inter font)
20. Improve UI spacing and alignment

---

## 🎨 SECTION 2: UI / UX ENHANCEMENTS (21–60)

21. Improve summary page with high graphic content
22. Make dashboard visually premium and executive-ready
23. Add gradient backgrounds to KPI cards
24. Add hero banner at top of summary page
25. Add animated feel using CSS transitions
26. Improve spacing between sections
27. Use rounded cards with shadows
28. Add hover effects on cards
29. Improve tab styling using custom CSS
30. Reduce clutter and increase readability
31. Improve font consistency across app
32. Add subtle background textures
33. Improve chart container styling
34. Add section headers with icons
35. Add divider styling improvements
36. Improve alignment of KPIs
37. Add grid layout for metrics
38. Make charts responsive
39. Improve sidebar visual hierarchy
40. Add branding colors (blue theme)
41. Add glassmorphism effects
42. Improve button styling
43. Add subtle animations to charts
44. Improve tooltip styling
45. Add consistent padding across sections
46. Improve layout for large screens
47. Reduce white space imbalance
48. Add card grouping for related metrics
49. Improve visual storytelling
50. Highlight important KPIs
51. Add contextual labels to charts
52. Improve legend readability
53. Add contrast improvements for dark mode
54. Optimize UI for presentations
55. Improve scroll experience
56. Add header shadows
57. Align icons and text properly
58. Add section separators
59. Improve mobile responsiveness
60. Final UI polish for executive demo

---

## 🏢 SECTION 3: LOGO & BRANDING (61–80)

61. Add company logos to app
62. Add C2F logo in sidebar
63. Add VCare logo in main dashboard
64. Load logos from `/images/logo`
65. Convert images to base64
66. Render logo using HTML in Streamlit
67. Adjust logo size in sidebar
68. Make sidebar logo bigger
69. Align logo center
70. Add logo inside hero banner
71. Place logo on right side of banner
72. Improve logo clarity
73. Adjust logo padding
74. Ensure logos work in dark mode
75. Add fallback if logo not found
76. Add branding text below logo
77. Improve logo scaling responsiveness
78. Align logo with title text
79. Add hover effect to logo
80. Ensure consistent branding across pages

---

## 📊 SECTION 4: SUMMARY DASHBOARD (81–120)

81. Build full `render_summary()` function
82. Add hero banner with KPIs
83. Show total claims
84. Show open claims
85. Show total paid amount
86. Show settlement rate
87. Add monthly trend chart
88. Add dual-axis chart (claims + paid)
89. Add performance gauges
90. Add settlement gauge
91. Add loss ratio gauge
92. Add fraud rate gauge
93. Add average close time gauge
94. Add financial waterfall chart
95. Add claims funnel chart
96. Add severity donut chart
97. Add top loss causes bar chart
98. Add heatmap (region vs severity)
99. Fix Plotly heatmap colorbar error
100. Add claims pipeline visualization
101. Add treemap for categories
102. Add paid vs reserves chart
103. Add regional performance cards
104. Add regional comparison chart
105. Add trend smoothing (spline)
106. Add dynamic annotations
107. Improve chart colors
108. Add hover templates
109. Add data labels on charts
110. Add grid styling
111. Improve legend positioning
112. Add insights summary section
113. Remove duplicate KPIs
114. Keep only unique metrics
115. Optimize query performance
116. Add caching to all queries
117. Improve chart readability
118. Add multi-metric overlays
119. Improve layout spacing
120. Final polish of summary dashboard

---

## 📈 SECTION 5: ANALYTICS DASHBOARD (121–150)

121. Build analytics dashboard page
122. Add hero banner for analytics
123. Add KPI summary row
124. Add multiple analysis dimensions
125. Add settlement vs pending analysis
126. Add loss ratio analytics
127. Add fraud analysis charts
128. Add weather impact analysis
129. Add category-based insights
130. Add claim type analysis
131. Improve tab UI for analytics
132. Add tab-based breakdowns
133. Add comparative charts
134. Add trend analysis visuals
135. Improve chart interactivity
136. Add drill-down capability
137. Add filters for analytics
138. Improve chart layout
139. Add dynamic titles
140. Add contextual tooltips
141. Add category color mapping
142. Improve gauge consistency
143. Add KPI grouping
144. Optimize SQL queries
145. Improve performance
146. Add caching layer
147. Improve responsiveness
148. Reduce visual clutter
149. Align all analytics sections
150. Final polish of analytics page

---

## 🤖 SECTION 6: CHATBOT (151–170)

151. Build chatbot tab
152. Integrate Snowflake Cortex Analyst
153. Add chat history support
154. Add message roles (user/assistant)
155. Fix avatar image error
156. Improve chatbot UI styling
157. Add AI-themed design
158. Add chat bubbles styling
159. Improve input box styling
160. Add loading spinner
161. Add error handling
162. Improve response formatting
163. Avoid showing raw SQL in output
164. Return business insights instead
165. Add context-aware responses
166. Improve NLP understanding
167. Add follow-up questions support
168. Add memory of last queries
169. Improve performance of responses
170. Final chatbot polish

---

## ⚙️ SECTION 7: DATA PIPELINE & TRANSFORM (171–190)

171. Build transform tab
172. Add pipeline overview
173. Handle initial data load
174. Design incremental pipeline
175. Use Snowflake Streams
176. Use Snowflake Tasks
177. Create incremental ingestion logic
178. Add UI for pipeline monitoring
179. Add pipeline status indicators
180. Add refresh controls
181. Add execution logs
182. Add error handling
183. Optimize transformation queries
184. Add scheduling logic
185. Improve pipeline scalability
186. Add data validation checks
187. Add retry mechanisms
188. Improve pipeline visualization
189. Add alerts for failures
190. Final pipeline design

---

## 🌙 SECTION 8: FINAL ENHANCEMENTS (191–200)

191. Add dark mode support
192. Add light mode support
193. Ensure consistent theming
194. Improve accessibility
195. Optimize performance
196. Reduce load times
197. Improve caching strategy
198. Final UI/UX polish
199. Prepare for production deployment
200. Make dashboard presentation-ready

---

## ✅ HOW TO USE

1. Copy prompts into Cortex / GPT
2. Execute step by step
3. Validate output after each stage
4. Combine all generated code

---

## 🏁 RESULT

Using these prompts, you can recreate:

- Full Streamlit App
- Snowflake Integration
- AI Chatbot
- Advanced Analytics Dashboard
- Production-ready UI

---

🔥 This is essentially a **prompt-driven architecture blueprint**
