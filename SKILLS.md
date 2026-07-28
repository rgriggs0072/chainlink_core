# SKILLS.md — Chainlink Analytics

> Project-level coding conventions and rules for `chainlink_core`.
> Add to this file whenever the same fix has to be applied more than once.
> Claude (and any other dev) should read this BEFORE writing or editing code in this repo.

---

## Tech StackTask Completion Report
- **Language**: Python 3.x
- **UI**: Streamlit **1.56.0** (pinned in `requirements.txt` on both `dev` and `main` — verify before targeting an API). `requirements-dev.txt` separately pins `streamlit==1.43.0` but is an undeployed "packages being tested for upgrade" file, not what either branch actually runs.
- **Backend**: Snowflake
- **Hosting**: Streamlit Community Cloud (`chainlinkcore-main.streamlit.app`)

---

## Streamlit Conventions

### `width='stretch'` / `width='content'` — NEVER use `use_container_width`

**Rule**: As of Streamlit 1.56.0+, `use_container_width` is deprecated and will be removed after 2025-12-31. Always use the `width` parameter instead.

| Old (deprecated) | New (use this) |
|---|---|
| `use_container_width=True` | `width='stretch'` |
| `use_container_width=False` | `width='content'` |

Applies to: `st.button`, `st.dataframe`, `st.image`, `st.plotly_chart`, `st.altair_chart`, `st.pyplot`, `st.download_button`, `st.form_submit_button`, `st.columns`, and any other component that accepts a width argument.

**Examples**:

```python
# ❌ WRONG — deprecated
st.button("Run", use_container_width=True)
st.dataframe(df, use_container_width=True)
st.image(img, use_container_width=False)

# ✅ RIGHT
st.button("Run", width='stretch')
st.dataframe(df, width='stretch')
st.image(img, width='content')
```

Both `dev` and `main` currently pin Streamlit 1.56.0, so this rule applies on either branch — no `width`-vs-`use_container_width` branch split exists today. Re-check `requirements.txt` if that ever changes.

### `st.form` for file uploaders

Wrap file uploaders in `st.form` to prevent Streamlit Community Cloud from wiping uploader state on rerun. This was the root cause of the v1.0.4 Reset Schedule upload bug.

```python
with st.form("uploader_form"):
    file = st.file_uploader("Upload file", type=["xlsx"])
    submitted = st.form_submit_button("Upload", width='stretch')
    if submitted and file:
        # process file
```

### File buffer reads

When a file uploader's bytes are needed in multiple places, **read once into a DataFrame and pass the DataFrame** — don't pass the file object around. Streamlit Community Cloud exhausts file buffers on second read (root cause of the v1.1.2 Supplier by County bug).

---

## Python / Pandas Conventions

### Cast numpy types to native Python before passing to the Snowflake connector

**Rule**: When building records for `cursor.executemany()` (or any direct bind to the Snowflake connector), explicitly cast values from a DataFrame to native Python types. **Do not pass numpy scalars (`np.int64`, `np.float64`, `np.bool_`, etc.) directly** — pandas 2.x returns these more aggressively and the Snowflake connector raises `NP.INT64` SQL compilation errors on them.

```python
# ❌ WRONG — numpy types leak through
records = [
    (row["STORE_NUMBER"], row["TENANT_ID"], row["STORE_NAME"])
    for _, row in df.iterrows()
]
cursor.executemany(sql, records)
# Snowflake raises: SQL compilation error: NP.INT64 ...

# ✅ RIGHT — explicit casts to native Python
records = [
    (
        int(row["STORE_NUMBER"]) if pd.notna(row["STORE_NUMBER"]) else None,
        int(row["TENANT_ID"]),
        str(row["STORE_NAME"]) if pd.notna(row["STORE_NAME"]) else None,
    )
    for _, row in df.iterrows()
]
cursor.executemany(sql, records)
```

**Apply this rule everywhere we write to Snowflake from a DataFrame**, not just the three functions fixed in v1.5.0:
- `write_customers_to_snowflake()`
- `write_products_to_snowflake()`
- `write_salesreport_to_snowflake()`
- Any new `write_*_to_snowflake()` function — add the casts from the start

**Cheat sheet** for the common cases:
- Integer column → `int(value) if pd.notna(value) else None`
- Float column → `float(value) if pd.notna(value) else None`
- String column → `str(value) if pd.notna(value) else None`
- Boolean column → `bool(value)` (be careful: `np.bool_` is truthy but not a Python bool)
- Date/datetime → `value.to_pydatetime() if pd.notna(value) else None` (returns Python datetime)
- NaN/NaT/None → always map to `None` for SQL `NULL`

**Alternative**: `df.astype(object).where(df.notna(), None).to_dict('records')` works for some cases but loses type information — explicit per-column casts are safer.

---

## Snowflake / Data Conventions

### TENANT_ID scoping is mandatory

Every query that touches tenant data MUST include `TENANT_ID = :tenant_id` (or equivalent bind). Pull `tenant_id` from session state — never hardcode, never trust user input. The AI Data Query module enforces this end-to-end (v1.3.0); follow the same pattern in any new query path.

### Use the `snowflake-schema` skill for SQL

For any SQL touching CUSTOMERS, DISTRO_GRID, PRODUCTS, SUPPLIER_COUNTY, RESET_SCHEDULE, or SALES_REPORT, invoke the `snowflake-schema` skill before writing the query. It has the full table/column/JOIN reference.

---

## Versioning

- Source of truth: `version.txt` (single line, format `X.X.X` — no `v` prefix in the file itself; the UI prepends `v` when displaying it, and `CHANGELOG.md` version headers use `[vX.X.X]`)
- `app.py` reads `version.txt` at runtime to display the version in the UI
- On every release: update `version.txt` AND `CHANGELOG.md` together
- See the `changelog` skill for the full release workflow

---

## Common Pitfalls (things that have bitten us before)

- **Don't use `pd.to_datetime()` on time-only values** — use the `_normalize_time()` helper in `utils/`. Root cause of v1.1.0 RESET_TIME bug.
- **Don't call `PARSE_JSON()` inside a SQL `VALUES` clause** — Snowflake doesn't support it. Use `INSERT INTO ... SELECT ... FROM (SELECT 1)` pattern instead (v1.2.0 LOG table fix).
- **Don't archive DISTRO_GRID rows BEFORE running `UPDATE_DISTRO_GRID`** — PRODUCT_ID and COUNTY won't be stamped yet, and the matched archive will write 0 rows. Archive AFTER the procedure (v1.2.0 fix).
- **Don't reference `DISTRO_GRID_ARCHIVE`** — it was renamed to `DISTRO_GRID_ARCHIVE_FULL` in v1.2.0. Placement Intelligence reads from `DISTRO_GRID_MATCHED_ARCHIVE`.

---


## Task Completion Reports

**Rule**: Every task ends with a summary block below — no exceptions, even for small or self-explanatory changes. Applies to code changes, merges, deployments, and investigations alike.

```
## Task Complete: <short title>

**What shipped:** 1-2 lines, plain language.

**Files changed:**
- path/to/file.py

**Commits:**
- dev: <hash> — <message>
- main: <hash> — <message>  (omit if not merged)

**Branch state:** left on `<branch>`, <clean|dirty>

**Tests run:** <what was tested> — <pass/fail/N/A>

**Blockers / follow-ups:** <none, or what's outstanding>
```

If a task stops mid-way or hits a blocker, the same report fires anyway — report current state and what's blocking. Never go quiet without it.

---

## How to Add to This File

When you (or Claude) catches the same mistake more than once, add a rule here. Format:

```markdown
### <short rule name>

**Rule**: <one-sentence statement of what to do / not do>

<optional: code example showing wrong vs right>

<optional: link to the bug/PR/changelog entry where this came up>
```

Keep rules concrete and actionable. If a rule is just "be careful," it's not a rule — it's a wish.
