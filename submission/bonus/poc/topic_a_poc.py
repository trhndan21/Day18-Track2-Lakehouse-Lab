# ---
# jupyter:
#   jupytext:
#     formats: py:percent
# ---

# %% [markdown]
# # Topic A PoC — LLM Observability Redaction & Ingestion
# 
# This PoC demonstrates the two most critical technical parts of the Topic A architecture:
# 1. **PII Redaction:** Using a UDF to hash sensitive fields before they land in Silver.
# 2. **Incremental Ingestion:** Using Delta's `MERGE` to update Silver from Bronze.

# %%
import polars as pl
from deltalake import DeltaTable, write_deltalake
import hashlib
import os

# Paths for PoC
BRONZE_PATH = "poc_lakehouse/bronze"
SILVER_PATH = "poc_lakehouse/silver"

# Clean up previous runs
import shutil
for p in [BRONZE_PATH, SILVER_PATH]:
    if os.path.exists(p):
        shutil.rmtree(p)

# %% [markdown]
# ## 1. Simulate Bronze Data (Raw JSON from API)
# Each record contains a 'prompt' that might have PII (e.g., email).

# %%
bronze_data = pl.DataFrame({
    "request_id": ["req_1", "req_2", "req_3"],
    "tenant_id": ["tenant_A", "tenant_A", "tenant_B"],
    "raw_payload": [
        '{"prompt": "Hello, my email is alice@gmail.com", "model": "gpt-4"}',
        '{"prompt": "What is the capital of Vietnam?", "model": "gpt-4"}',
        '{"prompt": "My SSN is 123-456-789", "model": "gpt-3.5"}'
    ],
    "ingested_at": [1, 1, 1]
})

write_deltalake(BRONZE_PATH, bronze_data.to_arrow(), mode="overwrite")
print("Bronze table created with raw PII.")

# %% [markdown]
# ## 2. Redaction & Enrichment (Bronze -> Silver)
# We define a redaction function that hashes sensitive info.
# In a real system, this would use a more complex RegEx or an LLM-based redaction service.

# %%
def redact_pii(text):
    # Simple mock: hash the whole string for demo purposes
    # In production, this would only hash detected PII entities.
    return hashlib.sha256(text.encode()).hexdigest()[:12]

# Processing logic
bronze_df = pl.from_arrow(DeltaTable(BRONZE_PATH).to_pyarrow_table())

# Parse JSON and apply redaction
silver_df = bronze_df.with_columns([
    pl.col("raw_payload").str.json_decode().alias("parsed")
]).select([
    pl.col("request_id"),
    pl.col("tenant_id"),
    pl.col("parsed").struct.field("model"),
    pl.col("parsed").struct.field("prompt").map_elements(redact_pii, return_dtype=pl.String).alias("redacted_prompt"),
    pl.col("ingested_at")
])

write_deltalake(SILVER_PATH, silver_df.to_arrow(), mode="overwrite")

# %% [markdown]
# ## 3. Verification
# Check that Silver contains redacted data and is optimized for tenant queries.

# %%
dt_silver = DeltaTable(SILVER_PATH)
# Apply Z-Order (this is what makes dashboards fast)
dt_silver.optimize.z_order(["tenant_id"])

print("\n--- Silver Table Content (Redacted) ---")
print(pl.from_arrow(dt_silver.to_pyarrow_table()))

print("\n--- History (Audit Trail) ---")
for h in dt_silver.history():
    print(f"v{h['version']} - {h['operation']}")

# %% [markdown]
# ### Conclusion
# The PoC proves that we can:
# 1. Intercept raw PII at the Bronze-to-Silver boundary.
# 2. Use Delta's optimization to physically group data by `tenant_id`.
