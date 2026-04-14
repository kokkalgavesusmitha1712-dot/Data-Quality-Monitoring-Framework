"""
generate_sample_data.py
Generates intentionally imperfect sample data to demo
all quality check types — nulls, duplicates, range errors,
invalid categories, and format violations.
"""

import pandas as pd
import numpy as np
from pathlib import Path

np.random.seed(99)
N = 300

statuses  = ["approved","denied","pending","under_review"]
cl_types  = ["medical","pharmacy","dental","vision","behavioral"]
regions   = ["Northeast","Southeast","Midwest","Southwest","West"]
plans     = ["PPO","HMO","EPO","HDHP","Medicaid"]

df = pd.DataFrame({
    "claim_id":       [f"CLM{str(i).zfill(5)}" for i in range(1, N+1)],
    "member_id":      [f"MBR{np.random.randint(1000,9999)}" for _ in range(N)],
    "provider_id":    [f"PRV{np.random.randint(100,999)}"  for _ in range(N)],
    "claim_date":     pd.date_range("2024-01-01", periods=N, freq="D").strftime("%Y-%m-%d"),
    "claim_type":     np.random.choice(cl_types, N),
    "billed_amount":  np.round(np.random.uniform(50, 5000, N), 2),
    "allowed_amount": np.round(np.random.uniform(40, 4500, N), 2),
    "paid_amount":    np.round(np.random.uniform(30, 4000, N), 2),
    "claim_status":   np.random.choice(statuses, N),
    "processing_days":np.random.randint(1, 20, N),
    "region":         np.random.choice(regions, N),
    "plan_type":      np.random.choice(plans, N),
})

# Introduce intentional quality issues for demo
df.loc[np.random.choice(N, 15, replace=False), "member_id"]    = None
df.loc[np.random.choice(N,  5, replace=False), "claim_status"] = "UNKNOWN"
df.loc[np.random.choice(N,  3, replace=False), "billed_amount"]= -99
df.loc[np.random.choice(N,  2, replace=False), "claim_date"]   = "bad-date"
df.loc[5, "claim_id"] = "CLM00001"   # introduce a duplicate

out = Path("data/sample/claims_sample.csv")
out.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(out, index=False)
print(f"Generated {N} claims records with intentional quality issues → {out}")

members = pd.DataFrame({
    "member_id":    [f"MBR{i}" for i in range(1000, 1200)],
    "member_name":  [f"Member {i}" for i in range(200)],
    "date_of_birth":pd.date_range("1960-01-01", periods=200, freq="180D").strftime("%Y-%m-%d"),
    "gender":       np.random.choice(["M","F","O"], 200),
    "plan_type":    np.random.choice(plans, 200),
    "state":        np.random.choice(["NY","NJ","CA","TX","FL"], 200),
})
members.loc[0, "member_name"] = None
members.to_csv("data/sample/members_sample.csv", index=False)
print(f"Generated 200 member records → data/sample/members_sample.csv")
