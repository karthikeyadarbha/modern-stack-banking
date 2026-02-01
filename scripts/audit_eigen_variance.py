#!/usr/bin/env python3
import duckdb
import numpy as np
import pandas as pd
import argparse
import os

def run_audit(db_path, sample_size):
    if not os.path.exists(db_path):
        print(f"❌ Error: Database not found at {db_path}")
        return

    # 1. Connect to DuckDB
    con = duckdb.connect(db_path)
    
    # 2. Extract PCA components dynamically
    # We use DuckDB's JSON extraction to turn the map into columns
    print(f"🔍 Sampling {sample_size} transactions from gold_fraud_analysis...")
    query = f"""
        SELECT 
            (pca_features->>'V1')::FLOAT as V1,
            (pca_features->>'V2')::FLOAT as V2,
            (pca_features->>'V3')::FLOAT as V3,
            (pca_features->>'V4')::FLOAT as V4,
            (pca_features->>'V14')::FLOAT as V14,
            (pca_features->>'V17')::FLOAT as V17
        FROM gold_fraud_analysis
        WHERE is_fraud_label IS NOT NULL
        LIMIT {sample_size}
    """
    df = con.execute(query).df()
    con.close()

    # 3. Standardize and Compute Covariance
    data_normalized = (df - df.mean()) / df.std()
    cov_matrix = np.cov(data_normalized.T)

    # 4. Calculate Eigenvalues
    eigenvalues, _ = np.linalg.eig(cov_matrix)
    eigenvalues = sorted(eigenvalues, reverse=True)

    # 5. Output Report
    print("\n📈 --- PCA VULNERABILITY & VARIANCE REPORT ---")
    total_variance = sum(eigenvalues)
    cumulative_variance = 0
    
    for i, val in enumerate(eigenvalues):
        var_pct = (val / total_variance) * 100
        cumulative_variance += var_pct
        status = "✅ CORE SIGNAL" if var_pct > 10 else "⚙️ NOISE/REFINEMENT"
        print(f"PC{i+1:02d}: {val:.4f} | {var_pct:5.2f}% (Total: {cumulative_variance:5.2f}%) | {status}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audit the Eigenvalues of the Data Vault.")
    parser.add_argument("--db", default="data/warehouse/argus_vault.db", help="Path to DuckDB")
    parser.add_argument("--size", type=int, default=500, help="Number of rows to sample")
    args = parser.parse_args()
    
    run_audit(args.db, args.size)