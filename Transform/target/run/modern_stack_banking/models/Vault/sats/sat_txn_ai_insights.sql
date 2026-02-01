

    insert into "argus_vault"."main"."sat_txn_ai_insights" ("txn_hash_key", "ai_reason_for_flag", "load_date", "record_source")
    (
        select "txn_hash_key", "ai_reason_for_flag", "load_date", "record_source"
        from "sat_txn_ai_insights__dbt_tmp20260201171107758436"
    )
  