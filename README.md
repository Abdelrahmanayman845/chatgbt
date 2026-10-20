# Dormancy Pipeline and Leads Inference

This project prepares monthly credit-card transaction features and uses a saved LSTM model to generate dormancy campaign leads.

The workflow contains four stages:

```text
Aggregation → DataPreparation → StopperPipeline → LeadsInference
```

The main runner executes these stages sequentially using a shared Spark session.

## Project Structure

```text
main.py
pipelines/
    ch1_aggregation.py
    ch2_Data_Preparation.py
    ch3_Stopper_Pipeline.py
    leads_inference.py
    ch4_model_data_prep.py
    ch5_train_lstm.py
    ch6_test_lstm.py
src/
    ch1_Data_loader.py
    ch2_Transactions_Aggregation.py
    ch3_Rolling_average.py
    ch4_Inactivity_counter.py
    ch5_remaining_balance.py
    ch6_Delay_days_gaps.py
    ch7_filling_missing.py
    ch8_modeling_data_prep.py
```

| Script | Responsibility |
| --- | --- |
| `main.py` | Creates Spark, runs all four stages, logs execution, and stops Spark |
| `pipelines/ch1_aggregation.py` | Aggregates transaction history into monthly account features |
| `pipelines/ch2_Data_Preparation.py` | Joins transaction features with statement balances and account information |
| `pipelines/ch3_Stopper_Pipeline.py` | Calculates the balance-adjusted features required by the model and leads |
| `pipelines/leads_inference.py` | Loads the saved model, generates predictions, applies lead filters, and saves campaigns |
| `pipelines/ch4_model_data_prep.py` | Prepares training arrays and a scaler separately |
| `pipelines/ch5_train_lstm.py` | Trains an LSTM separately |
| `pipelines/ch6_test_lstm.py` | Legacy separate model evaluation script |

Model training is not part of the main monthly run.

## Environment Requirements

Run the project in the Spark/Hive environment that has access to the organization's tables and model files.

Required components:

- Python and a compatible Spark/PySpark and Java runtime.
- Hive/catalog access.
- NumPy and pandas.
- scikit-learn.
- TensorFlow/Keras compatible with the saved model.
- PyArrow when Arrow conversion is enabled.
- Read access to source tables and model/scaler files.
- Permission to overwrite monthly feature tables and append to the campaign table.
- Enough driver memory for collecting inference sequences with `toPandas()`.

Use library versions compatible with the saved artifacts. No tested dependency lockfile is supplied for this workflow.

The code imports modules through both `pipelines` and `Development_Chadi.src`. Run from the project directory and ensure the project package is available on Python's import path.

## Input Tables

| Input | Table |
| --- | --- |
| Transactions | `staging_zone.hpsv3_mw_cr_transaction_view` |
| Statement balances | `staging_zone.hpsv3_v_cr_term_view` |
| Current account information | `staging_zone.hpsv3_shadow_account_cdc` |
| Customer profiles | `staging_zone.hpsv3_collection_data_file` |
| Monthly customer personas | `analytics_zone_data_mart.dm_credit_card_transactions_l2_mom_sa` |
| Previous campaign history | The table configured as `CAMPAIGN_TABLE` in inference |

External source tables are read from the locations listed above.

## Output Tables

All monthly feature outputs are saved in:

```text
analytics_zone_retail_products
```

| Stage | Output Table |
| --- | --- |
| Aggregation | `analytics_zone_retail_products.dormancy_step0_YYYY_MM` |
| DataPreparation | `analytics_zone_retail_products.dormancy_step1_YYYY_MM` |
| StopperPipeline | `analytics_zone_retail_products.dormancy_step2_YYYY_MM` |
| LeadsInference | The configured `CAMPAIGN_TABLE` |

The table connections must match:

```text
Aggregation writes Step 0
DataPreparation reads Step 0 and writes Step 1
StopperPipeline reads Step 1 and writes Step 2
LeadsInference reads Step 2
```

The current test campaign destination is:

```python
CAMPAIGN_TABLE = "analytics_zone_retail_products.dormancy_campaigns_test"
```

## Pipeline Dates

Monthly feature dates are calculated inside the pipeline scripts.

The start date is inclusive. The end date is exclusive, so the current month is excluded.

For an October 2026 run:

```text
Start date:                2025-06-01
End date, exclusive:       2026-10-01
Latest monthly period:     2026-09-01

Aggregation output:        analytics_zone_retail_products.dormancy_step0_2026_10
Data preparation output:   analytics_zone_retail_products.dormancy_step1_2026_10
Final feature output:      analytics_zone_retail_products.dormancy_step2_2026_10
```

## Stage 1: Aggregation

**Script:** `pipelines/ch1_aggregation.py`

**Class:** `Aggregation`

This stage:

1. Loads transaction history using the transaction filters.
2. Creates purchase frequency and amount indicators.
3. Aggregates transactions by account, year, and month.
4. Calculates transaction dates, transaction counts, and supporting gap features.
5. Generates the account-month reference grid.
6. Applies the output date range.
7. Saves the Step 0 table.

## Stage 2: Data Preparation

**Script:** `pipelines/ch2_Data_Preparation.py`

**Class:** `DataPreparation`

This stage:

1. Reads Step 0.
2. Loads statement balances.
3. Joins balances by account, year, and month.
4. Loads current account information.
5. Joins account opening, closing, and administrative-status information.
6. Calculates utilization, available credit, and account status.
7. Saves the Step 1 table.

## Stage 3: Stopper Features

**Script:** `pipelines/ch3_Stopper_Pipeline.py`

**Class:** `StopperPipeline`

This stage reads Step 1, removes duplicate rows, and prepares the balance-adjusted features required by the LSTM and leads output.

The calculation order is:

```python
df_new = df_new.dropDuplicates()
df_new = fill_missing_months(df_new)
df_new = delay_days_balance(df_new)
df_new = rolling_delay_stats(df_new)
df_new = filling_gaps(df_new)
df_new = recency_balance(df_new)
df_new = deviation_recency_balance(df_new)
```

The model feature is:

```text
RD_from_last_gap_balance =
    round(
        (recency_balance - rolling_avg_delay_days_p)
        / stddev_delay_days_p,
        4
    )
```

The stage uses balance adjustments, rolling windows, sample standard deviation, null handling, and forward filling.

### Final Columns

```text
SHADOW_ACCOUNT_NBR
Year
Month
period_date
opening_date
closing_date
ADMINISTRATIVE_STATUS
credit_limit
utilized_rate
trx_count
recency_balance
rolling_avg_delay_days_p
RD_from_last_gap_balance
```

## Stage 4: Leads Inference

**Script:** `pipelines/leads_inference.py`

**Class:** `LeadsInference`

The class uses the Spark session supplied by the main runner.

The inference source is:

```python
SOURCE_TABLE = f"analytics_zone_retail_products.dormancy_step2_{LEAD_YEAR}_{LEAD_MONTH:02d}"
```

### Model and Scaler

The current paths are:

```python
SCALER_PATH = "/home/cdsw/_ayman/dormancy/leads/model/scaler.pkl"
MODEL_PATH = "/home/cdsw/_ayman/dormancy/leads/model/smallseq2_31_03_2026"
```

The model is loaded with its custom loss:

```python
model = load_model(MODEL_PATH, custom_objects={"mse_mae_loss": mse_mae_loss})
```

Use the scaler paired with the deployed model.

### Lead Selection

Inference applies the following rules:

- Keeps dormancy scores greater than or equal to `2.2`.
- Joins account features from the previous month.
- Joins personas from the previous month.
- Joins customer profiles from the latest available profile insertion date.
- Requires card type, phone number, and guarantee type.
- Excludes guarantee types containing `staff`.
- Keeps utilization less than or equal to `0.8`.
- Applies the recency groups listed below.
- Excludes accounts present in other campaigns in the configured campaign table.
- Calculates behavior change and status buckets.
- Rounds the output dormancy score to four decimals.

Recency groups are:

| Group | Condition |
| --- | --- |
| Low | Recency below 30 days |
| Medium | Recency from 30 through 60 days |
| High | Recency above 60 through 300 days |

The groups are combined in the sequence high, medium, then low. Final output ordering is not guaranteed.

Profile and persona records are joined without additional deduplication.

### Status Buckets

```text
Behavior_Change = Recency_Balance - Avg_delay_days
```

Conditions are evaluated in this order:

| Condition | Status |
| --- | --- |
| Behavior change from 90 through 120 | `Low_Stop_Spender` |
| Behavior change above 120 through 200 | `Medium_Stop_Spender` |
| Behavior change above 200 | `High_Stop_Spender` |
| Otherwise, recency below 30 | `Low_Dormant` |
| Otherwise, recency from 30 through 60 | `Medium_Dormant` |
| Otherwise, recency above 60 | `High_Dormant` |

### Output Columns

Generated leads contain:

```text
account_number
card_type
phone_no
dormancy_score
credit_limit
utilized_rate
recency_balance
avg_delay_days
behavior_change
e_commerce_persona
traveler_person
shopping_person
foodie_person
investor_person
car_dependent_person
transportation_person
entertainment_person
health_conscious_person
education_spender
everyday_spender
auto_spender
inactive_person
status_bucket
Campaign_ID
```

## Running the Full Project

The main runner registers all four stages:

```python
pipelines = [
    ("aggregation", Aggregation),
    ("data_preparation", DataPreparation),
    ("stopper", StopperPipeline),
    ("leads_inference", LeadsInference),
]
```

Before running:

- Confirm the required scripts are available.
- Confirm source and target table names match across stages.
- Confirm source-table access and output-table permissions.
- Confirm the model and scaler files exist.
- Confirm the configured campaign table exists with the expected schema.
- Check the campaign ID and save setting.

### Execution Logs

Logs identify:

- Stage start.
- Stage completion and duration.
- Cache cleanup.
- Exceptions and failing stage.
- Overall completion.
- Spark shutdown.

When campaign saving succeeds, inference also prints:

```text
Saved <Campaign_ID>
```

A failure in a later stage does not roll back tables already written by earlier stages.

## Testing Without Saving

In `pipelines/leads_inference.py`, set:

```python
SAVE_CAMPAIGN = False
```

Inference still performs scoring, joins, filters, and campaign-history exclusions.

Only the final campaign append and duplicate-wave save check are skipped. The campaign table must still exist because it is read during inference.

Running the full `main.py` still overwrites the monthly Step 0–2 tables, even when campaign saving is disabled.

### Display Generated Leads

After the final `resultDF.select(...)` and before the save block, use:

```python
print("Generated leads:")
resultDF.show(100, truncate=False)

if SAVE_CAMPAIGN:
    save_campaign(spark, resultDF)
```

This displays the first 100 final lead rows.

The status-bucket count display provides a summary of the generated data.

## Saving to the Test Campaign Table

The test destination is:

```python
CAMPAIGN_TABLE = "analytics_zone_retail_products.dormancy_campaigns_test"
SAVE_CAMPAIGN = True
```

The default campaign ID is:

```python
CAMPAIGN_ID = f"wave_{LEAD_YEAR}{LEAD_MONTH:02d}"
```

For October 2026:

```text
wave_202610
```

The save function:

1. Reads the destination table.
2. Checks whether the campaign ID already exists.
3. Validates that generated leads contain the destination schema fields.
4. Casts and selects columns to match the destination schema.
5. Appends the result.

The destination must already exist. Changing `CAMPAIGN_TABLE` does not create it automatically.

### Duplicate-Wave Protection

If the campaign ID already exists, saving raises an error.

This is intentional protection against appending the same wave again.

For inspection, keep the real campaign ID and set:

```python
SAVE_CAMPAIGN = False
```

### Campaign History

`CAMPAIGN_TABLE` controls both:

- The history used to exclude previously contacted accounts.
- The destination used for appending new leads.

The selected campaign table must contain the history required for account exclusions.

When the configured destination is `dormancy_campaigns_test`, inference does not append to the original campaign table.

## Separate Training Workflow

Training is optional and separate from the monthly runner.

Before running it, ensure `load_data()` in `src/ch8_modeling_data_prep.py` reads the appropriate Step 2 table.

Required data and model directories must already exist and be writable.

## Summary

The pipeline prepares monthly transaction and account data, enriches it with balances and account information, and generates dormancy campaign leads using a saved LSTM model. The four stages run sequentially through `main.py`, with intermediate datasets stored in `analytics_zone_retail_products`. Inference adds customer profiles and personas, applies eligibility and campaign-history rules, and assigns behavior classifications. Campaign saving appends results to the selected destination after schema validation and duplicate-wave checks; saving can be disabled to review results without appending a campaign.
