# Dormancy Pipeline and Leads Inference

This project prepares monthly credit-card transaction features and uses a saved LSTM model to generate dormancy campaign leads.

The updated workflow contains four stages:

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

Use library versions compatible with the saved artifacts. No tested dependency lockfile is supplied for the updated workflow.

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

The output-zone change does not move or rename these external source tables.

## Output Tables

All monthly feature outputs are saved in:

```text
analytics_zone_retail_products
```

| Stage | Output Table | Write Mode |
| --- | --- | --- |
| Aggregation | `analytics_zone_retail_products.dormancy_step0_YYYY_MM` | Overwrite |
| DataPreparation | `analytics_zone_retail_products.dormancy_step1_YYYY_MM` | Overwrite |
| StopperPipeline | `analytics_zone_retail_products.dormancy_step2_YYYY_MM` | Overwrite |
| LeadsInference | The configured `CAMPAIGN_TABLE` | Append when enabled |

The new monthly table names do not contain an `_cdc` suffix.

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

Monthly feature dates are calculated inside the pipeline scripts. No separate configuration file is required.

```python
self.run_month = date.today().replace(day=1)
self.start_date = "2025-06-01"
self.end_date = self.run_month.isoformat()
```

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

Dates use the execution machine's local date.

All scripts should use the same run month. Avoid crossing a month boundary during execution without ensuring the independently calculated dates agree.

### Display the Current Values

Run this in a notebook:

```python
from datetime import date

run_month = date.today().replace(day=1)

print("Start date:", "2025-06-01")
print("End date, exclusive:", run_month.isoformat())
print("Step 0:", f"analytics_zone_retail_products.dormancy_step0_{run_month:%Y_%m}")
print("Step 1:", f"analytics_zone_retail_products.dormancy_step1_{run_month:%Y_%m}")
print("Step 2:", f"analytics_zone_retail_products.dormancy_step2_{run_month:%Y_%m}")
```

## Stage 1: Aggregation

Script:

```text
pipelines/ch1_aggregation.py
```

Class:

```python
Aggregation
```

This stage:

1. Loads transaction history using the existing transaction filters.
2. Creates purchase frequency and amount indicators.
3. Aggregates transactions by account, year, and month.
4. Calculates transaction dates, transaction counts, and supporting gap features.
5. Generates the account-month reference grid.
6. Applies the output date range.
7. Saves the Step 0 table.

The existing upstream calculations are retained through the downstream deduplication boundary. Removing columns before `dropDuplicates()` could change which rows survive.

## Stage 2: Data Preparation

Script:

```text
pipelines/ch2_Data_Preparation.py
```

Class:

```python
DataPreparation
```

This stage:

1. Reads Step 0.
2. Loads statement balances.
3. Joins balances by account, year, and month.
4. Loads current account information.
5. Joins account opening, closing, and administrative-status information.
6. Calculates utilization, available credit, and account status.
7. Saves the Step 1 table.

Utilization is calculated as:

```text
utilized_rate = opening_balance / (credit_limit + 1)
```

The available-credit calculation retains its existing threshold:

```python
final_df = available_balance(final_df, 0.95)
```

Do not replace this threshold with the inference threshold of `0.8`. They are used at different stages, and changing the upstream threshold changes the model features.

## Stage 3: Stopper Features

Script:

```text
pipelines/ch3_Stopper_Pipeline.py
```

Class:

```python
StopperPipeline
```

This stage reads Step 1, performs the original deduplication, and retains the calculations required by the LSTM and leads output.

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

Existing balance adjustments, rolling windows, sample standard deviation, null handling, and forward filling are preserved.

No new division-by-zero handling is introduced; existing Spark behavior and settings still apply.

### Retained Final Columns

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

Intermediate features such as `available_credit`, transaction dates, and rolling standard deviation remain necessary during calculation even though they are not all saved in the final selection.

### Removed Unused Calculations

The reduced Stopper pipeline no longer calls:

```text
first_day_month
recency
deviation_recency
next_six_months
compute_rolling_stats_P
freq_deviation_P
compute_rolling_amount_stats_P
amount_deviation_P
counter_remaining_balance_P
limit_segment
```

Their outputs do not feed the retained features or the supplied leads workflow.

The helper definitions can remain in their source files. Other consumers expecting the previous wider output schema must be reviewed separately.

## Stage 4: Leads Inference

Script:

```text
pipelines/leads_inference.py
```

Class:

```python
LeadsInference
```

The class uses the Spark session supplied by the main runner:

```python
class LeadsInference:

    def __init__(self, spark):
        self.spark = spark

    def run(self):
        run_inference(self.spark)
```

The inference source is:

```python
SOURCE_TABLE = f"analytics_zone_retail_products.dormancy_step2_{LEAD_YEAR}_{LEAD_MONTH:02d}"
```

### Sequence Preparation

Inference:

1. Excludes administrative statuses `6` and `7`.
2. Selects the nine calendar months preceding the lead month.
3. Keeps accounts with at most three null model-feature values in that selected history.
4. Sorts account history by `period_date`.
5. Takes the latest seven available values per account.
6. Excludes accounts with fewer than seven rows.
7. Clips values to `[-5, 3.5]`.
8. Replaces missing values with sentinel `5.0`.
9. Applies the saved scaler to non-sentinel values.

The model input shape is:

```text
(number_of_accounts, 7, 1)
```

The only LSTM input feature is:

```text
RD_from_last_gap_balance
```

Inference uses available rows from Step 2. It does not independently create missing months or enforce a new deduplication rule.

### Model and Scaler

The current paths are:

```python
SCALER_PATH = "/home/cdsw/_ayman/dormancy/leads/model/scaler.pkl"
MODEL_PATH = "/home/cdsw/_ayman/dormancy/leads/model/smallseq2_31_03_2026"
```

The model is loaded with its existing custom loss:

```python
model = load_model(MODEL_PATH, custom_objects={"mse_mae_loss": mse_mae_loss})
```

Use the scaler paired with the deployed model.

### Lead Selection

The existing inference logic:

- Keeps dormancy scores greater than or equal to `2.2`.
- Joins account features from the previous month.
- Joins personas from the previous month.
- Joins customer profiles from the latest available profile insertion date.
- Requires card type, phone number, and guarantee type.
- Excludes guarantee types containing `staff`.
- Keeps utilization less than or equal to `0.8`.
- Retains the existing recency groups.
- Excludes accounts present in other campaigns in the configured campaign table.
- Calculates behavior change and status buckets.
- Rounds the output dormancy score to four decimals.

Recency groups are:

| Group | Condition |
| --- | --- |
| Low | Recency below 30 days |
| Medium | Recency from 30 through 60 days |
| High | Recency above 60 through 300 days |

The existing union order is high, medium, then low.

Profile and persona joins retain their original behavior; no new deduplication is added.

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

- Confirm the updated scripts are deployed.
- Confirm source and target table names match across stages.
- Confirm source-table access and output-table permissions.
- Confirm the model and scaler files exist.
- Confirm the configured campaign table exists with the expected schema.
- Check the campaign ID and save setting.

From the directory containing `main.py`:

```bash
python main.py
```

From a notebook opened in that directory:

```python
%run ./main.py
```

The updated `main.py` defines `create_spark()` directly, so this import is not required:

```python
from helpers import create_spark
```

The main runner uses the existing Spark settings, sets the checkpoint directory to `/tmp/spark-checkpoints`, and stops Spark after the workflow finishes.

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

## Running Inference Only

If the current-month Step 2 table already exists, run:

```bash
python -m pipelines.leads_inference
```

Or use an existing Spark session in a notebook:

```python
from pipelines.leads_inference import LeadsInference

LeadsInference(spark).run()
```

Inference-only execution avoids rebuilding Step 0, Step 1, and Step 2.

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

The existing status-bucket count display provides a summary of the generated data.

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

For an intentional separate saved test, use an unused test campaign ID.

For example:

```python
CAMPAIGN_ID = "wave_202611"
```

Changing the ID alone does not change the data month. If `LEAD_YEAR` and `LEAD_MONTH` remain October, inference still uses October's source table and date windows.

Changing the campaign ID also changes previous-campaign exclusions. Accounts saved under the original October wave become accounts from another campaign and may be excluded. Results may therefore be smaller or empty.

Restore the automatic campaign ID after a temporary override.

### Campaign History and Zone Changes

`CAMPAIGN_TABLE` controls both:

- The history used to exclude previously contacted accounts.
- The destination used for appending new leads.

Preserve required campaign history when moving the table between zones.

When the configured destination is `dormancy_campaigns_test`, inference does not append to the original campaign table.

## Separate Training Workflow

Training is optional and separate from the monthly runner.

Before running it, ensure `load_data()` in `src/ch8_modeling_data_prep.py` reads the renamed Step 2 table.

Commands:

```bash
python -m pipelines.ch4_model_data_prep
python -m pipelines.ch5_train_lstm
```

Required data and model directories must already exist and be writable.

### Training Filters

The latest requested training filter keeps static dates:

| Setting | Value |
| --- | --- |
| Period start, inclusive | `2022-06-01` |
| Period end, exclusive | `2026-07-01` |
| Opening date cutoff, inclusive | `2026-06-30` |
| Maximum null feature values per account | `5` |

These static cutoffs do not advance automatically.

They correspond to the original three-month lag for an October 2026 run. Actual available history is also limited by the Step 2 output, which starts in June 2025.

The reduced `prepare_features()` function returns the input unchanged because its previous counter adjustment and `TENURE` calculation were unused.

### Training Behavior

The existing training workflow:

- Builds seven-step sequences to predict the next feature value.
- Clips values and handles sentinel values using the existing logic.
- Splits accounts into approximately 70% training, 15% validation, and 15% testing with the existing seeded split.
- Fits the scaler on non-sentinel training values.
- Uses Masking, LSTM layers of 128/64/32 units, and a single dense output.
- Uses a loss combining MSE and MAE equally.
- Trains for up to 50 epochs with batch size 32.
- Uses early stopping with patience 2.
- Reports MAE, MSE, and R².

Training preparation writes arrays and a scaler under:

```text
/home/cdsw/_ayman/dormancy/data/
```

Training saves its model under:

```text
/home/cdsw/_ayman/dormancy/model/lstm_model
```

These paths differ from the inference artifact paths.

Running training does not automatically replace the model or scaler used by inference.

The legacy `ch6_test_lstm.py` script is not part of `main.py`. Review its historical tables, schema assumptions, artifact paths, and existing syntax issues before running it separately.

## Troubleshooting

| Error or symptom | Check |
| --- | --- |
| `No module named 'helpers'` | Use the updated runner with `create_spark()` defined directly in `main.py` |
| Cannot import `pipelines` or `Development_Chadi` | Check the working directory, package layout, and Python import paths |
| Cannot import `LeadsInference` | Confirm the file is inside `pipelines/` and defines the class |
| Monthly table not found | Check the zone, month suffix, preceding stage, and permissions |
| `self.run_month` is missing | Define it before building names and import `date` in the pipeline script |
| Unresolved `balance_and_inactivity_p_counter` | Remove the unused inference adjustment and read Step 2 directly |
| `output_table_name` is undefined | Remove the obsolete print call or print `CAMPAIGN_TABLE` instead |
| Campaign table not found | Create or copy the destination with the required schema before running |
| Campaign ID already exists | Disable saving for inspection or use an intentional unused test ID |
| Missing campaign destination columns | Compare the campaign schema with the final lead selection |
| No eligible accounts have a full sequence | Check history availability, account statuses, null counts, and row counts |
| No final leads | Check score, utilization, recency, profile completeness, and campaign-history exclusions |
| Model or scaler load failure | Check paths, permissions, paired artifacts, and compatible library versions |
| Driver memory failure | Check the sequence-data volume collected by `toPandas()` |

### Existing Balance-Loader Behavior

The original balance loader contains a query with a static insertion date that is immediately overwritten by a fresh table read.

That static date does not currently select the balance snapshot. Applying a new snapshot filter would change existing behavior.

### Spark Session Handling

Some legacy pipeline modules create Spark sessions at import time.

Keep the deployed Spark configuration consistent and use the supplied session inside pipeline methods:

```python
df_new = self.spark.table(self.source_table)
```

The main runner owns Spark shutdown for the complete workflow.

## Validation

Validate the updated workflow in the target Spark environment by checking:

- All four stages complete.
- The monthly tables have the expected names and dates.
- The required Step 2 columns exist.
- The saved model and scaler load successfully.
- Generated lead values and status distributions are reasonable.
- Saving affects only the intended campaign table.
- Duplicate-wave protection behaves as expected.

The updated workflow has been reviewed against the supplied code. A successful production execution and output equivalence must be verified in the actual data environment.

No verified automated test suite is supplied for the updated workflow.
